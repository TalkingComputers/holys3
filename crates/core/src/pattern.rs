use std::mem::size_of;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProofDirection {
    Forward,
    Reverse,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FallbackExtent {
    Lines,
    Document,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SearchExtent {
    Bytes { span: usize },
    Lines,
    Document,
}

#[derive(Clone)]
pub enum MatchWitness {
    Exact {
        bytes: usize,
    },
    Proven {
        bytes: usize,
        direction: ProofDirection,
        machine: std::sync::Arc<regex_automata::dfa::sparse::DFA<Vec<u8>>>,
    },
}

#[derive(Clone)]
pub struct MatchBounds {
    pub exact_bytes: Option<usize>,
    pub witness: Option<MatchWitness>,
    pub fallback: FallbackExtent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PatternMatch {
    pub pattern: usize,
    pub start: usize,
    pub end: usize,
}

pub struct PatternProgram {
    regex: regex_automata::meta::Regex,
    pattern_ids: Box<[usize]>,
}

pub struct PatternCache {
    cache: regex_automata::meta::Cache,
}

pub struct PatternMatches<'p, 'c, 'h> {
    program: &'p PatternProgram,
    cache: &'c mut PatternCache,
    searcher: regex_automata::util::iter::Searcher<'h>,
}

struct ProofCandidate {
    bytes: usize,
    direction: ProofDirection,
    machine: regex_automata::dfa::sparse::DFA<Vec<u8>>,
    retained_bytes: usize,
}

struct ProofGraph {
    start: usize,
    states: Vec<regex_automata::util::primitives::StateID>,
    accepting: Vec<bool>,
    edges: Vec<Vec<usize>>,
}

const PROOF_NFA_BYTES: usize = 8 * 1024 * 1024;
const PROOF_DFA_BYTES: usize = 8 * 1024 * 1024;
const PROOF_SCRATCH_BYTES: usize = 16 * 1024 * 1024;
const RETAINED_PROOF_BYTES: usize = 32 * 1024 * 1024;
const PROOF_MAP_BUCKET_BYTES: usize =
    size_of::<(regex_automata::util::primitives::StateID, usize)>() + 2 * size_of::<usize>();

pub fn parse_pattern(pattern: &str) -> anyhow::Result<regex_syntax::hir::Hir> {
    Ok(regex_syntax::ParserBuilder::new()
        .utf8(false)
        .build()
        .parse(pattern)?)
}

pub fn analyze_patterns(hirs: &[regex_syntax::hir::Hir]) -> Vec<MatchBounds> {
    let mut retained_bytes = 0;
    hirs.iter()
        .map(|hir| analyze_pattern(hir, &mut retained_bytes))
        .collect()
}

fn analyze_pattern(hir: &regex_syntax::hir::Hir, retained_bytes: &mut usize) -> MatchBounds {
    let fallback = choose_fallback(hir);
    if let Some(bytes) = find_exact_bytes(hir) {
        return MatchBounds {
            exact_bytes: Some(bytes),
            witness: Some(MatchWitness::Exact { bytes }),
            fallback,
        };
    }
    let forward = find_proof(hir, ProofDirection::Forward);
    let reverse = find_proof(hir, ProofDirection::Reverse);
    let candidate = match (forward, reverse) {
        (Some(forward), Some(reverse)) if reverse.bytes < forward.bytes => Some(reverse),
        (Some(forward), _) => Some(forward),
        (None, reverse) => reverse,
    };
    let witness = candidate.and_then(|candidate| {
        let next = retained_bytes.checked_add(candidate.retained_bytes)?;
        if next > RETAINED_PROOF_BYTES {
            return None;
        }
        *retained_bytes = next;
        Some(MatchWitness::Proven {
            bytes: candidate.bytes,
            direction: candidate.direction,
            machine: std::sync::Arc::new(candidate.machine),
        })
    });
    MatchBounds {
        exact_bytes: None,
        witness,
        fallback,
    }
}

fn find_exact_bytes(hir: &regex_syntax::hir::Hir) -> Option<usize> {
    if !hir.properties().look_set().is_empty() || can_match_newline(hir) {
        return None;
    }
    hir.properties().maximum_len().filter(|bytes| *bytes > 0)
}

fn choose_fallback(hir: &regex_syntax::hir::Hir) -> FallbackExtent {
    if hir.properties().look_set().contains_anchor_haystack() || can_match_newline(hir) {
        FallbackExtent::Document
    } else {
        FallbackExtent::Lines
    }
}

fn can_match_newline(hir: &regex_syntax::hir::Hir) -> bool {
    use regex_syntax::hir::{Class, HirKind};

    match hir.kind() {
        HirKind::Literal(literal) => literal.0.contains(&b'\n'),
        HirKind::Class(Class::Bytes(class)) => class
            .ranges()
            .iter()
            .any(|range| range.start() <= b'\n' && b'\n' <= range.end()),
        HirKind::Class(Class::Unicode(class)) => class
            .ranges()
            .iter()
            .any(|range| range.start() <= '\n' && '\n' <= range.end()),
        HirKind::Repetition(repetition) => can_match_newline(&repetition.sub),
        HirKind::Capture(capture) => can_match_newline(&capture.sub),
        HirKind::Concat(children) | HirKind::Alternation(children) => {
            children.iter().any(can_match_newline)
        }
        HirKind::Empty | HirKind::Look(_) => false,
    }
}

fn find_proof(hir: &regex_syntax::hir::Hir, direction: ProofDirection) -> Option<ProofCandidate> {
    if !is_proof_eligible(hir) {
        return None;
    }
    let dense = build_proof_dfa(hir, direction)?;
    let graph = build_proof_graph(&dense, direction)?;
    let bytes = find_longest_accepting_path(&graph)?;
    if bytes == 0 || bytes > crate::CANDIDATE_BLOCK_BYTES {
        return None;
    }
    let machine = dense.to_sparse().ok()?;
    let retained_bytes = machine.memory_usage();
    Some(ProofCandidate {
        bytes,
        direction,
        machine,
        retained_bytes,
    })
}

fn is_proof_eligible(hir: &regex_syntax::hir::Hir) -> bool {
    let properties = hir.properties();
    properties.look_set().is_empty()
        && properties
            .minimum_len()
            .is_some_and(|bytes| bytes > 0 && bytes <= crate::CANDIDATE_BLOCK_BYTES)
        && !can_match_newline(hir)
        && properties.maximum_len().is_none()
}

fn build_proof_dfa(
    hir: &regex_syntax::hir::Hir,
    direction: ProofDirection,
) -> Option<regex_automata::dfa::dense::DFA<Vec<u32>>> {
    build_proof_dfa_with_nfa_limit(hir, direction, PROOF_NFA_BYTES)
}

fn build_proof_dfa_with_nfa_limit(
    hir: &regex_syntax::hir::Hir,
    direction: ProofDirection,
    nfa_bytes: usize,
) -> Option<regex_automata::dfa::dense::DFA<Vec<u32>>> {
    let nfa_config = regex_automata::nfa::thompson::Config::new()
        .which_captures(regex_automata::nfa::thompson::WhichCaptures::None)
        .utf8(false)
        .reverse(direction == ProofDirection::Reverse)
        .nfa_size_limit(Some(nfa_bytes));
    let mut compiler = regex_automata::nfa::thompson::Compiler::new();
    compiler.configure(nfa_config);
    let nfa = compiler.build_from_hir(hir).ok()?;
    let dfa_config = regex_automata::dfa::dense::Config::new()
        .start_kind(regex_automata::dfa::StartKind::Anchored)
        .match_kind(regex_automata::MatchKind::All)
        .dfa_size_limit(Some(PROOF_DFA_BYTES))
        .determinize_size_limit(Some(PROOF_SCRATCH_BYTES));
    let mut builder = regex_automata::dfa::dense::Builder::new();
    builder.configure(dfa_config);
    builder.build_from_nfa(&nfa).ok()
}

fn build_proof_graph(
    dfa: &regex_automata::dfa::dense::DFA<Vec<u32>>,
    direction: ProofDirection,
) -> Option<ProofGraph> {
    build_proof_graph_with_limit(dfa, direction, PROOF_SCRATCH_BYTES)
}

fn build_proof_graph_with_limit(
    dfa: &regex_automata::dfa::dense::DFA<Vec<u32>>,
    direction: ProofDirection,
    scratch_limit: usize,
) -> Option<ProofGraph> {
    use regex_automata::dfa::Automaton;

    let input = regex_automata::Input::new(&[]).anchored(regex_automata::Anchored::Yes);
    let start = match direction {
        ProofDirection::Forward => dfa.start_state_forward(&input),
        ProofDirection::Reverse => dfa.start_state_reverse(&input),
    }
    .ok()?;
    if dfa.is_dead_state(start) || dfa.is_quit_state(start) {
        return None;
    }
    let mut indices = std::collections::HashMap::new();
    let mut queue = std::collections::VecDeque::new();
    let mut states = Vec::new();
    let mut accepting = Vec::new();
    let mut edges = Vec::new();
    let mut scratch_bytes = 0;
    reserve_indices(&mut indices, 1, &mut scratch_bytes, scratch_limit)?;
    reserve_queue(&mut queue, 1, &mut scratch_bytes, scratch_limit)?;
    reserve_vec(&mut states, 1, &mut scratch_bytes, scratch_limit)?;
    reserve_bits(&mut accepting, 1, &mut scratch_bytes, scratch_limit)?;
    reserve_vec(&mut edges, 1, &mut scratch_bytes, scratch_limit)?;
    indices.insert(start, 0);
    queue.push_back(0);
    states.push(start);
    accepting.push(false);
    edges.push(Vec::new());
    while let Some(index) = queue.pop_front() {
        if is_accepting(dfa, states[index]) {
            accepting[index] = true;
            continue;
        }
        let mut destinations = Vec::new();
        for unit in dfa.byte_classes().representatives(..) {
            let Some(byte) = unit.as_u8() else {
                continue;
            };
            let next = dfa.next_state(states[index], byte);
            if dfa.is_quit_state(next) {
                return None;
            }
            if dfa.is_dead_state(next) || destinations.contains(&next) {
                continue;
            }
            reserve_vec(&mut destinations, 1, &mut scratch_bytes, scratch_limit)?;
            destinations.push(next);
            let next_index = if let Some(&next_index) = indices.get(&next) {
                next_index
            } else {
                reserve_indices(&mut indices, 1, &mut scratch_bytes, scratch_limit)?;
                reserve_queue(&mut queue, 1, &mut scratch_bytes, scratch_limit)?;
                reserve_vec(&mut states, 1, &mut scratch_bytes, scratch_limit)?;
                reserve_bits(&mut accepting, 1, &mut scratch_bytes, scratch_limit)?;
                reserve_vec(&mut edges, 1, &mut scratch_bytes, scratch_limit)?;
                let next_index = states.len();
                indices.insert(next, next_index);
                states.push(next);
                accepting.push(false);
                edges.push(Vec::new());
                queue.push_back(next_index);
                next_index
            };
            reserve_vec(&mut edges[index], 1, &mut scratch_bytes, scratch_limit)?;
            edges[index].push(next_index);
        }
        scratch_bytes = scratch_bytes.checked_sub(
            destinations
                .capacity()
                .checked_mul(size_of::<regex_automata::util::primitives::StateID>())?,
        )?;
    }
    drop(indices);
    drop(queue);
    scratch_bytes = count_graph_bytes(&states, &accepting, &edges)?;
    let mut reverse = Vec::new();
    reserve_vec(
        &mut reverse,
        states.len(),
        &mut scratch_bytes,
        scratch_limit,
    )?;
    reverse.resize_with(states.len(), Vec::new);
    for (source, destinations) in edges.iter().enumerate() {
        for &destination in destinations {
            reserve_vec(
                &mut reverse[destination],
                1,
                &mut scratch_bytes,
                scratch_limit,
            )?;
            reverse[destination].push(source);
        }
    }
    let mut coaccessible = Vec::new();
    reserve_bits(
        &mut coaccessible,
        states.len(),
        &mut scratch_bytes,
        scratch_limit,
    )?;
    coaccessible.resize(states.len(), false);
    let mut queue = std::collections::VecDeque::new();
    reserve_queue(&mut queue, states.len(), &mut scratch_bytes, scratch_limit)?;
    for (index, &is_accepting) in accepting.iter().enumerate() {
        if is_accepting {
            coaccessible[index] = true;
            queue.push_back(index);
        }
    }
    while let Some(index) = queue.pop_front() {
        for &source in &reverse[index] {
            if !coaccessible[source] {
                coaccessible[source] = true;
                queue.push_back(source);
            }
        }
    }
    if !coaccessible[0] {
        return None;
    }
    drop(reverse);
    drop(queue);
    scratch_bytes = count_graph_bytes(&states, &accepting, &edges)?;
    scratch_bytes =
        scratch_bytes.checked_add(coaccessible.capacity().checked_mul(size_of::<bool>())?)?;
    if scratch_bytes > scratch_limit {
        return None;
    }
    let mut remap = Vec::new();
    reserve_vec(&mut remap, states.len(), &mut scratch_bytes, scratch_limit)?;
    remap.resize(states.len(), usize::MAX);
    let mut next_index = 0;
    for (index, &keep) in coaccessible.iter().enumerate() {
        if keep {
            remap[index] = next_index;
            next_index += 1;
        }
    }
    for destinations in &mut edges {
        destinations.retain(|destination| coaccessible[*destination]);
        for destination in destinations {
            *destination = remap[*destination];
        }
    }
    let mut index = 0;
    states.retain(|_| {
        let keep = coaccessible[index];
        index += 1;
        keep
    });
    index = 0;
    accepting.retain(|_| {
        let keep = coaccessible[index];
        index += 1;
        keep
    });
    index = 0;
    edges.retain(|_| {
        let keep = coaccessible[index];
        index += 1;
        keep
    });
    Some(ProofGraph {
        start: remap[0],
        states,
        accepting,
        edges,
    })
}

fn reserve_vec<T>(
    values: &mut Vec<T>,
    additional: usize,
    scratch_bytes: &mut usize,
    scratch_limit: usize,
) -> Option<()> {
    let before = values.capacity();
    let target = plan_capacity(
        values.len(),
        before,
        additional,
        size_of::<T>(),
        *scratch_bytes,
        scratch_limit,
    )?;
    if target == before {
        return Some(());
    }
    values
        .try_reserve_exact(target.checked_sub(values.len())?)
        .ok()?;
    if add_capacity_bytes(
        scratch_bytes,
        before,
        values.capacity(),
        size_of::<T>(),
        scratch_limit,
    )
    .is_none()
    {
        *values = Vec::new();
        *scratch_bytes = scratch_bytes.checked_sub(before.checked_mul(size_of::<T>())?)?;
        return None;
    }
    Some(())
}

fn reserve_bits(
    values: &mut Vec<bool>,
    additional: usize,
    scratch_bytes: &mut usize,
    scratch_limit: usize,
) -> Option<()> {
    reserve_vec(values, additional, scratch_bytes, scratch_limit)
}

fn reserve_queue<T>(
    values: &mut std::collections::VecDeque<T>,
    additional: usize,
    scratch_bytes: &mut usize,
    scratch_limit: usize,
) -> Option<()> {
    let before = values.capacity();
    let target = plan_capacity(
        values.len(),
        before,
        additional,
        size_of::<T>(),
        *scratch_bytes,
        scratch_limit,
    )?;
    if target == before {
        return Some(());
    }
    values
        .try_reserve_exact(target.checked_sub(values.len())?)
        .ok()?;
    if add_capacity_bytes(
        scratch_bytes,
        before,
        values.capacity(),
        size_of::<T>(),
        scratch_limit,
    )
    .is_none()
    {
        *values = std::collections::VecDeque::new();
        *scratch_bytes = scratch_bytes.checked_sub(before.checked_mul(size_of::<T>())?)?;
        return None;
    }
    Some(())
}

fn reserve_indices(
    values: &mut std::collections::HashMap<regex_automata::util::primitives::StateID, usize>,
    additional: usize,
    scratch_bytes: &mut usize,
    scratch_limit: usize,
) -> Option<()> {
    let before = values.capacity();
    let target = plan_capacity(
        values.len(),
        before,
        additional,
        PROOF_MAP_BUCKET_BYTES,
        *scratch_bytes,
        scratch_limit,
    )?;
    if target == before {
        return Some(());
    }
    values.try_reserve(target.checked_sub(values.len())?).ok()?;
    if add_capacity_bytes(
        scratch_bytes,
        before,
        values.capacity(),
        PROOF_MAP_BUCKET_BYTES,
        scratch_limit,
    )
    .is_none()
    {
        *values = std::collections::HashMap::new();
        *scratch_bytes = scratch_bytes.checked_sub(before.checked_mul(PROOF_MAP_BUCKET_BYTES)?)?;
        return None;
    }
    Some(())
}

fn plan_capacity(
    length: usize,
    capacity: usize,
    additional: usize,
    element_bytes: usize,
    scratch_bytes: usize,
    scratch_limit: usize,
) -> Option<usize> {
    let required = length.checked_add(additional)?;
    if required <= capacity {
        return Some(capacity);
    }
    let remaining = scratch_limit.checked_sub(scratch_bytes)?;
    let max_capacity = remaining.checked_div(element_bytes)?;
    if required > max_capacity {
        return None;
    }
    Some(capacity.saturating_mul(2).max(required).min(max_capacity))
}

fn add_capacity_bytes(
    scratch_bytes: &mut usize,
    before: usize,
    after: usize,
    element_bytes: usize,
    scratch_limit: usize,
) -> Option<()> {
    let remaining = scratch_limit.checked_sub(*scratch_bytes)?;
    if after.checked_mul(element_bytes)? > remaining {
        return None;
    }
    let growth = after.checked_sub(before)?.checked_mul(element_bytes)?;
    let next = scratch_bytes.checked_add(growth)?;
    if next > scratch_limit {
        return None;
    }
    *scratch_bytes = next;
    Some(())
}

fn count_graph_bytes(
    states: &Vec<regex_automata::util::primitives::StateID>,
    accepting: &Vec<bool>,
    edges: &Vec<Vec<usize>>,
) -> Option<usize> {
    let mut bytes = states
        .capacity()
        .checked_mul(size_of::<regex_automata::util::primitives::StateID>())?;
    bytes = bytes.checked_add(accepting.capacity().checked_mul(size_of::<bool>())?)?;
    bytes = bytes.checked_add(edges.capacity().checked_mul(size_of::<Vec<usize>>())?)?;
    for destinations in edges {
        bytes = bytes.checked_add(destinations.capacity().checked_mul(size_of::<usize>())?)?;
    }
    Some(bytes)
}

fn find_longest_accepting_path(graph: &ProofGraph) -> Option<usize> {
    let mut scratch_bytes = count_graph_bytes(&graph.states, &graph.accepting, &graph.edges)?;
    let mut colors = Vec::new();
    reserve_vec(
        &mut colors,
        graph.states.len(),
        &mut scratch_bytes,
        PROOF_SCRATCH_BYTES,
    )?;
    colors.resize(graph.states.len(), 0u8);
    let mut distances = Vec::new();
    reserve_vec(
        &mut distances,
        graph.states.len(),
        &mut scratch_bytes,
        PROOF_SCRATCH_BYTES,
    )?;
    distances.resize(graph.states.len(), 0usize);
    let mut stack = Vec::new();
    reserve_vec(
        &mut stack,
        graph.states.len(),
        &mut scratch_bytes,
        PROOF_SCRATCH_BYTES,
    )?;
    stack.push((graph.start, 0usize));
    colors[graph.start] = 1;
    while let Some((node, edge_index)) = stack.last_mut() {
        if graph.accepting[*node] {
            colors[*node] = 2;
            stack.pop();
            continue;
        }
        if *edge_index < graph.edges[*node].len() {
            let child = graph.edges[*node][*edge_index];
            *edge_index += 1;
            match colors[child] {
                0 => {
                    colors[child] = 1;
                    stack.push((child, 0));
                }
                1 => return None,
                _ => {}
            }
            continue;
        }
        let mut distance = None;
        for child in &graph.edges[*node] {
            let child_distance = distances[*child].checked_add(1)?;
            distance =
                Some(distance.map_or(child_distance, |current: usize| current.max(child_distance)));
        }
        let distance = distance?;
        distances[*node] = distance;
        colors[*node] = 2;
        stack.pop();
    }
    Some(distances[graph.start])
}

fn is_accepting<A: regex_automata::dfa::Automaton>(
    dfa: &A,
    state: regex_automata::util::primitives::StateID,
) -> bool {
    let state = dfa.next_eoi_state(state);
    !dfa.is_dead_state(state) && !dfa.is_quit_state(state) && dfa.is_match_state(state)
}

impl MatchWitness {
    pub fn find_witness(
        &self,
        haystack: &[u8],
        matched: PatternMatch,
    ) -> anyhow::Result<std::ops::Range<usize>> {
        if matched.start > matched.end || matched.end > haystack.len() {
            anyhow::bail!(
                "verifier match {}..{} is outside a {}-byte region",
                matched.start,
                matched.end,
                haystack.len()
            );
        }
        let MatchWitness::Proven {
            bytes,
            direction,
            machine,
        } = self
        else {
            return Ok(matched.start..matched.end);
        };
        use regex_automata::dfa::Automaton;

        let region = &haystack[matched.start..matched.end];
        let input = regex_automata::Input::new(region).anchored(regex_automata::Anchored::Yes);
        let start = match direction {
            ProofDirection::Forward => machine.start_state_forward(&input),
            ProofDirection::Reverse => machine.start_state_reverse(&input),
        };
        if let Ok(mut state) = start {
            match direction {
                ProofDirection::Forward => {
                    for (index, &byte) in region.iter().take(*bytes).enumerate() {
                        state = machine.next_state(state, byte);
                        if machine.is_dead_state(state) || machine.is_quit_state(state) {
                            break;
                        }
                        if is_accepting(&**machine, state) {
                            return Ok(matched.start..matched.start + index + 1);
                        }
                    }
                }
                ProofDirection::Reverse => {
                    for (index, &byte) in region.iter().rev().take(*bytes).enumerate() {
                        state = machine.next_state(state, byte);
                        if machine.is_dead_state(state) || machine.is_quit_state(state) {
                            break;
                        }
                        if is_accepting(&**machine, state) {
                            return Ok(matched.end - index - 1..matched.end);
                        }
                    }
                }
            }
        }
        anyhow::bail!(
            "finite witness for pattern {} did not accept within {} bytes",
            matched.pattern,
            bytes
        )
    }
}

impl PatternProgram {
    pub fn compile(
        hirs: &[regex_syntax::hir::Hir],
        pattern_ids: &[usize],
    ) -> anyhow::Result<PatternProgram> {
        if hirs.is_empty() {
            anyhow::bail!("pattern program must contain at least one HIR");
        }
        if hirs.len() != pattern_ids.len() {
            anyhow::bail!(
                "pattern HIR count {} differs from pattern ID count {}",
                hirs.len(),
                pattern_ids.len()
            );
        }
        let mut seen = std::collections::HashSet::with_capacity(pattern_ids.len());
        for &pattern_id in pattern_ids {
            if !seen.insert(pattern_id) {
                anyhow::bail!("pattern ID {pattern_id} appears more than once");
            }
        }
        let regex = regex_automata::meta::Regex::builder()
            .configure(
                regex_automata::meta::Regex::config()
                    .which_captures(regex_automata::nfa::thompson::WhichCaptures::Implicit)
                    .utf8_empty(false),
            )
            .build_many_from_hir(hirs)?;
        Ok(PatternProgram {
            regex,
            pattern_ids: pattern_ids.into(),
        })
    }

    pub fn create_cache(&self) -> PatternCache {
        PatternCache {
            cache: self.regex.create_cache(),
        }
    }

    pub fn find_iter<'p, 'c, 'h>(
        &'p self,
        cache: &'c mut PatternCache,
        haystack: &'h [u8],
    ) -> PatternMatches<'p, 'c, 'h> {
        PatternMatches {
            program: self,
            cache,
            searcher: regex_automata::util::iter::Searcher::new(regex_automata::Input::new(
                haystack,
            )),
        }
    }
}

impl Iterator for PatternMatches<'_, '_, '_> {
    type Item = PatternMatch;

    fn next(&mut self) -> Option<PatternMatch> {
        let regex = &self.program.regex;
        let cache = &mut self.cache.cache;
        let matched = self
            .searcher
            .advance(|input| Ok(regex.search_with(cache, input)))?;
        Some(PatternMatch {
            pattern: self.program.pattern_ids[matched.pattern().as_usize()],
            start: matched.start(),
            end: matched.end(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Eq, PartialEq)]
    enum WitnessShape {
        Exact(usize),
        Proven(usize, ProofDirection),
    }

    #[derive(Debug, Eq, PartialEq)]
    struct BoundShape {
        exact_bytes: Option<usize>,
        witness: Option<WitnessShape>,
        fallback: FallbackExtent,
    }

    fn get_bound_shape(bound: &MatchBounds) -> BoundShape {
        let witness = bound.witness.as_ref().map(|witness| match witness {
            MatchWitness::Exact { bytes } => WitnessShape::Exact(*bytes),
            MatchWitness::Proven {
                bytes, direction, ..
            } => WitnessShape::Proven(*bytes, *direction),
        });
        BoundShape {
            exact_bytes: bound.exact_bytes,
            witness,
            fallback: bound.fallback,
        }
    }

    #[test]
    fn parse_pattern_keeps_byte_mode_and_original_error() {
        assert!(parse_pattern(r"(?-u)\xFF").is_ok());
        let error = parse_pattern("(").unwrap_err();
        assert!(error.downcast_ref::<regex_syntax::Error>().is_some());
    }

    #[test]
    fn program_preserves_pattern_ids_and_empty_match_progress() {
        let hirs = [parse_pattern("a").unwrap(), parse_pattern("").unwrap()];
        let program = PatternProgram::compile(&hirs, &[7, 3]).unwrap();
        let mut cache = program.create_cache();
        let matches = program.find_iter(&mut cache, b"ba").collect::<Vec<_>>();

        assert_eq!(
            matches,
            vec![
                PatternMatch {
                    pattern: 3,
                    start: 0,
                    end: 0,
                },
                PatternMatch {
                    pattern: 7,
                    start: 1,
                    end: 2,
                },
            ]
        );
    }

    #[test]
    fn program_rejects_invalid_inputs() {
        let hir = parse_pattern("a").unwrap();
        assert_eq!(
            PatternProgram::compile(&[], &[]).err().unwrap().to_string(),
            "pattern program must contain at least one HIR"
        );
        assert_eq!(
            PatternProgram::compile(std::slice::from_ref(&hir), &[])
                .err()
                .unwrap()
                .to_string(),
            "pattern HIR count 1 differs from pattern ID count 0"
        );
        assert_eq!(
            PatternProgram::compile(&[hir.clone(), hir], &[4, 4])
                .err()
                .unwrap()
                .to_string(),
            "pattern ID 4 appears more than once"
        );
    }

    #[test]
    fn program_uses_byte_semantics() {
        let hir = parse_pattern(r"(?-u)\xFF+").unwrap();
        let program = PatternProgram::compile(&[hir], &[5]).unwrap();
        let mut cache = program.create_cache();
        assert_eq!(
            program
                .find_iter(&mut cache, b"\xFF\xFF")
                .collect::<Vec<_>>(),
            vec![PatternMatch {
                pattern: 5,
                start: 0,
                end: 2,
            }]
        );

        let empty = PatternProgram::compile(&[parse_pattern("").unwrap()], &[6]).unwrap();
        let mut empty_cache = empty.create_cache();
        assert_eq!(
            empty
                .find_iter(&mut empty_cache, "☃".as_bytes())
                .collect::<Vec<_>>(),
            (0..=3)
                .map(|offset| PatternMatch {
                    pattern: 6,
                    start: offset,
                    end: offset,
                })
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn analyze_pattern_table() {
        let large = format!("a{{{},}}", crate::CANDIDATE_BLOCK_BYTES + 1);
        let cases = [
            (
                "foo.{2}",
                BoundShape {
                    exact_bytes: Some(11),
                    witness: Some(WitnessShape::Exact(11)),
                    fallback: FallbackExtent::Lines,
                },
            ),
            (
                "[A-Z0-9]{20,}",
                BoundShape {
                    exact_bytes: None,
                    witness: Some(WitnessShape::Proven(20, ProofDirection::Forward)),
                    fallback: FallbackExtent::Lines,
                },
            ),
            (
                "foo.*",
                BoundShape {
                    exact_bytes: None,
                    witness: Some(WitnessShape::Proven(3, ProofDirection::Forward)),
                    fallback: FallbackExtent::Lines,
                },
            ),
            (
                ".*token",
                BoundShape {
                    exact_bytes: None,
                    witness: Some(WitnessShape::Proven(5, ProofDirection::Reverse)),
                    fallback: FallbackExtent::Lines,
                },
            ),
            (
                "foo.*bar",
                BoundShape {
                    exact_bytes: None,
                    witness: None,
                    fallback: FallbackExtent::Lines,
                },
            ),
            (
                "foo|bar.*baz",
                BoundShape {
                    exact_bytes: None,
                    witness: None,
                    fallback: FallbackExtent::Lines,
                },
            ),
            (
                "(?m)^foo$",
                BoundShape {
                    exact_bytes: None,
                    witness: None,
                    fallback: FallbackExtent::Lines,
                },
            ),
            (
                r"\bfoo",
                BoundShape {
                    exact_bytes: None,
                    witness: None,
                    fallback: FallbackExtent::Lines,
                },
            ),
            (
                r"\Afoo",
                BoundShape {
                    exact_bytes: None,
                    witness: None,
                    fallback: FallbackExtent::Document,
                },
            ),
            (
                r"foo\z",
                BoundShape {
                    exact_bytes: None,
                    witness: None,
                    fallback: FallbackExtent::Document,
                },
            ),
            (
                "foo\nbar",
                BoundShape {
                    exact_bytes: None,
                    witness: None,
                    fallback: FallbackExtent::Document,
                },
            ),
            (
                "a*",
                BoundShape {
                    exact_bytes: None,
                    witness: None,
                    fallback: FallbackExtent::Lines,
                },
            ),
            (
                r"(?-u)\xFF+",
                BoundShape {
                    exact_bytes: None,
                    witness: Some(WitnessShape::Proven(1, ProofDirection::Forward)),
                    fallback: FallbackExtent::Lines,
                },
            ),
            (
                large.as_str(),
                BoundShape {
                    exact_bytes: None,
                    witness: None,
                    fallback: FallbackExtent::Lines,
                },
            ),
            (
                r"[01]*1[01]{16}1[01]*",
                BoundShape {
                    exact_bytes: None,
                    witness: None,
                    fallback: FallbackExtent::Lines,
                },
            ),
        ];
        let hirs = cases
            .iter()
            .map(|(pattern, _)| parse_pattern(pattern).unwrap())
            .collect::<Vec<_>>();
        let bounds = analyze_patterns(&hirs);

        assert!(analyze_patterns(&[]).is_empty());
        assert_eq!(bounds.len(), cases.len());
        for ((pattern, expected), bound) in cases.iter().zip(&bounds) {
            assert_eq!(&get_bound_shape(bound), expected, "{pattern}");
        }
    }

    #[test]
    fn oversized_minimum_skips_proof_construction() {
        let hir = parse_pattern("a{4294967295,}").unwrap();
        assert!(hir.properties().minimum_len().unwrap() > crate::CANDIDATE_BLOCK_BYTES);
        assert!(!is_proof_eligible(&hir));
        assert_eq!(
            get_bound_shape(&analyze_patterns(&[hir]).pop().unwrap()),
            BoundShape {
                exact_bytes: None,
                witness: None,
                fallback: FallbackExtent::Lines,
            }
        );
    }

    #[test]
    fn proof_dfa_honors_nfa_limit() {
        let hir = parse_pattern("[A-Z]{20,}").unwrap();
        assert!(build_proof_dfa_with_nfa_limit(&hir, ProofDirection::Forward, 1).is_none());
        assert!(build_proof_dfa(&hir, ProofDirection::Forward).is_some());
    }

    #[test]
    fn proof_dfa_honors_production_limits() {
        let control = parse_pattern(r"[01]*1[01]{15}1[01]*").unwrap();
        let limited = parse_pattern(r"[01]*1[01]{16}1[01]*").unwrap();
        for direction in [ProofDirection::Forward, ProofDirection::Reverse] {
            assert!(
                build_proof_dfa(&control, direction).is_some(),
                "{direction:?}"
            );
            assert!(
                build_proof_dfa(&limited, direction).is_none(),
                "{direction:?}"
            );
        }
    }

    #[test]
    fn proof_witness_returns_concrete_absolute_slice() {
        let forward_hir = parse_pattern("[A-Z0-9]{20,}").unwrap();
        let forward_bound = analyze_patterns(std::slice::from_ref(&forward_hir))
            .pop()
            .unwrap();
        let forward_witness = forward_bound.witness.unwrap();
        let forward_program = PatternProgram::compile(&[forward_hir], &[8]).unwrap();
        let mut forward_cache = forward_program.create_cache();
        let forward_haystack = b"xxABCDEFGHIJKLMNOPQRSTUVWXYzz";
        let forward_match = forward_program
            .find_iter(&mut forward_cache, forward_haystack)
            .next()
            .unwrap();
        let forward_range = forward_witness
            .find_witness(forward_haystack, forward_match)
            .unwrap();
        assert_eq!(forward_range, 2..22);
        assert!(regex::bytes::Regex::new(r"^(?:[A-Z0-9]{20,})$")
            .unwrap()
            .is_match(&forward_haystack[forward_range]));

        let reverse_hir = parse_pattern(".*token").unwrap();
        let reverse_bound = analyze_patterns(std::slice::from_ref(&reverse_hir))
            .pop()
            .unwrap();
        let reverse_witness = reverse_bound.witness.unwrap();
        let reverse_program = PatternProgram::compile(&[reverse_hir], &[9]).unwrap();
        let mut reverse_cache = reverse_program.create_cache();
        let reverse_haystack = b"prefix___token_suffix";
        let reverse_match = reverse_program
            .find_iter(&mut reverse_cache, reverse_haystack)
            .next()
            .unwrap();
        let reverse_range = reverse_witness
            .find_witness(reverse_haystack, reverse_match)
            .unwrap();
        assert_eq!(reverse_range, 9..14);
        assert!(regex::bytes::Regex::new(r"^(?:.*token)$")
            .unwrap()
            .is_match(&reverse_haystack[reverse_range]));
    }

    #[test]
    fn witness_reports_exact_contract_errors() {
        let exact = MatchWitness::Exact { bytes: 3 };
        assert_eq!(
            exact
                .find_witness(
                    b"foo",
                    PatternMatch {
                        pattern: 2,
                        start: 4,
                        end: 4,
                    },
                )
                .unwrap_err()
                .to_string(),
            "verifier match 4..4 is outside a 3-byte region"
        );

        let hir = parse_pattern("[A-Z]{2,}").unwrap();
        let witness = analyze_patterns(&[hir]).pop().unwrap().witness.unwrap();
        assert_eq!(
            witness
                .find_witness(
                    b"A",
                    PatternMatch {
                        pattern: 11,
                        start: 0,
                        end: 1,
                    },
                )
                .unwrap_err()
                .to_string(),
            "finite witness for pattern 11 did not accept within 2 bytes"
        );
    }

    #[test]
    fn proof_graph_honors_scratch_limit() {
        let trivial_hir = parse_pattern("a*").unwrap();
        let trivial = build_proof_dfa(&trivial_hir, ProofDirection::Forward).unwrap();
        let growing_hir = parse_pattern("[A-Z]{20,}").unwrap();
        let growing = build_proof_dfa(&growing_hir, ProofDirection::Forward).unwrap();
        let traversal_limit = (1..=4_096)
            .find(|&limit| {
                build_proof_graph_with_limit(&trivial, ProofDirection::Forward, limit).is_some()
                    && build_proof_graph_with_limit(&growing, ProofDirection::Forward, limit)
                        .is_none()
            })
            .unwrap();
        assert!(
            build_proof_graph_with_limit(&trivial, ProofDirection::Forward, traversal_limit)
                .is_some()
        );
        assert!(
            build_proof_graph_with_limit(&growing, ProofDirection::Forward, traversal_limit)
                .is_none()
        );
        assert!(build_proof_graph_with_limit(
            &growing,
            ProofDirection::Forward,
            PROOF_SCRATCH_BYTES
        )
        .is_some());
    }

    #[test]
    fn scratch_reservations_are_geometric_and_accounted() {
        let mut values = Vec::new();
        let mut value_bytes = 0;
        let mut value_growths = 0;
        for value in 0..4_096usize {
            let before = values.capacity();
            reserve_vec(&mut values, 1, &mut value_bytes, usize::MAX).unwrap();
            value_growths += usize::from(values.capacity() != before);
            values.push(value);
        }
        assert!(value_growths < 64, "{value_growths}");
        assert_eq!(value_bytes, values.capacity() * size_of::<usize>());

        let mut bits = Vec::new();
        let mut bit_bytes = 0;
        let mut bit_growths = 0;
        for value in 0..4_096 {
            let before = bits.capacity();
            reserve_bits(&mut bits, 1, &mut bit_bytes, usize::MAX).unwrap();
            bit_growths += usize::from(bits.capacity() != before);
            bits.push(value % 2 == 0);
        }
        assert!(bit_growths < 64, "{bit_growths}");
        assert_eq!(bit_bytes, bits.capacity() * size_of::<bool>());

        let mut queue = std::collections::VecDeque::new();
        let mut queue_bytes = 0;
        let mut queue_growths = 0;
        for value in 0..4_096usize {
            let before = queue.capacity();
            reserve_queue(&mut queue, 1, &mut queue_bytes, usize::MAX).unwrap();
            queue_growths += usize::from(queue.capacity() != before);
            queue.push_back(value);
        }
        assert!(queue_growths < 64, "{queue_growths}");
        assert_eq!(queue_bytes, queue.capacity() * size_of::<usize>());
    }

    #[test]
    fn scratch_preflight_preserves_full_collections() {
        let mut values = Vec::with_capacity(8);
        values.resize(values.capacity(), 0u8);
        let value_capacity = values.capacity();
        let mut value_bytes = value_capacity;
        let value_limit = value_bytes * 2;
        assert!(reserve_vec(&mut values, 1, &mut value_bytes, value_limit).is_none());
        assert_eq!(values.capacity(), value_capacity);
        assert_eq!(value_bytes, value_capacity);

        let mut bits = Vec::with_capacity(8);
        bits.resize(bits.capacity(), false);
        let bit_capacity = bits.capacity();
        let mut bit_bytes = bit_capacity * size_of::<bool>();
        let bit_limit = bit_bytes * 2;
        assert!(reserve_bits(&mut bits, 1, &mut bit_bytes, bit_limit).is_none());
        assert_eq!(bits.capacity(), bit_capacity);
        assert_eq!(bit_bytes, bit_capacity * size_of::<bool>());

        let mut queue = std::collections::VecDeque::with_capacity(8);
        queue.resize(queue.capacity(), 0u8);
        let queue_capacity = queue.capacity();
        let mut queue_bytes = queue_capacity;
        let queue_limit = queue_bytes * 2;
        assert!(reserve_queue(&mut queue, 1, &mut queue_bytes, queue_limit).is_none());
        assert_eq!(queue.capacity(), queue_capacity);
        assert_eq!(queue_bytes, queue_capacity);

        let mut indices = std::collections::HashMap::with_capacity(8);
        let index_capacity = indices.capacity();
        for index in 0..index_capacity {
            indices.insert(
                regex_automata::util::primitives::StateID::new(index).unwrap(),
                index,
            );
        }
        let mut index_bytes = index_capacity * PROOF_MAP_BUCKET_BYTES;
        let index_limit = index_bytes * 2;
        assert!(reserve_indices(&mut indices, 1, &mut index_bytes, index_limit).is_none());
        assert_eq!(indices.capacity(), index_capacity);
        assert_eq!(index_bytes, index_capacity * PROOF_MAP_BUCKET_BYTES);
    }

    #[test]
    fn scratch_postcheck_discards_oversized_indices() {
        let mut indices = std::collections::HashMap::with_capacity(1);
        let initial_capacity = indices.capacity();
        for index in 0..initial_capacity {
            indices.insert(
                regex_automata::util::primitives::StateID::new(index).unwrap(),
                index,
            );
        }
        let mut scratch_bytes = initial_capacity * PROOF_MAP_BUCKET_BYTES;
        let scratch_limit = initial_capacity * 3 * PROOF_MAP_BUCKET_BYTES;
        assert!(reserve_indices(&mut indices, 1, &mut scratch_bytes, scratch_limit).is_none());
        assert_eq!(indices.capacity(), 0);
        assert_eq!(scratch_bytes, 0);
    }
}
