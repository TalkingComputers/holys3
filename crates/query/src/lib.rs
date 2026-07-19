#![cfg_attr(docsrs, feature(doc_auto_cfg))]
//! Regex query planning for seagrep indexes.

use seagrep_core::{grams_query, Strategy};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Query {
    All,
    And(Vec<Query>),
    Or(Vec<Query>),
    Gram(Vec<u8>),
}

fn lit_query(lit: &[u8], s: Strategy) -> Query {
    let grams = grams_query(lit, s);
    if grams.is_empty() {
        Query::All
    } else {
        Query::And(grams.into_iter().map(Query::Gram).collect())
    }
}

/// One side's literal set (prefixes OR suffixes) as a gram constraint.
/// `None` = this side constrains nothing.
fn side_query(seq: &regex_syntax::hir::literal::Seq, strategy: Strategy) -> Option<Query> {
    let literals = seq.literals()?;
    if literals.is_empty() {
        return None;
    }
    let branches: Vec<Query> = literals
        .iter()
        .map(|l| lit_query(l.as_bytes(), strategy))
        .collect();
    if branches.contains(&Query::All) {
        None
    } else {
        Some(Query::Or(branches))
    }
}

/// Gram constraints of one HIR node: every match must START with one of its
/// prefix literals AND END with one of its suffix literals, so both sides'
/// grams are necessary conditions — `foo.*bar` constrains on `foo` grams AND
/// `bar` grams. Where the ends are unconstrained, required INNER literals
/// still constrain (Cox-style): each concat element's text appears in every
/// match, so `.*ERROR.*` plans `ERROR` grams instead of a full scan; an
/// alternation is necessarily one of its branches; a repetition's body
/// appears whenever `min >= 1`. (Inexact literals still appear verbatim
/// inside every match, which is all the gram constraint needs; candidates
/// remain a strict superset.)
fn hir_query(hir: &regex_syntax::hir::Hir, strategy: Strategy) -> Query {
    use regex_syntax::hir::literal::{ExtractKind, Extractor};
    use regex_syntax::hir::HirKind;
    let mut parts: Vec<Query> = [
        Extractor::new().extract(hir),
        Extractor::new().kind(ExtractKind::Suffix).extract(hir),
    ]
    .iter()
    .filter_map(|seq| side_query(seq, strategy))
    .collect();
    match hir.kind() {
        HirKind::Concat(children) => {
            parts.extend(children.iter().map(|child| hir_query(child, strategy)));
        }
        HirKind::Alternation(alternatives) => {
            let branches: Vec<Query> = alternatives
                .iter()
                .map(|alt| hir_query(alt, strategy))
                .collect();
            if !branches.contains(&Query::All) {
                parts.push(Query::Or(branches));
            }
        }
        HirKind::Capture(capture) => parts.push(hir_query(&capture.sub, strategy)),
        HirKind::Repetition(rep) if rep.min >= 1 => {
            parts.push(hir_query(&rep.sub, strategy));
        }
        _ => {}
    }
    let mut unique: Vec<Query> = Vec::new();
    for part in parts {
        if part != Query::All && !unique.contains(&part) {
            unique.push(part);
        }
    }
    match unique.len() {
        0 => Query::All,
        1 => unique.swap_remove(0),
        _ => Query::And(unique),
    }
}

pub fn plan(pattern: &str, strategy: Strategy) -> anyhow::Result<Query> {
    Ok(plan_hir(&seagrep_core::parse_pattern(pattern)?, strategy))
}

/// Plan from an already-parsed pattern; the search path parses once with
/// `seagrep_core::parse_pattern` and shares the Hir with the grep-mode
/// analyses.
pub fn plan_hir(hir: &regex_syntax::hir::Hir, strategy: Strategy) -> Query {
    hir_query(hir, strategy)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wildcard_is_all() {
        assert_eq!(plan(".*", Strategy::Sparse).unwrap(), Query::All);
    }

    #[test]
    fn single_char_literal_is_all() {
        // one byte -> no pair -> no gram -> All
        assert_eq!(plan("a", Strategy::Sparse).unwrap(), Query::All);
    }

    #[test]
    fn literal_yields_gram_conjunction() {
        // a real literal produces an OR of one AND-of-grams (not All, not None)
        // (prefix and suffix extraction agree on a plain literal, so the
        // two sides dedup to a single Or rather than And([q, q]))
        let q = plan("handleClick", Strategy::Sparse).unwrap();
        assert!(matches!(q, Query::Or(_)));
        assert_ne!(q, Query::All);
    }

    fn grams_of(q: &Query) -> Vec<Vec<u8>> {
        match q {
            Query::Gram(g) => vec![g.clone()],
            Query::And(children) | Query::Or(children) => {
                children.iter().flat_map(grams_of).collect()
            }
            Query::All => Vec::new(),
        }
    }

    #[test]
    fn dot_star_gap_constrains_both_sides() {
        let q = plan("needle.*haystack", Strategy::Trigram).unwrap();
        let Query::And(sides) = &q else {
            panic!("expected And of prefix+suffix sides, got {q:?}")
        };
        assert_eq!(sides.len(), 2);
        let grams = grams_of(&q);
        assert!(grams.contains(&b"nee".to_vec()), "missing prefix grams");
        assert!(grams.contains(&b"hay".to_vec()), "missing suffix grams");
    }

    #[test]
    fn unbounded_suffix_still_uses_prefix() {
        // `needle.*` has no suffix literals; the prefix side alone applies
        let q = plan("needle.*", Strategy::Trigram).unwrap();
        assert!(matches!(q, Query::Or(_)));
        assert!(grams_of(&q).contains(&b"nee".to_vec()));
    }

    #[test]
    fn alternation_with_common_suffix() {
        // every match ends with "_total"; the suffix side must survive even
        // though the prefix side is an alternation
        let q = plan("(http|grpc)_requests_total", Strategy::Trigram).unwrap();
        let grams = grams_of(&q);
        assert!(
            grams.contains(&b"tot".to_vec()),
            "suffix constraint dropped"
        );
    }

    #[test]
    fn inner_literal_constrains_unanchored_pattern() {
        // both ends are unconstrained, but ERROR appears in every match
        let q = plan(".*ERROR.*", Strategy::Trigram).unwrap();
        assert_ne!(q, Query::All, ".*ERROR.* must not scan everything");
        assert!(grams_of(&q).contains(&b"ERR".to_vec()));
    }

    #[test]
    fn inner_alternation_constrains() {
        // every match contains quick or lazy
        let q = plan(".*(quick|lazy).*", Strategy::Trigram).unwrap();
        assert_ne!(q, Query::All);
        let grams = grams_of(&q);
        assert!(grams.contains(&b"qui".to_vec()));
        assert!(grams.contains(&b"laz".to_vec()));
    }

    #[test]
    fn optional_inner_literal_stays_all() {
        // (ERROR)? may match zero times: its grams are NOT necessary
        assert_eq!(plan(".*(ERROR)?.*", Strategy::Trigram).unwrap(), Query::All);
    }

    #[test]
    fn repeated_inner_literal_constrains() {
        // (ERROR)+ appears at least once
        let q = plan(".*(ERROR)+.*", Strategy::Trigram).unwrap();
        assert!(grams_of(&q).contains(&b"ERR".to_vec()));
    }
}
