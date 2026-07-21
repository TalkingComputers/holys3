use super::{SearchDetail, FILE_MATCH_OVERLAP_MAX};
use anyhow::{Context, Result};
use seagrep_core::{
    analyze_patterns, FallbackExtent, MatchBounds, MatchOptions, MatchWitness, PatternCache,
    PatternProgram, RegionProgram, SearchExtent, CANDIDATE_BLOCK_BYTES,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PatternKind {
    Exact,
    Proof,
    Fallback,
}

pub(super) struct PatternPlan {
    pub(super) id: usize,
    pub(super) query: seagrep_query::Query,
    pub(super) bounds: MatchBounds,
    pub(super) extent: SearchExtent,
    pub(super) kind: PatternKind,
}

pub(super) struct SearchPrograms {
    pub(super) whole: PatternProgram,
    pub(super) lines: Option<PatternProgram>,
    pub(super) regional: Option<PatternProgram>,
}

pub(super) struct WorkerCache {
    pub(super) whole: PatternCache,
    pub(super) lines: Option<PatternCache>,
    pub(super) regional: Option<PatternCache>,
}

#[derive(Clone, Copy)]
pub(super) struct SearchContext<'a> {
    pub(super) plans: &'a [PatternPlan],
    pub(super) programs: &'a SearchPrograms,
    pub(super) stream_overlap: Option<usize>,
    pub(super) options: MatchOptions,
    pub(super) detail: SearchDetail,
}

pub(super) fn build_plans(
    hirs: &[regex_syntax::hir::Hir],
    strategy: seagrep_core::Strategy,
    detail: SearchDetail,
) -> Result<Vec<PatternPlan>> {
    anyhow::ensure!(!hirs.is_empty(), "search requires at least one pattern");
    if let SearchDetail::MatchWindows { max_bytes } = detail {
        anyhow::ensure!(max_bytes > 0, "match window must be greater than 0");
    }
    Ok(hirs
        .iter()
        .zip(analyze_patterns(hirs))
        .enumerate()
        .map(|(id, (hir, bounds))| {
            let query = seagrep_query::plan_hir(hir, strategy);
            let selected = match detail {
                SearchDetail::MatchCount => {
                    bounds.exact_bytes.map(|bytes| (bytes, PatternKind::Exact))
                }
                SearchDetail::Documents
                | SearchDetail::MatchingLines
                | SearchDetail::MatchWindows { .. }
                | SearchDetail::FullLines => bounds.witness.as_ref().map(|witness| match witness {
                    MatchWitness::Exact { bytes } => (*bytes, PatternKind::Exact),
                    MatchWitness::Proven { bytes, .. } => (*bytes, PatternKind::Proof),
                }),
            };
            let (extent, kind) = match selected {
                Some((span, kind))
                    if query != seagrep_query::Query::All
                        && span > 0
                        && span <= CANDIDATE_BLOCK_BYTES =>
                {
                    (SearchExtent::Bytes { span }, kind)
                }
                _ => {
                    let extent = match bounds.fallback {
                        FallbackExtent::Lines => SearchExtent::Lines,
                        FallbackExtent::Document => SearchExtent::Document,
                    };
                    (extent, PatternKind::Fallback)
                }
            };
            PatternPlan {
                id,
                query,
                bounds,
                extent,
                kind,
            }
        })
        .collect())
}

pub(super) fn get_stream_overlap(plans: &[PatternPlan]) -> Option<usize> {
    let mut max_span = 0usize;
    for plan in plans {
        let SearchExtent::Bytes { span } = plan.extent else {
            return None;
        };
        max_span = max_span.max(span);
    }
    let overlap = max_span.checked_sub(1)?;
    (overlap <= FILE_MATCH_OVERLAP_MAX).then_some(overlap)
}

impl SearchPrograms {
    pub(super) fn compile(
        hirs: &[regex_syntax::hir::Hir],
        plans: &[PatternPlan],
    ) -> Result<SearchPrograms> {
        anyhow::ensure!(
            hirs.len() == plans.len(),
            "pattern HIR count {} differs from plan count {}",
            hirs.len(),
            plans.len()
        );
        for (index, plan) in plans.iter().enumerate() {
            anyhow::ensure!(
                plan.id == index,
                "pattern plan {index} carries ID {}",
                plan.id
            );
        }
        let ids = plans.iter().map(|plan| plan.id).collect::<Vec<_>>();
        let whole = PatternProgram::compile(hirs, &ids)
            .with_context(|| format!("compiling {}-pattern whole verifier", hirs.len()))?;
        let compile_subset = |include: &dyn Fn(SearchExtent) -> bool,
                              name: &str|
         -> Result<Option<PatternProgram>> {
            let selected = hirs
                .iter()
                .zip(plans)
                .filter(|(_, plan)| include(plan.extent))
                .collect::<Vec<_>>();
            if selected.is_empty() {
                return Ok(None);
            }
            let subset_hirs = selected
                .iter()
                .map(|(hir, _)| (*hir).clone())
                .collect::<Vec<_>>();
            let subset_ids = selected.iter().map(|(_, plan)| plan.id).collect::<Vec<_>>();
            PatternProgram::compile(&subset_hirs, &subset_ids)
                .with_context(|| format!("compiling {}-pattern {name} verifier", selected.len()))
                .map(Some)
        };
        let lines = compile_subset(&|extent| extent != SearchExtent::Document, "line")?;
        let regional = compile_subset(
            &|extent| matches!(extent, SearchExtent::Bytes { .. }),
            "regional",
        )?;
        Ok(SearchPrograms {
            whole,
            lines,
            regional,
        })
    }
}

impl WorkerCache {
    pub(super) fn create(programs: &SearchPrograms) -> WorkerCache {
        WorkerCache {
            whole: programs.whole.create_cache(),
            lines: programs.lines.as_ref().map(PatternProgram::create_cache),
            regional: programs.regional.as_ref().map(PatternProgram::create_cache),
        }
    }
}

pub(super) fn get_region_program<'a>(
    programs: &'a SearchPrograms,
    cache: &'a mut WorkerCache,
    region: RegionProgram,
) -> Result<(&'a PatternProgram, &'a mut PatternCache)> {
    match region {
        RegionProgram::Regional => Ok((
            programs
                .regional
                .as_ref()
                .context("regional verifier is missing")?,
            cache
                .regional
                .as_mut()
                .context("regional verifier cache is missing")?,
        )),
        RegionProgram::Full => Ok((
            programs
                .lines
                .as_ref()
                .context("line verifier is missing")?,
            cache
                .lines
                .as_mut()
                .context("line verifier cache is missing")?,
        )),
    }
}
