use crate::eval::{bind_queries, blocks_needed, eval, Selection, TermValue};
use crate::format::SegmentTables;
use anyhow::{Context, Result};
use seagrep_core::{CandidateRange, DocId, SearchExtent, Strategy, CANDIDATE_BLOCK_BYTES};
use seagrep_query::Query;
use std::collections::BTreeMap;
use std::ops::RangeInclusive;

#[derive(Debug, Clone, Copy)]
pub struct CandidatePlan<'a> {
    pub query: &'a Query,
    pub extent: SearchExtent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CandidateBatchLimits {
    pub documents: usize,
    pub decoded_bytes: u64,
}

pub(crate) fn validate_candidate_plans(
    plans: &[CandidatePlan<'_>],
    limits: CandidateBatchLimits,
) -> Result<()> {
    anyhow::ensure!(!plans.is_empty(), "candidate plans must not be empty");
    anyhow::ensure!(
        limits.documents > 0,
        "candidate document batch limit must be positive"
    );
    anyhow::ensure!(
        limits.decoded_bytes > 0,
        "candidate decoded-byte batch limit must be positive"
    );
    anyhow::ensure!(
        plans
            .iter()
            .all(|plan| !matches!(plan.extent, SearchExtent::Bytes { span: 0 })),
        "candidate byte span must be positive"
    );
    Ok(())
}

pub(crate) fn visit_candidate_selections(
    plans: &[CandidatePlan<'_>],
    id_space: u32,
    strategy: Strategy,
    lookup: &dyn Fn(&[u8]) -> Result<Option<TermValue>>,
    expand: &dyn Fn(usize, DocId) -> RangeInclusive<DocId>,
    fetch_blocks: impl FnOnce(&BTreeMap<u64, (u32, u64)>) -> Result<BTreeMap<u64, Vec<DocId>>>,
    visit: &mut dyn FnMut(usize, Selection) -> Result<()>,
) -> Result<()> {
    let queries = plans.iter().map(|plan| plan.query).collect::<Vec<_>>();
    let bound = bind_queries(&queries, id_space, strategy, lookup)?;
    let mut needed = BTreeMap::new();
    for query in &bound {
        blocks_needed(query, &mut needed);
    }
    let blocks = fetch_blocks(&needed)?;
    for (index, query) in bound.iter().enumerate() {
        let expand_ids = |id| expand(index, id);
        visit(index, eval(query, &blocks, Some(&expand_ids))?)?;
    }
    Ok(())
}

pub(crate) fn build_block_bases(tables: &SegmentTables) -> Result<Vec<u32>> {
    let mut bases = Vec::with_capacity(tables.documents.len() + 1);
    let mut next = 0u32;
    bases.push(next);
    for document in &tables.documents {
        let blocks = document.decoded_size.div_ceil(CANDIDATE_BLOCK_BYTES as u64);
        next = next
            .checked_add(u32::try_from(blocks)?)
            .context("segment candidate block count overflows u32")?;
        bases.push(next);
    }
    Ok(bases)
}

pub(super) fn get_block_document(id: u32, bases: &[u32]) -> usize {
    bases.partition_point(|base| *base <= id) - 1
}

pub(crate) fn expand_candidate_block(
    id: u32,
    bases: &[u32],
    tables: &SegmentTables,
    extent: SearchExtent,
) -> RangeInclusive<u32> {
    let document = get_block_document(id, bases);
    let start = bases[document];
    let end = bases[document + 1] - 1;
    if extent == SearchExtent::Document {
        return start..=end;
    }
    let slack = match extent {
        SearchExtent::Bytes { span } => {
            let blocks = 1 + (span - 1) / CANDIDATE_BLOCK_BYTES;
            blocks.min(u32::MAX as usize) as u32
        }
        SearchExtent::Lines => match tables.documents[document].max_line_len {
            u32::MAX => end - start + 1,
            max_line_len => {
                1 + max_line_len
                    / u32::try_from(CANDIDATE_BLOCK_BYTES).expect("candidate block size fits u32")
            }
        },
        SearchExtent::Document => unreachable!(),
    };
    id.saturating_sub(slack).max(start)..=id.saturating_add(slack).min(end)
}

fn group_candidate_blocks(
    ids: Vec<u32>,
    bases: &[u32],
    extent: SearchExtent,
) -> Result<Vec<(u32, Vec<CandidateRange>)>> {
    anyhow::ensure!(
        extent != SearchExtent::Document,
        "document extent cannot be stored as a candidate range"
    );
    let total = bases.last().copied().unwrap_or(0);
    let mut documents: Vec<(u32, Vec<CandidateRange>)> = Vec::new();
    for id in ids {
        anyhow::ensure!(id < total, "candidate block {id} is outside 0..{total}");
        let document_index = get_block_document(id, bases);
        let document = u32::try_from(document_index)?;
        let local = id - bases[document_index];
        if documents
            .last()
            .is_none_or(|(current, _)| *current != document)
        {
            documents.push((
                document,
                vec![CandidateRange {
                    blocks: local..local + 1,
                    extent,
                }],
            ));
            continue;
        }
        let ranges = &mut documents.last_mut().expect("document exists").1;
        let range = ranges.last_mut().expect("candidate range exists");
        if range.blocks.end == local {
            range.blocks.end += 1;
        } else {
            ranges.push(CandidateRange {
                blocks: local..local + 1,
                extent,
            });
        }
    }
    Ok(documents)
}

fn pick_broader_extent(left: SearchExtent, right: SearchExtent) -> SearchExtent {
    match (left, right) {
        (SearchExtent::Document, _) | (_, SearchExtent::Document) => SearchExtent::Document,
        (SearchExtent::Lines, _) | (_, SearchExtent::Lines) => SearchExtent::Lines,
        (SearchExtent::Bytes { span: left }, SearchExtent::Bytes { span: right }) => {
            SearchExtent::Bytes {
                span: left.max(right),
            }
        }
    }
}

fn merge_candidate_ranges(current: &mut Vec<CandidateRange>, incoming: Vec<CandidateRange>) {
    current.extend(incoming);
    current.sort_unstable_by_key(|range| (range.blocks.start, range.blocks.end));
    let mut merged: Vec<CandidateRange> = Vec::with_capacity(current.len());
    for range in current.drain(..) {
        if let Some(previous) = merged.last_mut() {
            let overlaps = range.blocks.start < previous.blocks.end;
            let joins =
                range.blocks.start == previous.blocks.end && range.extent == previous.extent;
            if overlaps || joins {
                previous.blocks.end = previous.blocks.end.max(range.blocks.end);
                if overlaps {
                    previous.extent = pick_broader_extent(previous.extent, range.extent);
                }
                continue;
            }
        }
        merged.push(range);
    }
    *current = merged;
}

pub(crate) fn add_candidate_selection(
    documents: &mut BTreeMap<u32, Option<Vec<CandidateRange>>>,
    selection: Selection,
    extent: SearchExtent,
    strategy: Strategy,
    document_count: u32,
    block_bases: Option<&[u32]>,
) -> Result<()> {
    let Selection::Ids(ids) = selection else {
        for document in 0..document_count {
            documents.insert(document, None);
        }
        return Ok(());
    };
    if strategy == Strategy::Sparse {
        for document in ids {
            anyhow::ensure!(
                document < document_count,
                "candidate document {document} is outside 0..{document_count}"
            );
            documents.insert(document, None);
        }
        return Ok(());
    }
    let bases = block_bases.context("trigram candidate block bases are missing")?;
    if extent == SearchExtent::Document {
        let total = bases.last().copied().unwrap_or(0);
        for id in ids {
            anyhow::ensure!(id < total, "candidate block {id} is outside 0..{total}");
            documents.insert(u32::try_from(get_block_document(id, bases))?, None);
        }
        return Ok(());
    }
    for (document, ranges) in group_candidate_blocks(ids, bases, extent)? {
        match documents
            .entry(document)
            .or_insert_with(|| Some(Vec::new()))
        {
            Some(current) => merge_candidate_ranges(current, ranges),
            None => {}
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::eval::Selection;
    use crate::format::{DocEntry, SegmentTables};
    use seagrep_core::{CandidateRange, SearchExtent, Strategy, CANDIDATE_BLOCK_BYTES};
    use std::collections::BTreeMap;

    fn tables(blocks: u64, max_line_len: u32) -> SegmentTables {
        SegmentTables {
            sources: Vec::new(),
            documents: vec![DocEntry {
                display_key: "large".into(),
                source_id: 0,
                member_path: None,
                decoded_size: blocks * CANDIDATE_BLOCK_BYTES as u64,
                first_block: 0,
                block_offset: 0,
                max_line_len,
                block_newlines: Vec::new(),
            }],
            blocks: Vec::new(),
        }
    }

    #[test]
    fn saturated_max_line_expands_to_the_whole_document() {
        let tables = tables(4, u32::MAX);
        let bases = build_block_bases(&tables).unwrap();

        assert_eq!(
            expand_candidate_block(2, &bases, &tables, SearchExtent::Lines),
            0..=3
        );
        assert_eq!(
            group_candidate_blocks(vec![0, 1, 2, 3], &bases, SearchExtent::Lines).unwrap(),
            vec![(
                0,
                vec![CandidateRange {
                    blocks: 0..4,
                    extent: SearchExtent::Lines,
                }]
            )]
        );
    }

    #[test]
    fn bounded_match_slack_beats_saturated_line_slack() {
        let tables = tables(8, u32::MAX);
        let bases = build_block_bases(&tables).unwrap();

        assert_eq!(
            expand_candidate_block(4, &bases, &tables, SearchExtent::Bytes { span: 40 },),
            3..=5
        );
    }

    #[test]
    fn block_candidates_group_and_merge_per_document() {
        assert_eq!(
            group_candidate_blocks(
                vec![0, 1, 3, 4, 5, 7],
                &[0, 4, 8],
                SearchExtent::Bytes { span: 3 },
            )
            .unwrap(),
            vec![
                (
                    0,
                    vec![
                        CandidateRange {
                            blocks: 0..2,
                            extent: SearchExtent::Bytes { span: 3 },
                        },
                        CandidateRange {
                            blocks: 3..4,
                            extent: SearchExtent::Bytes { span: 3 },
                        },
                    ],
                ),
                (
                    1,
                    vec![
                        CandidateRange {
                            blocks: 0..2,
                            extent: SearchExtent::Bytes { span: 3 },
                        },
                        CandidateRange {
                            blocks: 3..4,
                            extent: SearchExtent::Bytes { span: 3 },
                        },
                    ],
                ),
            ]
        );
    }

    #[test]
    fn selections_merge_extents_and_documents_once() {
        let mut documents = BTreeMap::new();
        let bases = [0, 6, 10];

        add_candidate_selection(
            &mut documents,
            Selection::Ids(vec![0, 1]),
            SearchExtent::Bytes { span: 4 },
            Strategy::Trigram,
            2,
            Some(&bases),
        )
        .unwrap();
        add_candidate_selection(
            &mut documents,
            Selection::Ids(vec![1, 2, 4, 6]),
            SearchExtent::Bytes { span: 8 },
            Strategy::Trigram,
            2,
            Some(&bases),
        )
        .unwrap();
        assert_eq!(
            documents.get(&0),
            Some(&Some(vec![
                CandidateRange {
                    blocks: 0..3,
                    extent: SearchExtent::Bytes { span: 8 },
                },
                CandidateRange {
                    blocks: 4..5,
                    extent: SearchExtent::Bytes { span: 8 },
                },
            ]))
        );
        add_candidate_selection(
            &mut documents,
            Selection::Ids(vec![2]),
            SearchExtent::Lines,
            Strategy::Trigram,
            2,
            Some(&bases),
        )
        .unwrap();

        assert_eq!(
            documents.get(&0),
            Some(&Some(vec![
                CandidateRange {
                    blocks: 0..3,
                    extent: SearchExtent::Lines,
                },
                CandidateRange {
                    blocks: 4..5,
                    extent: SearchExtent::Bytes { span: 8 },
                },
            ]))
        );
        assert_eq!(
            documents.get(&1),
            Some(&Some(vec![CandidateRange {
                blocks: 0..1,
                extent: SearchExtent::Bytes { span: 8 },
            }]))
        );

        add_candidate_selection(
            &mut documents,
            Selection::Ids(vec![4]),
            SearchExtent::Document,
            Strategy::Trigram,
            2,
            Some(&bases),
        )
        .unwrap();
        add_candidate_selection(
            &mut documents,
            Selection::Ids(vec![5]),
            SearchExtent::Bytes { span: 32 },
            Strategy::Trigram,
            2,
            Some(&bases),
        )
        .unwrap();
        assert_eq!(documents.get(&0), Some(&None));

        let mut sparse = BTreeMap::new();
        add_candidate_selection(
            &mut sparse,
            Selection::Ids(vec![0, 1]),
            SearchExtent::Bytes { span: 4 },
            Strategy::Sparse,
            2,
            None,
        )
        .unwrap();
        assert_eq!(sparse, BTreeMap::from([(0, None), (1, None)]));
    }

    #[test]
    fn validation_is_exact() {
        let query = Query::All;
        let valid = [CandidatePlan {
            query: &query,
            extent: SearchExtent::Bytes { span: 1 },
        }];
        assert!(validate_candidate_plans(
            &valid,
            CandidateBatchLimits {
                documents: 1,
                decoded_bytes: 1,
            }
        )
        .is_ok());
        let invalid = [CandidatePlan {
            query: &query,
            extent: SearchExtent::Bytes { span: 0 },
        }];
        let error = validate_candidate_plans(
            &invalid,
            CandidateBatchLimits {
                documents: 1,
                decoded_bytes: 1,
            },
        )
        .unwrap_err();
        assert_eq!(error.to_string(), "candidate byte span must be positive");
    }
}
