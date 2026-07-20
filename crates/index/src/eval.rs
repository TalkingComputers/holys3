//! Pure query evaluation over prefetched posting blocks.
//!
//! The fst value packs a posting block's byte offset and id count, so the
//! whole pipeline is: resolve grams against the fst (no IO) -> prune -> fetch
//! every needed block in one concurrent batch -> evaluate set algebra over
//! sorted id vectors.

use anyhow::{Context, Result};
use seagrep_core::{DocId, Strategy};
use seagrep_query::Query;
use std::collections::BTreeMap;
use std::ops::RangeInclusive;

/// Keep at most this many (rarest) gram constraints per AND group. Extra
/// grams only narrow an already-small candidate set; dropping them keeps the
/// result a superset of the true matches, which regex verification filters.
/// This bounds index round trips per query regardless of literal length.
const SPARSE_MAX_GRAMS_PER_AND: usize = 8;
const BLOCK_MAX_GRAMS_PER_AND: usize = 64;

const OFFSET_BITS: u32 = 40;
const COUNT_BITS: u32 = 24;

/// The largest id count `pack_posting` can store, and therefore the largest
/// posting id space (candidate blocks for trigram, documents for sparse) one
/// segment may expose: a gram present at every id must still pack.
pub(crate) const MAX_POSTING_COUNT: u64 = (1 << COUNT_BITS) - 1;

/// Pack a posting block's byte offset (40 bits, 1 TiB) and id count
/// (24 bits, 16.7M ids) into one fst value.
pub(crate) fn pack_posting(offset: u64, count: usize) -> Result<u64> {
    anyhow::ensure!(
        offset < 1 << OFFSET_BITS,
        "postings offset {offset} exceeds the 1 TiB format limit"
    );
    let count = u64::try_from(count)?;
    anyhow::ensure!(
        count < 1 << COUNT_BITS,
        "posting list of {count} docs exceeds the format limit"
    );
    Ok(offset | (count << OFFSET_BITS))
}

pub(crate) fn unpack_posting(value: u64) -> (u64, u32) {
    (
        value & ((1 << OFFSET_BITS) - 1),
        (value >> OFFSET_BITS) as u32,
    )
}

/// A dictionary hit: the packed (offset, count) plus, for sparse indexes,
/// the encoded posting-list byte length — delta blocks make length
/// underivable from the count, so the dictionary carries it. `None` means
/// derive it (trigram's fixed-width codec).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TermValue {
    pub packed: u64,
    pub len: Option<u64>,
}

/// A query with every gram resolved against the term dictionary.
pub(crate) enum Resolved {
    All,
    None,
    /// A singleton gram: exactly one posting id, inlined in the term value.
    Doc(DocId),
    Gram {
        offset: u64,
        count: u32,
        len: u64,
    },
    And(Vec<Resolved>),
    Or(Vec<Resolved>),
}

/// Resolve grams via `lookup` (an fst get) and simplify: absent grams make
/// their AND empty without any postings fetch; ALL branches collapse. A gram
/// present at every id (`count == id_space`) constrains nothing, so it
/// resolves to ALL and its posting block is never fetched.
pub(crate) fn resolve(
    q: &Query,
    id_space: u32,
    strategy: Strategy,
    lookup: &dyn Fn(&[u8]) -> Result<Option<TermValue>>,
) -> Result<Resolved> {
    Ok(match q {
        Query::All => Resolved::All,
        Query::Gram(gram) => match lookup(gram)? {
            Some(value) => {
                let (offset, count) = unpack_posting(value.packed);
                if count == 1 {
                    // Singleton grams inline their doc id in the offset
                    // field: no postings entry exists and none is fetched.
                    // An out-of-range id is a corrupt dictionary and must
                    // fail loudly — mapping it to an empty result would be
                    // a silent false negative. Checked before the ALL
                    // shortcut so one-document segments validate too.
                    let id = u32::try_from(offset)
                        .ok()
                        .filter(|id| *id < id_space)
                        .with_context(|| {
                            format!("singleton posting id {offset} is outside 0..{id_space}")
                        })?;
                    Resolved::Doc(id)
                } else if count >= id_space {
                    Resolved::All
                } else {
                    let len = value
                        .len
                        .unwrap_or_else(|| crate::posting_block_len(count, id_space));
                    Resolved::Gram { offset, count, len }
                }
            }
            None => Resolved::None,
        },
        Query::And(subs) => {
            let mut children = Vec::new();
            for sub in subs {
                match resolve(sub, id_space, strategy, lookup)? {
                    Resolved::None => return Ok(Resolved::None),
                    Resolved::All => {}
                    resolved => children.push(resolved),
                }
            }
            prune_and(children, strategy)
        }
        Query::Or(subs) => {
            let mut children = Vec::new();
            for sub in subs {
                match resolve(sub, id_space, strategy, lookup)? {
                    Resolved::All => return Ok(Resolved::All),
                    Resolved::None => {}
                    resolved => children.push(resolved),
                }
            }
            if children.len() == 1 {
                children.swap_remove(0)
            } else if children.is_empty() {
                Resolved::None
            } else {
                Resolved::Or(children)
            }
        }
    })
}

fn prune_and(children: Vec<Resolved>, strategy: Strategy) -> Resolved {
    // Any singleton child already bounds the AND to (at most) its one
    // document: every gram sibling becomes redundant work, so drop them all
    // and fetch no postings. Multiple singletons intersect for free.
    if children
        .iter()
        .any(|child| matches!(child, Resolved::Doc(_)))
    {
        let mut docs: Vec<Resolved> = children
            .into_iter()
            .filter(|child| !matches!(child, Resolved::Gram { .. }))
            .collect();
        return if docs.len() == 1 {
            docs.swap_remove(0)
        } else {
            Resolved::And(docs)
        };
    }
    let (mut grams, others): (Vec<_>, Vec<_>) = children
        .into_iter()
        .partition(|child| matches!(child, Resolved::Gram { .. }));
    let limit = match strategy {
        Strategy::Trigram => BLOCK_MAX_GRAMS_PER_AND,
        Strategy::Sparse => SPARSE_MAX_GRAMS_PER_AND,
    };
    if grams.len() > limit {
        grams.sort_by_key(|child| match child {
            Resolved::Gram { count, .. } => *count,
            _ => u32::MAX,
        });
        grams.truncate(limit);
    }
    grams.extend(others);
    if grams.len() == 1 {
        grams.swap_remove(0)
    } else if grams.is_empty() {
        Resolved::All
    } else {
        Resolved::And(grams)
    }
}

/// Collect every posting block the resolved query needs: offset -> doc count.
pub(crate) fn blocks_needed(resolved: &Resolved, out: &mut BTreeMap<u64, (u32, u64)>) {
    match resolved {
        Resolved::Gram { offset, count, len } => {
            out.insert(*offset, (*count, *len));
        }
        Resolved::And(children) | Resolved::Or(children) => {
            for child in children {
                blocks_needed(child, out);
            }
        }
        Resolved::All | Resolved::None | Resolved::Doc(_) => {}
    }
}

pub(crate) enum Selection {
    All,
    Ids(Vec<DocId>),
}

fn expand_ids(
    ids: &[DocId],
    expand: Option<&dyn Fn(DocId) -> RangeInclusive<DocId>>,
) -> Vec<DocId> {
    let Some(expand) = expand else {
        return ids.to_vec();
    };
    let mut expanded: Vec<DocId> = ids.iter().flat_map(|id| expand(*id)).collect();
    expanded.sort_unstable();
    expanded.dedup();
    expanded
}

/// Evaluate the resolved query over prefetched blocks. Pure set algebra on
/// sorted id vectors; no IO.
pub(crate) fn eval(
    resolved: &Resolved,
    blocks: &BTreeMap<u64, Vec<DocId>>,
    expand: Option<&dyn Fn(DocId) -> RangeInclusive<DocId>>,
) -> Result<Selection> {
    Ok(match resolved {
        Resolved::All => Selection::All,
        Resolved::None => Selection::Ids(Vec::new()),
        Resolved::Doc(id) => Selection::Ids(expand_ids(&[*id], expand)),
        Resolved::Gram { offset, .. } => {
            let ids = blocks
                .get(offset)
                .with_context(|| format!("posting block at offset {offset} was not fetched"))?;
            Selection::Ids(expand_ids(ids, expand))
        }
        Resolved::And(children) => {
            let mut sets = Vec::with_capacity(children.len());
            for child in children {
                match eval(child, blocks, expand)? {
                    Selection::All => {}
                    Selection::Ids(ids) => {
                        if ids.is_empty() {
                            return Ok(Selection::Ids(Vec::new()));
                        }
                        sets.push(ids);
                    }
                }
            }
            sets.sort_by_key(Vec::len);
            let mut sets = sets.into_iter();
            match sets.next() {
                None => Selection::All,
                Some(mut acc) => {
                    for set in sets {
                        acc = intersect(&acc, &set);
                        if acc.is_empty() {
                            break;
                        }
                    }
                    Selection::Ids(acc)
                }
            }
        }
        Resolved::Or(children) => {
            let mut lists = Vec::with_capacity(children.len());
            for child in children {
                match eval(child, blocks, expand)? {
                    Selection::All => return Ok(Selection::All),
                    Selection::Ids(ids) => lists.push(ids),
                }
            }
            Selection::Ids(union_many(lists))
        }
    })
}

/// Intersection of two sorted id slices.
fn intersect(a: &[DocId], b: &[DocId]) -> Vec<DocId> {
    let mut out = Vec::with_capacity(a.len().min(b.len()));
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                out.push(a[i]);
                i += 1;
                j += 1;
            }
        }
    }
    out
}

fn union_many(lists: Vec<Vec<DocId>>) -> Vec<DocId> {
    let mut out: Vec<DocId> = lists.into_iter().flatten().collect();
    out.sort_unstable();
    out.dedup();
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tv(packed: u64) -> TermValue {
        TermValue { packed, len: None }
    }

    fn gram(offset: u64, count: u32) -> Resolved {
        Resolved::Gram {
            offset,
            count,
            len: u64::from(count),
        }
    }

    #[test]
    fn pack_round_trips() {
        let packed = pack_posting(123_456, 789).unwrap();
        assert_eq!(unpack_posting(packed), (123_456, 789));
        assert!(pack_posting(1 << 40, 1).is_err());
        assert!(pack_posting(0, 1 << 24).is_err());
        // MAX_POSTING_COUNT is exactly the largest packable count: an id
        // space capped there keeps every gram encodable.
        assert!(pack_posting(0, usize::try_from(MAX_POSTING_COUNT).unwrap()).is_ok());
    }

    #[test]
    fn corrupt_singleton_ids_fail_loudly() {
        // count == 1 tags the offset as a doc id; an out-of-range id must
        // error, never silently shrink results.
        let lookup = |_: &[u8]| Ok(Some(tv(pack_posting(500, 1).expect("test setup failed"))));
        let error = match resolve(
            &Query::Gram(b"abc".to_vec()),
            100,
            Strategy::Sparse,
            &lookup,
        ) {
            Err(error) => error,
            Ok(_) => panic!("out-of-range singleton must error"),
        };
        assert!(error.to_string().contains("singleton"), "{error:#}");
        // One-document segments must validate too: count == 1 also satisfies
        // count >= doc_count, and the ALL shortcut must not mask corruption.
        assert!(resolve(&Query::Gram(b"abc".to_vec()), 1, Strategy::Sparse, &lookup).is_err());
        let valid = |_: &[u8]| Ok(Some(tv(pack_posting(0, 1).expect("test setup failed"))));
        assert!(matches!(
            resolve(&Query::Gram(b"abc".to_vec()), 1, Strategy::Sparse, &valid).expect("resolve"),
            Resolved::Doc(0)
        ));
    }

    #[test]
    fn absent_gram_collapses_and_to_none() {
        let q = Query::And(vec![
            Query::Gram(b"abc".to_vec()),
            Query::Gram(b"zzz".to_vec()),
        ]);
        let resolved = resolve(&q, 100, Strategy::Sparse, &|g: &[u8]| {
            Ok((g == b"abc").then(|| tv(pack_posting(0, 2).expect("test setup failed"))))
        })
        .expect("resolve");
        assert!(matches!(resolved, Resolved::None));
        let mut needed = BTreeMap::new();
        blocks_needed(&resolved, &mut needed);
        assert!(needed.is_empty());
    }

    #[test]
    fn dense_gram_resolves_to_all_without_fetch() {
        // a gram in every doc constrains nothing: All, no block needed
        let lookup = |_: &[u8]| Ok(Some(tv(pack_posting(64, 100).expect("test setup failed"))));
        let resolved = resolve(
            &Query::Gram(b"abc".to_vec()),
            100,
            Strategy::Sparse,
            &lookup,
        )
        .expect("resolve");
        assert!(matches!(resolved, Resolved::All));
        let mut needed = BTreeMap::new();
        blocks_needed(&resolved, &mut needed);
        assert!(needed.is_empty());
        // one doc short of dense -> still a real constraint
        let lookup = |_: &[u8]| Ok(Some(tv(pack_posting(64, 99).expect("test setup failed"))));
        assert!(matches!(
            resolve(
                &Query::Gram(b"abc".to_vec()),
                100,
                Strategy::Sparse,
                &lookup
            )
            .expect("resolve"),
            Resolved::Gram { count: 99, .. }
        ));
    }

    #[test]
    fn and_prunes_to_rarest_grams() {
        let children = (0..12u64)
            .map(|i| gram(i * 100, 1000 - i as u32))
            .collect::<Vec<_>>();
        let Resolved::And(kept) = prune_and(children, Strategy::Sparse) else {
            panic!("expected And");
        };
        assert_eq!(kept.len(), SPARSE_MAX_GRAMS_PER_AND);
        // The rarest grams (highest offsets here) survive.
        assert!(kept.iter().all(|child| match child {
            Resolved::Gram { count, .. } => *count <= 1000 - 4,
            _ => false,
        }));
    }

    #[test]
    fn block_and_keeps_up_to_sixty_four_grams() {
        let children = (0..80u64)
            .map(|offset| gram(offset, 1000 - offset as u32))
            .collect::<Vec<_>>();
        let Resolved::And(kept) = prune_and(children, Strategy::Trigram) else {
            panic!("expected And");
        };
        assert_eq!(kept.len(), BLOCK_MAX_GRAMS_PER_AND);
        assert!(kept.iter().all(|child| match child {
            Resolved::Gram { count, .. } => *count <= 1000 - 16,
            _ => false,
        }));
    }

    #[test]
    fn eval_and_or() {
        let blocks = BTreeMap::from([
            (0u64, vec![1u32, 3, 5, 8]),
            (16, vec![3, 5, 7]),
            (32, vec![9]),
        ]);
        let and = Resolved::And(vec![gram(0, 4), gram(16, 3)]);
        let Selection::Ids(ids) = eval(&and, &blocks, None).unwrap() else {
            panic!("expected ids");
        };
        assert_eq!(ids, vec![3, 5]);

        let or = Resolved::Or(vec![and, gram(32, 1)]);
        let Selection::Ids(ids) = eval(&or, &blocks, None).unwrap() else {
            panic!("expected ids");
        };
        assert_eq!(ids, vec![3, 5, 9]);
    }

    #[test]
    fn eval_all_passthrough() {
        let blocks = BTreeMap::new();
        assert!(matches!(
            eval(&Resolved::All, &blocks, None).unwrap(),
            Selection::All
        ));
        let and_of_all = Resolved::And(vec![Resolved::All]);
        assert!(matches!(
            eval(&and_of_all, &blocks, None).unwrap(),
            Selection::All
        ));
        let blocks = BTreeMap::from([(0u64, vec![1u32])]);
        let or_with_all = Resolved::Or(vec![gram(0, 1), Resolved::All]);
        assert!(matches!(
            eval(&or_with_all, &blocks, None).unwrap(),
            Selection::All
        ));
    }

    #[test]
    fn eval_expands_singletons_and_postings() {
        let blocks = BTreeMap::from([(0u64, vec![4u32, 8])]);
        let expand = |id: u32| id.saturating_sub(1)..=id + 1;
        let resolved = Resolved::Or(vec![Resolved::Doc(2), gram(0, 2)]);
        let Selection::Ids(ids) = eval(&resolved, &blocks, Some(&expand)).unwrap() else {
            panic!("expected ids");
        };
        assert_eq!(ids, vec![1, 2, 3, 4, 5, 7, 8, 9]);
    }
}
