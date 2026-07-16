//! Pure query evaluation over prefetched posting blocks.
//!
//! The fst value packs a posting block's byte offset and doc count, so the
//! whole pipeline is: resolve grams against the fst (no IO) -> prune -> fetch
//! every needed block in one concurrent batch -> evaluate set algebra over
//! sorted id vectors.

use anyhow::{Context, Result};
use holys3_core::DocId;
use holys3_query::Query;
use std::collections::BTreeMap;

/// Keep at most this many (rarest) gram constraints per AND group. Extra
/// grams only narrow an already-small candidate set; dropping them keeps the
/// result a superset of the true matches, which regex verification filters.
/// This bounds index round trips per query regardless of literal length.
const MAX_GRAMS_PER_AND: usize = 8;

const OFFSET_BITS: u32 = 40;
const COUNT_BITS: u32 = 24;

/// Pack a posting block's byte offset (40 bits, 1 TiB) and doc count
/// (24 bits, 16.7M docs) into one fst value.
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

/// A query with every gram resolved against the term dictionary.
pub(crate) enum Resolved {
    All,
    None,
    /// A singleton gram: exactly one document, id inlined in the term value.
    Doc(DocId),
    Gram {
        offset: u64,
        count: u32,
    },
    And(Vec<Resolved>),
    Or(Vec<Resolved>),
}

/// Resolve grams via `lookup` (an fst get) and simplify: absent grams make
/// their AND empty without any postings fetch; ALL branches collapse. A gram
/// present in every doc (`count == doc_count`) constrains nothing, so it
/// resolves to ALL and its posting block is never fetched.
pub(crate) fn resolve(
    q: &Query,
    doc_count: u32,
    lookup: &dyn Fn(&[u8]) -> Result<Option<u64>>,
) -> Result<Resolved> {
    Ok(match q {
        Query::All => Resolved::All,
        Query::Gram(gram) => match lookup(gram)? {
            Some(value) => {
                let (offset, count) = unpack_posting(value);
                if count == 1 {
                    // Singleton grams inline their doc id in the offset
                    // field: no postings entry exists and none is fetched.
                    // An out-of-range id is a corrupt dictionary and must
                    // fail loudly — mapping it to an empty result would be
                    // a silent false negative. Checked before the ALL
                    // shortcut so one-document segments validate too.
                    let id = u32::try_from(offset)
                        .ok()
                        .filter(|id| *id < doc_count)
                        .with_context(|| {
                            format!("singleton doc id {offset} is outside 0..{doc_count}")
                        })?;
                    Resolved::Doc(id)
                } else if count >= doc_count {
                    Resolved::All
                } else {
                    Resolved::Gram { offset, count }
                }
            }
            None => Resolved::None,
        },
        Query::And(subs) => {
            let mut children = Vec::new();
            for sub in subs {
                match resolve(sub, doc_count, lookup)? {
                    Resolved::None => return Ok(Resolved::None),
                    Resolved::All => {}
                    resolved => children.push(resolved),
                }
            }
            prune_and(children)
        }
        Query::Or(subs) => {
            let mut children = Vec::new();
            for sub in subs {
                match resolve(sub, doc_count, lookup)? {
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

fn prune_and(children: Vec<Resolved>) -> Resolved {
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
    if grams.len() > MAX_GRAMS_PER_AND {
        grams.sort_by_key(|child| match child {
            Resolved::Gram { count, .. } => *count,
            _ => u32::MAX,
        });
        grams.truncate(MAX_GRAMS_PER_AND);
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
pub(crate) fn blocks_needed(resolved: &Resolved, out: &mut BTreeMap<u64, u32>) {
    match resolved {
        Resolved::Gram { offset, count } => {
            out.insert(*offset, *count);
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

/// Evaluate the resolved query over prefetched blocks. Pure set algebra on
/// sorted id vectors; no IO.
pub(crate) fn eval(resolved: &Resolved, blocks: &BTreeMap<u64, Vec<DocId>>) -> Result<Selection> {
    Ok(match resolved {
        Resolved::All => Selection::All,
        Resolved::None => Selection::Ids(Vec::new()),
        Resolved::Doc(id) => Selection::Ids(vec![*id]),
        Resolved::Gram { offset, .. } => Selection::Ids(
            blocks
                .get(offset)
                .with_context(|| format!("posting block at offset {offset} was not fetched"))?
                .clone(),
        ),
        Resolved::And(children) => {
            let mut sets = Vec::with_capacity(children.len());
            for child in children {
                match eval(child, blocks)? {
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
                match eval(child, blocks)? {
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

    fn gram(offset: u64, count: u32) -> Resolved {
        Resolved::Gram { offset, count }
    }

    #[test]
    fn pack_round_trips() {
        let packed = pack_posting(123_456, 789).unwrap();
        assert_eq!(unpack_posting(packed), (123_456, 789));
        assert!(pack_posting(1 << 40, 1).is_err());
        assert!(pack_posting(0, 1 << 24).is_err());
    }

    #[test]
    fn corrupt_singleton_ids_fail_loudly() {
        // count == 1 tags the offset as a doc id; an out-of-range id must
        // error, never silently shrink results.
        let lookup = |_: &[u8]| Ok(Some(pack_posting(500, 1).expect("test setup failed")));
        let error = match resolve(&Query::Gram(b"abc".to_vec()), 100, &lookup) {
            Err(error) => error,
            Ok(_) => panic!("out-of-range singleton must error"),
        };
        assert!(error.to_string().contains("singleton"), "{error:#}");
        // One-document segments must validate too: count == 1 also satisfies
        // count >= doc_count, and the ALL shortcut must not mask corruption.
        assert!(resolve(&Query::Gram(b"abc".to_vec()), 1, &lookup).is_err());
        let valid = |_: &[u8]| Ok(Some(pack_posting(0, 1).expect("test setup failed")));
        assert!(matches!(
            resolve(&Query::Gram(b"abc".to_vec()), 1, &valid).expect("resolve"),
            Resolved::Doc(0)
        ));
    }

    #[test]
    fn absent_gram_collapses_and_to_none() {
        let q = Query::And(vec![
            Query::Gram(b"abc".to_vec()),
            Query::Gram(b"zzz".to_vec()),
        ]);
        let resolved = resolve(&q, 100, &|g: &[u8]| {
            Ok((g == b"abc").then(|| pack_posting(0, 2).expect("test setup failed")))
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
        let lookup = |_: &[u8]| Ok(Some(pack_posting(64, 100).expect("test setup failed")));
        let resolved = resolve(&Query::Gram(b"abc".to_vec()), 100, &lookup).expect("resolve");
        assert!(matches!(resolved, Resolved::All));
        let mut needed = BTreeMap::new();
        blocks_needed(&resolved, &mut needed);
        assert!(needed.is_empty());
        // one doc short of dense -> still a real constraint
        let lookup = |_: &[u8]| Ok(Some(pack_posting(64, 99).expect("test setup failed")));
        assert!(matches!(
            resolve(&Query::Gram(b"abc".to_vec()), 100, &lookup).expect("resolve"),
            Resolved::Gram { count: 99, .. }
        ));
    }

    #[test]
    fn and_prunes_to_rarest_grams() {
        let children = (0..12u64)
            .map(|i| gram(i * 100, 1000 - i as u32))
            .collect::<Vec<_>>();
        let Resolved::And(kept) = prune_and(children) else {
            panic!("expected And");
        };
        assert_eq!(kept.len(), MAX_GRAMS_PER_AND);
        // The rarest grams (highest offsets here) survive.
        assert!(kept.iter().all(|child| match child {
            Resolved::Gram { count, .. } => *count <= 1000 - 4,
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
        let Selection::Ids(ids) = eval(&and, &blocks).unwrap() else {
            panic!("expected ids");
        };
        assert_eq!(ids, vec![3, 5]);

        let or = Resolved::Or(vec![and, gram(32, 1)]);
        let Selection::Ids(ids) = eval(&or, &blocks).unwrap() else {
            panic!("expected ids");
        };
        assert_eq!(ids, vec![3, 5, 9]);
    }

    #[test]
    fn eval_all_passthrough() {
        let blocks = BTreeMap::new();
        assert!(matches!(
            eval(&Resolved::All, &blocks).unwrap(),
            Selection::All
        ));
        let and_of_all = Resolved::And(vec![Resolved::All]);
        assert!(matches!(
            eval(&and_of_all, &blocks).unwrap(),
            Selection::All
        ));
        let blocks = BTreeMap::from([(0u64, vec![1u32])]);
        let or_with_all = Resolved::Or(vec![gram(0, 1), Resolved::All]);
        assert!(matches!(
            eval(&or_with_all, &blocks).unwrap(),
            Selection::All
        ));
    }
}
