#![cfg_attr(docsrs, feature(doc_auto_cfg))]
//! Regex query planning for holys3 indexes.

use holys3_core::{grams_query, Strategy};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Query {
    All,
    None,
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
    let lits = seq.literals()?;
    if lits.is_empty() {
        return None;
    }
    let branches: Vec<Query> = lits
        .iter()
        .map(|l| lit_query(l.as_bytes(), strategy))
        .collect();
    if branches.contains(&Query::All) {
        None
    } else {
        Some(Query::Or(branches))
    }
}

/// Every match must START with one of the prefix literals AND END with one
/// of the suffix literals, so both sides' grams are necessary conditions —
/// `foo.*bar` constrains on `foo` grams AND `bar` grams, not `foo` alone.
/// (Inexact literals still appear verbatim inside every match, which is all
/// the gram constraint needs; candidates remain a strict superset.)
pub fn plan(pattern: &str, strategy: Strategy) -> anyhow::Result<Query> {
    use regex_syntax::hir::literal::{ExtractKind, Extractor};
    let hir = regex_syntax::parse(pattern)?;
    let prefixes = Extractor::new().extract(&hir);
    let mut suffix_extractor = Extractor::new();
    suffix_extractor.kind(ExtractKind::Suffix);
    let suffixes = suffix_extractor.extract(&hir);
    let mut sides: Vec<Query> = [prefixes, suffixes]
        .iter()
        .filter_map(|seq| side_query(seq, strategy))
        .collect();
    sides.dedup();
    match sides.len() {
        0 => Ok(Query::All),
        1 => Ok(sides.swap_remove(0)),
        _ => Ok(Query::And(sides)),
    }
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
            Query::All | Query::None => Vec::new(),
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
}
