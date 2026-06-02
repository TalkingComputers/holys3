use holys3_core::extract_sparse_ngrams_covering;
use std::collections::HashSet;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Query {
    All,
    None,
    And(Vec<Query>),
    Or(Vec<Query>),
    Gram(u64),
}

/// AND of the covering sparse-gram hashes of a literal; `All` if it yields none
/// (literal under 2 bytes => no grams => cannot constrain).
fn lit_query(lit: &[u8]) -> Query {
    let grams = extract_sparse_ngrams_covering(lit);
    if grams.is_empty() {
        Query::All
    } else {
        Query::And(grams.into_iter().map(|(h, _)| Query::Gram(h)).collect())
    }
}

pub fn plan(pattern: &str) -> anyhow::Result<Query> {
    let hir = regex_syntax::parse(pattern)?;
    let seq = regex_syntax::hir::literal::Extractor::new().extract(&hir);
    match seq.literals() {
        None => Ok(Query::All),
        Some([]) => Ok(Query::All),
        Some(lits) => {
            let branches: Vec<Query> = lits.iter().map(|l| lit_query(l.as_bytes())).collect();
            if branches.contains(&Query::All) {
                Ok(Query::All)
            } else {
                Ok(Query::Or(branches))
            }
        }
    }
}

pub fn matches_grams(q: &Query, doc: &HashSet<u64>) -> bool {
    match q {
        Query::All => true,
        Query::None => false,
        Query::Gram(h) => doc.contains(h),
        Query::And(subs) => subs.iter().all(|s| matches_grams(s, doc)),
        Query::Or(subs) => subs.iter().any(|s| matches_grams(s, doc)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wildcard_is_all() {
        assert_eq!(plan(".*").unwrap(), Query::All);
    }

    #[test]
    fn single_char_literal_is_all() {
        // one byte -> no pair -> no gram -> All
        assert_eq!(plan("a").unwrap(), Query::All);
    }

    #[test]
    fn literal_yields_gram_conjunction() {
        // a real literal produces an OR of one AND-of-grams (not All, not None)
        let q = plan("handleClick").unwrap();
        assert!(matches!(q, Query::Or(_)));
        assert_ne!(q, Query::All);
    }
}
