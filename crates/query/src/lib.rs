use holys3_core::sparse_grams_covering_bytes;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Query {
    All,
    None,
    And(Vec<Query>),
    Or(Vec<Query>),
    Gram(Vec<u8>),
}

fn lit_query(lit: &[u8]) -> Query {
    let grams = sparse_grams_covering_bytes(lit);
    if grams.is_empty() {
        Query::All
    } else {
        Query::And(grams.into_iter().map(Query::Gram).collect())
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
