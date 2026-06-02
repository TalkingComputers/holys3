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

pub fn plan(pattern: &str, strategy: Strategy) -> anyhow::Result<Query> {
    let hir = regex_syntax::parse(pattern)?;
    let seq = regex_syntax::hir::literal::Extractor::new().extract(&hir);
    match seq.literals() {
        None => Ok(Query::All),
        Some([]) => Ok(Query::All),
        Some(lits) => {
            let branches: Vec<Query> = lits
                .iter()
                .map(|l| lit_query(l.as_bytes(), strategy))
                .collect();
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
        let q = plan("handleClick", Strategy::Sparse).unwrap();
        assert!(matches!(q, Query::Or(_)));
        assert_ne!(q, Query::All);
    }
}
