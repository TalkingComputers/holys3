use holys3_core::trigrams;
use regex_syntax::hir::literal::Extractor;
use regex_syntax::hir::literal::Seq;
use std::collections::HashSet;

/// Boolean query over trigram keys. `All` = scan everything (no usable literal).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Query {
    All,
    None,
    And(Vec<Query>),
    Or(Vec<Query>),
    Trigram(u32),
}

/// AND of all trigrams in a literal >= 3 bytes; `All` for shorter literals.
fn lit_query(lit: &[u8]) -> Query {
    let tg = trigrams(lit);
    if tg.is_empty() {
        Query::All
    } else {
        Query::And(tg.into_iter().map(Query::Trigram).collect())
    }
}

/// Decompose a regex into a conservative trigram query.
/// Uses prefix literals: any match starts with one of them, so their trigrams
/// must be present in any matching document (no false negatives). When literals
/// are unavailable/insufficient, returns `All`.
pub fn plan(pattern: &str) -> anyhow::Result<Query> {
    let hir = regex_syntax::parse(pattern)?;
    let seq: Seq = Extractor::new().extract(&hir);
    match seq.literals() {
        None => Ok(Query::All), // infinite / unbounded
        Some([]) => Ok(Query::All),
        Some(lits) => {
            let mut branches = Vec::new();
            for l in lits {
                branches.push(lit_query(l.as_bytes()));
            }
            // OR across alternatives; if any branch is All, the whole thing is All.
            if branches.contains(&Query::All) {
                Ok(Query::All)
            } else {
                Ok(Query::Or(branches))
            }
        }
    }
}

/// Evaluate a query against a document's trigram set (for tests + in-memory eval).
pub fn matches_trigrams(q: &Query, doc: &HashSet<u32>) -> bool {
    match q {
        Query::All => true,
        Query::None => false,
        Query::Trigram(t) => doc.contains(t),
        Query::And(subs) => subs.iter().all(|s| matches_trigrams(s, doc)),
        Query::Or(subs) => subs.iter().any(|s| matches_trigrams(s, doc)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tg(s: &str) -> u32 {
        let b = s.as_bytes();
        (b[0] as u32) << 16 | (b[1] as u32) << 8 | b[2] as u32
    }

    #[test]
    fn literal_becomes_and_of_trigrams() {
        let q = plan("world").unwrap();
        // "world" -> wor, orl, rld
        let expected = Query::Or(vec![Query::And(vec![
            Query::Trigram(tg("orl")),
            Query::Trigram(tg("rld")),
            Query::Trigram(tg("wor")),
        ])]);
        // order-independent compare: both reduce to the same trigram set
        let set: HashSet<u32> = match &q {
            Query::Or(b) => match &b[0] {
                Query::And(ts) => ts
                    .iter()
                    .map(|t| if let Query::Trigram(x) = t { *x } else { 0 })
                    .collect(),
                _ => panic!(),
            },
            _ => panic!("{q:?}"),
        };
        assert_eq!(set, HashSet::from([tg("wor"), tg("orl"), tg("rld")]));
        let _ = expected;
    }

    #[test]
    fn unanchored_wildcard_is_all() {
        assert_eq!(plan(".*").unwrap(), Query::All);
    }

    #[test]
    fn short_literal_is_all() {
        // "ab" has no trigram
        assert_eq!(plan("ab").unwrap(), Query::All);
    }
}
