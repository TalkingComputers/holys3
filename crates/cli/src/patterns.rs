//! Pattern composition: rg-style -e/-F/-w/-i/-S transforms applied BEFORE
//! the gram planner and the verifier, so both see the identical final regex.

/// `-F` escapes, `-w` wraps in word boundaries, multiple patterns OR, `-i`
/// prepends `(?i)`. The result may be an invalid regex — `Regex::new`/`plan`
/// report that with the real error.
pub(crate) fn compose_pattern(
    patterns: &[String],
    fixed_strings: bool,
    word_regexp: bool,
    insensitive: bool,
) -> String {
    let transformed: Vec<String> = patterns
        .iter()
        .map(|p| {
            let p = if fixed_strings {
                regex::escape(p)
            } else {
                p.clone()
            };
            if word_regexp {
                format!(r"\b(?:{p})\b")
            } else {
                p
            }
        })
        .collect();
    let joined = if transformed.len() == 1 {
        transformed.into_iter().next().expect("one pattern")
    } else {
        transformed
            .iter()
            .map(|p| format!("(?:{p})"))
            .collect::<Vec<_>>()
            .join("|")
    };
    // (?m): rg matches line-by-line, so ^ and $ anchor at EVERY line
    // boundary, not just the start and end of the object
    if insensitive {
        format!("(?mi){joined}")
    } else {
        format!("(?m){joined}")
    }
}

/// rg rejects literal line terminators in patterns: a match can never span
/// the line-oriented engine's unit of output, so the result would silently
/// truncate at the first line.
pub(crate) fn reject_line_terminators(patterns: &[String]) -> anyhow::Result<()> {
    for pattern in patterns {
        anyhow::ensure!(
            !pattern.contains('\n'),
            "the literal '\\n' is not allowed in a regex"
        );
    }
    Ok(())
}

/// Resolve the -i/-S/-s trio (clap guarantees at most one is set).
pub(crate) fn is_insensitive(ignore_case: bool, smart_case: bool, patterns: &[String]) -> bool {
    if ignore_case {
        return true;
    }
    if smart_case {
        return smart_case_insensitive(patterns);
    }
    false
}

/// rg smart case: insensitive iff the patterns contain at least one literal
/// character and none of the literal characters are uppercase. Classes like
/// `\pL` are not literals.
fn smart_case_insensitive(patterns: &[String]) -> bool {
    let mut literals = Vec::new();
    for pattern in patterns {
        let Ok(hir) = regex_syntax::parse(pattern) else {
            return false; // compile will surface the real error
        };
        collect_literal_chars(&hir, &mut literals);
    }
    !literals.is_empty() && !literals.iter().any(|c| c.is_uppercase())
}

fn collect_literal_chars(hir: &regex_syntax::hir::Hir, out: &mut Vec<char>) {
    use regex_syntax::hir::HirKind;
    match hir.kind() {
        HirKind::Literal(lit) => out.extend(String::from_utf8_lossy(&lit.0).chars()),
        HirKind::Concat(children) | HirKind::Alternation(children) => {
            for child in children {
                collect_literal_chars(child, out);
            }
        }
        HirKind::Capture(capture) => collect_literal_chars(&capture.sub, out),
        HirKind::Repetition(rep) => collect_literal_chars(&rep.sub, out),
        HirKind::Empty | HirKind::Class(_) | HirKind::Look(_) => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn one(p: &str) -> Vec<String> {
        vec![p.to_owned()]
    }

    #[test]
    fn compose_matrix() {
        assert_eq!(
            compose_pattern(&one("a.b"), true, false, false),
            r"(?m)a\.b"
        );
        assert_eq!(
            compose_pattern(&one("foo"), false, true, false),
            r"(?m)\b(?:foo)\b"
        );
        assert_eq!(
            compose_pattern(&["a".into(), "b".into()], false, false, false),
            "(?m)(?:a)|(?:b)"
        );
        assert_eq!(compose_pattern(&one("foo"), false, false, true), "(?mi)foo");
        assert_eq!(
            compose_pattern(&["a.".into(), "b".into()], true, true, true),
            r"(?mi)(?:\b(?:a\.)\b)|(?:\b(?:b)\b)"
        );
    }

    #[test]
    fn smart_case_rules() {
        assert!(is_insensitive(false, true, &one("foo")));
        assert!(!is_insensitive(false, true, &one("Foo")));
        assert!(is_insensitive(false, true, &one(r"foo\pL")));
        assert!(!is_insensitive(false, true, &one(r"Foo\pL")));
        assert!(!is_insensitive(false, true, &one(r"\d+"))); // no literals
        assert!(!is_insensitive(false, true, &one("(")));
        assert!(is_insensitive(true, false, &one("FOO")));
        assert!(!is_insensitive(false, false, &one("foo")));
    }
}
