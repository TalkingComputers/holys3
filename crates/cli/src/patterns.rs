//! Pattern composition: rg-style -e/-F/-w/-i/-S transforms applied BEFORE
//! the gram planner and the verifier, so both see the identical final regex.

/// THE pattern entry point: escapes (`-F`), resolves case (`-i`/`-S`),
/// composes (`-w`, multi-pattern OR, `(?m)`), and sanitizes line terminators
/// — in that order, so the mandatory sanitize step cannot be skipped at a
/// call site. Smart case analyzes the escaped forms, so `-F -S 'foo('`
/// stays insensitive like rg.
pub(crate) fn build_pattern(
    patterns: &[String],
    fixed_strings: bool,
    word_regexp: bool,
    ignore_case: bool,
    smart_case: bool,
) -> anyhow::Result<String> {
    let patterns: Vec<String> = if fixed_strings {
        patterns.iter().map(|p| regex::escape(p)).collect()
    } else {
        patterns.to_vec()
    };
    let insensitive = ignore_case || (smart_case && smart_case_insensitive(&patterns));
    sanitize_line_terminators(&compose_pattern(&patterns, word_regexp, insensitive))
}

/// `-w` wraps in word boundaries, multiple patterns OR, `-i` prepends
/// `(?i)`. The result may be an invalid regex — the sanitize parse reports
/// that with the real error.
fn compose_pattern(patterns: &[String], word_regexp: bool, insensitive: bool) -> String {
    let transformed: Vec<String> = patterns
        .iter()
        .map(|p| {
            if word_regexp {
                // rg uses HALF word boundaries: full \b anchors incorrectly when the
                // pattern's first/last char is a non-word char (`foo(`, `->`)
                format!(r"\b{{start-half}}(?:{p})\b{{end-half}}")
            } else {
                p.clone()
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

/// rg's line-terminator discipline, applied to the COMPOSED pattern: strip
/// \n out of every character class that matches it (this is what makes
/// `[^x]` and `(?s).` line-oriented — a class spanning \n would swallow
/// following lines into one phantom match), then reject any literal \n
/// left (a match can never span the line-oriented unit of output).
fn sanitize_line_terminators(pattern: &str) -> anyhow::Result<String> {
    use regex_syntax::hir::{Class, Hir, HirKind, Literal};
    fn strip(hir: &Hir) -> anyhow::Result<Hir> {
        Ok(match hir.kind() {
            HirKind::Literal(Literal(bytes)) => {
                anyhow::ensure!(
                    !bytes.contains(&b'\n'),
                    "the literal '\\n' is not allowed in a regex"
                );
                hir.clone()
            }
            HirKind::Class(Class::Bytes(class)) => {
                let mut class = class.clone();
                let mut newline =
                    regex_syntax::hir::ClassBytes::new([regex_syntax::hir::ClassBytesRange::new(
                        b'\n', b'\n',
                    )]);
                newline.negate();
                class.intersect(&newline);
                Hir::class(Class::Bytes(class))
            }
            HirKind::Class(Class::Unicode(class)) => {
                let mut class = class.clone();
                let mut newline = regex_syntax::hir::ClassUnicode::new([
                    regex_syntax::hir::ClassUnicodeRange::new('\n', '\n'),
                ]);
                newline.negate();
                class.intersect(&newline);
                Hir::class(Class::Unicode(class))
            }
            HirKind::Repetition(rep) => {
                let mut rep = rep.clone();
                rep.sub = Box::new(strip(&rep.sub)?);
                Hir::repetition(rep)
            }
            HirKind::Capture(cap) => {
                let mut cap = cap.clone();
                cap.sub = Box::new(strip(&cap.sub)?);
                Hir::capture(cap)
            }
            HirKind::Concat(subs) => {
                Hir::concat(subs.iter().map(strip).collect::<anyhow::Result<_>>()?)
            }
            HirKind::Alternation(subs) => {
                Hir::alternation(subs.iter().map(strip).collect::<anyhow::Result<_>>()?)
            }
            HirKind::Empty | HirKind::Look(_) => hir.clone(),
        })
    }
    let hir = regex_syntax::ParserBuilder::new()
        .utf8(false)
        .build()
        .parse(pattern)?;
    Ok(strip(&hir)?.to_string())
}

/// rg smart case: insensitive iff the patterns contain at least one literal
/// character and none of the literal characters are uppercase. Classes like
/// `\pL` are not literals. An unparsable raw pattern is "not lowercase" —
/// the sanitize parse surfaces the real error.
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
        assert_eq!(compose_pattern(&one(r"a\.b"), false, false), r"(?m)a\.b");
        assert_eq!(
            compose_pattern(&one("foo"), true, false),
            r"(?m)\b{start-half}(?:foo)\b{end-half}"
        );
        assert_eq!(
            compose_pattern(&["a".into(), "b".into()], false, false),
            "(?m)(?:a)|(?:b)"
        );
        assert_eq!(compose_pattern(&one("foo"), false, true), "(?mi)foo");
        assert_eq!(
            compose_pattern(&[r"a\.".into(), "b".into()], true, true),
            r"(?mi)(?:\b{start-half}(?:a\.)\b{end-half})|(?:\b{start-half}(?:b)\b{end-half})"
        );
    }

    #[test]
    fn build_pattern_escapes_fixed_strings() {
        let re = |p: &str, fixed: bool| {
            regex::bytes::Regex::new(&build_pattern(&one(p), fixed, false, false, false).unwrap())
                .unwrap()
        };
        assert!(re("a.b", true).is_match(b"a.b"));
        assert!(!re("a.b", true).is_match(b"axb"));
        assert!(re("a.b", false).is_match(b"axb"));
    }

    #[test]
    fn sanitize_strips_newline_from_classes_and_rejects_literals() {
        let re =
            |p: &str| regex::bytes::Regex::new(&sanitize_line_terminators(p).unwrap()).unwrap();
        // [^x] must not swallow the newline
        assert!(!re("[^x]").is_match(b"\n"));
        // (?s). is a class containing \n: stripped back to line-oriented
        assert!(!re("(?s).").is_match(b"\n"));
        assert!(re("(?s)a.b").is_match(b"axb"));
        assert!(!re("(?s)a.b").is_match(b"a\nb"));
        // \D and \W contain \n
        assert!(!re(r"\D").is_match(b"\n"));
        // literal newlines are rejected, in every spelling
        assert!(sanitize_line_terminators("a\nb").is_err());
        assert!(sanitize_line_terminators(r"a\nb").is_err());
        assert!(sanitize_line_terminators(r"a\x0Ab").is_err());
        // plain patterns round-trip
        assert!(re("(?m)^needle$").is_match(b"x\nneedle\ny"));
    }

    #[test]
    fn smart_case_rules() {
        assert!(smart_case_insensitive(&one("foo")));
        assert!(!smart_case_insensitive(&one("Foo")));
        assert!(smart_case_insensitive(&one(r"foo\pL")));
        assert!(!smart_case_insensitive(&one(r"Foo\pL")));
        assert!(!smart_case_insensitive(&one(r"\d+"))); // no literals
        assert!(!smart_case_insensitive(&one("(")));
        // an unparsable raw pattern errors out of build_pattern
        assert!(build_pattern(&one("("), false, false, false, true).is_err());
    }

    #[test]
    fn fixed_strings_smart_case_analyzes_escaped_pattern() {
        // rg parity: `rg -F -S 'foo('` matches `FOO(`
        let re = |p: &str| {
            regex::bytes::Regex::new(&build_pattern(&one(p), true, false, false, true).unwrap())
                .unwrap()
        };
        assert!(re("foo(").is_match(b"FOO("));
        assert!(re("Foo(").is_match(b"Foo("));
        assert!(!re("Foo(").is_match(b"FOO("));
    }
}
