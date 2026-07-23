//! Pattern composition: rg-style -e/-F/-w/-i/-S transforms applied BEFORE
//! the gram planner and the verifier, so both see the identical final regex.

use anyhow::Context;

/// THE pattern entry point: escapes (`-F`), resolves case (`-i`/`-S`),
/// composes (`-w`, `(?m)`), and sanitizes line terminators — per pattern, in
/// that order, so the mandatory sanitize step cannot be skipped at a call
/// site. Patterns are never joined: the engine plans and verifies each HIR
/// independently and unions the results. Smart case analyzes the escaped
/// forms, so `-F -S 'foo('` stays insensitive like rg.
pub(crate) fn build_patterns(
    patterns: &[String],
    fixed_strings: bool,
    word_regexp: bool,
    ignore_case: bool,
    smart_case: bool,
) -> anyhow::Result<Vec<regex_syntax::hir::Hir>> {
    anyhow::ensure!(!patterns.is_empty(), "at least one pattern is required");
    let escaped: Vec<String> = if fixed_strings {
        patterns.iter().map(|p| regex::escape(p)).collect()
    } else {
        patterns.to_vec()
    };
    let escaped_hirs = escaped
        .iter()
        .zip(patterns)
        .enumerate()
        .map(|(index, (escaped, raw))| {
            seagrep_core::parse_pattern(escaped)
                .with_context(|| format!("invalid pattern {} {raw:?}", index + 1))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let insensitive = ignore_case || (smart_case && smart_case_insensitive(&escaped_hirs));
    escaped
        .iter()
        .zip(patterns)
        .enumerate()
        .map(|(index, (escaped, raw))| {
            let composed = compose_pattern(escaped, word_regexp, insensitive);
            seagrep_core::parse_pattern(&composed)
                .and_then(|hir| sanitize_line_terminators(&hir))
                .with_context(|| format!("invalid pattern {} {raw:?}", index + 1))
        })
        .collect()
}

/// `-w` wraps in word boundaries, `-i` selects `(?mi)`. The result may be
/// an invalid regex — the sanitize parse reports that with the real error.
fn compose_pattern(pattern: &str, word_regexp: bool, insensitive: bool) -> String {
    let wrapped = if word_regexp {
        // rg uses HALF word boundaries: full \b anchors incorrectly when the
        // pattern's first/last char is a non-word char (`foo(`, `->`)
        format!(r"\b{{start-half}}(?:{pattern})\b{{end-half}}")
    } else {
        pattern.to_owned()
    };
    // (?m): rg matches line-by-line, so ^ and $ anchor at EVERY line
    // boundary, not just the start and end of the object
    if insensitive {
        format!("(?mi){wrapped}")
    } else {
        format!("(?m){wrapped}")
    }
}

/// rg's line-terminator discipline: strip \n out of every character class
/// that matches it (this is what makes `[^x]` and `(?s).` line-oriented — a
/// class spanning \n would swallow following lines into one phantom match),
/// then reject any literal \n left (a match can never span the line-oriented
/// unit of output). Returns the sanitized HIR directly; it is never
/// stringified back into a pattern.
fn sanitize_line_terminators(
    hir: &regex_syntax::hir::Hir,
) -> anyhow::Result<regex_syntax::hir::Hir> {
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
    strip(hir)
}

/// rg smart case: insensitive iff the patterns contain at least one literal
/// character and none of the literal characters are uppercase. Classes like
/// `\pL` are not literals. Unparsable patterns never reach here —
/// `build_patterns` has already returned their indexed parse error.
fn smart_case_insensitive(hirs: &[regex_syntax::hir::Hir]) -> bool {
    let mut literals = Vec::new();
    for hir in hirs {
        collect_literal_chars(hir, &mut literals);
    }
    !literals.is_empty() && !literals.iter().any(|c| c.is_uppercase())
}

fn collect_literal_chars(hir: &regex_syntax::hir::Hir, output: &mut Vec<char>) {
    use regex_syntax::hir::HirKind;
    match hir.kind() {
        HirKind::Literal(lit) => output.extend(String::from_utf8_lossy(&lit.0).chars()),
        HirKind::Concat(children) | HirKind::Alternation(children) => {
            for child in children {
                collect_literal_chars(child, output);
            }
        }
        HirKind::Capture(capture) => collect_literal_chars(&capture.sub, output),
        HirKind::Repetition(rep) => collect_literal_chars(&rep.sub, output),
        HirKind::Empty | HirKind::Class(_) | HirKind::Look(_) => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use seagrep_core::PatternProgram;

    fn one(p: &str) -> Vec<String> {
        vec![p.to_owned()]
    }

    fn is_match(hir: &regex_syntax::hir::Hir, haystack: &[u8]) -> bool {
        let program = PatternProgram::compile(std::slice::from_ref(hir), &[0]).unwrap();
        let mut cache = program.create_cache();
        program.find_iter(&mut cache, haystack).next().is_some()
    }

    #[test]
    fn builds_one_ordered_hir_per_pattern() {
        let hirs =
            build_patterns(&["beta".into(), "alpha".into()], false, false, false, false).unwrap();
        assert_eq!(hirs.len(), 2);
        let program = PatternProgram::compile(&hirs, &[0, 1]).unwrap();
        let mut cache = program.create_cache();
        let matched: Vec<usize> = program
            .find_iter(&mut cache, b"alpha then beta")
            .map(|matched| matched.pattern)
            .collect();
        assert_eq!(matched, vec![1, 0]);
    }

    #[test]
    fn compose_wraps_word_boundaries_and_flags() {
        assert_eq!(compose_pattern(r"a\.b", false, false), r"(?m)a\.b");
        assert_eq!(
            compose_pattern("foo", true, false),
            r"(?m)\b{start-half}(?:foo)\b{end-half}"
        );
        assert_eq!(compose_pattern("foo", false, true), "(?mi)foo");
        assert_eq!(
            compose_pattern(r"a\.", true, true),
            r"(?mi)\b{start-half}(?:a\.)\b{end-half}"
        );
    }

    #[test]
    fn build_patterns_escapes_fixed_strings() {
        let build = |p: &str, fixed: bool| {
            build_patterns(&one(p), fixed, false, false, false)
                .unwrap()
                .remove(0)
        };
        assert!(is_match(&build("a.b", true), b"a.b"));
        assert!(!is_match(&build("a.b", true), b"axb"));
        assert!(is_match(&build("a.b", false), b"axb"));
    }

    #[test]
    fn parse_errors_carry_one_based_pattern_context() {
        let error =
            build_patterns(&["fine".into(), "(".into()], false, false, false, false).unwrap_err();
        assert!(
            format!("{error:#}").starts_with("invalid pattern 2 \"(\": "),
            "{error:#}"
        );
        assert_eq!(
            build_patterns(&[], false, false, false, false)
                .unwrap_err()
                .to_string(),
            "at least one pattern is required"
        );
    }

    #[test]
    fn sanitize_strips_newline_from_classes_and_rejects_literals() {
        let build = |p: &str| {
            build_patterns(&one(p), false, false, false, false)
                .unwrap()
                .remove(0)
        };
        // [^x] must not swallow the newline
        assert!(!is_match(&build("[^x]"), b"\n"));
        // (?s). is a class containing \n: stripped back to line-oriented
        assert!(!is_match(&build("(?s)."), b"\n"));
        assert!(is_match(&build("(?s)a.b"), b"axb"));
        assert!(!is_match(&build("(?s)a.b"), b"a\nb"));
        // \D and \W contain \n
        assert!(!is_match(&build(r"\D"), b"\n"));
        // literal newlines are rejected, in every spelling
        assert!(build_patterns(&one("a\nb"), false, false, false, false).is_err());
        assert!(build_patterns(&one(r"a\nb"), false, false, false, false).is_err());
        assert!(build_patterns(&one(r"a\x0Ab"), false, false, false, false).is_err());
        // plain patterns round-trip
        assert!(is_match(&build("(?m)^needle$"), b"x\nneedle\ny"));
    }

    #[test]
    fn smart_case_rules() {
        let analyzed = |p: &str| {
            let hir = seagrep_core::parse_pattern(p).unwrap();
            smart_case_insensitive(std::slice::from_ref(&hir))
        };
        assert!(analyzed("foo"));
        assert!(!analyzed("Foo"));
        assert!(analyzed(r"foo\pL"));
        assert!(!analyzed(r"Foo\pL"));
        assert!(!analyzed(r"\d+")); // no literals
                                    // an unparsable raw pattern errors out of build_patterns with context
        let error = build_patterns(&one("("), false, false, false, true).unwrap_err();
        assert!(format!("{error:#}").starts_with("invalid pattern 1 \"(\": "));
    }

    #[test]
    fn fixed_strings_smart_case_analyzes_escaped_pattern() {
        // rg parity: `rg -F -S 'foo('` matches `FOO(`
        let build = |p: &str| {
            build_patterns(&one(p), true, false, false, true)
                .unwrap()
                .remove(0)
        };
        assert!(is_match(&build("foo("), b"FOO("));
        assert!(is_match(&build("Foo("), b"Foo("));
        assert!(!is_match(&build("Foo("), b"FOO("));
    }
}
