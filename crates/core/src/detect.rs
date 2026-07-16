//! Content-class detection for automatic index-strategy selection.
//!
//! Trigram dictionaries are compact and selective on structured content
//! (code, logs, JSON) but collapse on natural-language prose, where every
//! common word's trigrams appear in nearly every document. Sparse
//! dictionaries invert the trade. The classifier below separates the two on
//! decoded text; thresholds were calibrated on real corpora (Gutenberg
//! books, the Linux kernel tree, GH Archive JSON): prose scored 100% of
//! bytes prose-like, the kernel tree 24% (its Documentation prose), JSON 0%.

/// Whether decoded content reads as natural-language prose. `None` when the
/// sample is too short to judge.
///
/// Only ASCII letters count as alphabetic, so non-Latin prose (CJK, heavy
/// diacritics) classifies as structured and gets the trigram strategy — the
/// conservative direction, since multi-byte scripts produce high-entropy
/// trigrams that prune well.
pub fn is_prose_like(data: &[u8]) -> Option<bool> {
    const MIN_SAMPLE_BYTES: usize = 256;
    if data.len() < MIN_SAMPLE_BYTES {
        return None;
    }
    let mut alpha = 0usize;
    let mut space = 0usize;
    let mut digit = 0usize;
    let mut structural = 0usize;
    for &byte in data {
        match byte {
            b'a'..=b'z' | b'A'..=b'Z' => alpha += 1,
            b' ' | b'\n' | b'\r' | b'\t' => space += 1,
            b'0'..=b'9' => digit += 1,
            b'{' | b'}' | b'[' | b']' | b'"' | b':' | b',' | b'=' | b'<' | b'>' | b'(' | b')'
            | b';' | b'_' | b'/' | b'\\' => structural += 1,
            _ => {}
        }
    }
    let len = data.len() as f64;
    Some(
        (alpha + space) as f64 / len >= 0.85
            && structural as f64 / len <= 0.04
            && digit as f64 / len <= 0.03,
    )
}

#[cfg(test)]
mod tests {
    use super::is_prose_like;

    #[test]
    fn classifies_real_content_shapes() {
        let prose = "It is a truth universally acknowledged, that a single man in \
                     possession of a good fortune, must be in want of a wife. However \
                     little known the feelings or views of such a man may be on his \
                     first entering a neighbourhood, this truth is so well fixed in \
                     the minds of the surrounding families, that he is considered as \
                     the rightful property of some one or other of their daughters."
            .repeat(2);
        assert_eq!(is_prose_like(prose.as_bytes()), Some(true));

        let json = r#"{"id":"4818103462","type":"PushEvent","actor":{"id":583231,"login":"octocat","display_login":"octocat","gravatar_id":"","url":"https://api.github.com/users/octocat"},"repo":{"id":1296269,"name":"octocat/Hello-World"},"payload":{"push_id":1558437314,"size":1,"distinct_size":1,"ref":"refs/heads/main"}}"#
            .repeat(2);
        assert_eq!(is_prose_like(json.as_bytes()), Some(false));

        let code = "static int vf610_mscm_ir_domain_alloc(struct irq_domain *domain, \
                    unsigned int virq,\n\tunsigned int nr_irqs, void *arg)\n{\n\tstruct \
                    irq_fwspec *fwspec = arg;\n\tstruct irq_fwspec parent_fwspec;\n\tint \
                    i, irq = fwspec->param[0];\n\n\tif (WARN_ON(nr_irqs != 1))\n\t\treturn \
                    -EINVAL;\n"
            .repeat(3);
        assert_eq!(is_prose_like(code.as_bytes()), Some(false));

        assert_eq!(is_prose_like(b"too short"), None);
    }
}
