#![allow(dead_code)]

use holys3_core::{testutil::MemCorpus, DocId};

pub(crate) const PATTERNS: &[&str] = &[
    "world",
    "handleClick",
    "quick.*fox",
    "EMAIL",
    r"\w+@\w+",
    ".*",
    "zzzznotpresent",
    "ab",
    "second line",
];

pub(crate) fn corpus() -> MemCorpus {
    let bodies: Vec<&[u8]> = vec![
        b"fn handleClick() { return 42; }",
        b"the quick brown fox",
        b"hello world\nsecond line with world",
        b"nothing interesting",
        b"EMAIL: a@b.com and c@d.org",
        b"",
        b"\xff\xfe binary-ish \x00 bytes world",
    ];
    let docs = (0..bodies.len())
        .map(|i| (i as DocId, format!("doc{i}")))
        .collect();
    MemCorpus::new(docs, bodies.into_iter().map(|b| b.to_vec()).collect())
}
