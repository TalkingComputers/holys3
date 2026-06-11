#![allow(dead_code)]

use holys3_core::{decode_body, testutil::MemCorpus, Corpus, DocId};
use std::io::Write;

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
    ".*quick.*",
    ".*(quick|world).*",
    ".*(quick)+.*",
    ".*(zzzznotpresent)?.*",
];

fn bodies() -> Vec<Vec<u8>> {
    let bodies: Vec<&[u8]> = vec![
        b"fn handleClick() { return 42; }",
        b"the quick brown fox",
        b"hello world\nsecond line with world",
        b"nothing interesting",
        b"EMAIL: a@b.com and c@d.org",
        b"",
        b"\xff\xfe binary-ish \x00 bytes world",
    ];
    bodies.into_iter().map(|b| b.to_vec()).collect()
}

fn docs_for(bodies: &[Vec<u8>]) -> Vec<(DocId, String)> {
    (0..bodies.len())
        .map(|i| (i as DocId, format!("doc{i}")))
        .collect()
}

pub(crate) fn corpus() -> MemCorpus {
    let bodies = bodies();
    MemCorpus::new(docs_for(&bodies), bodies)
}

fn gzip(data: &[u8]) -> Vec<u8> {
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    enc.write_all(data).unwrap();
    enc.finish().unwrap()
}

/// Same corpus with every other body gzip-compressed (one of them
/// multi-member, like real AWS log deliveries).
pub(crate) fn gzipped_corpus() -> MemCorpus {
    let bodies = bodies()
        .into_iter()
        .enumerate()
        .map(|(i, body)| match i % 2 {
            0 => body,
            _ if i == 1 => {
                let (a, b) = body.split_at(body.len() / 2);
                let mut multi = gzip(a);
                multi.extend(gzip(b));
                multi
            }
            _ => gzip(&body),
        })
        .collect::<Vec<_>>();
    MemCorpus::new(docs_for(&bodies), bodies)
}

/// Same corpus with a different format per doc — every supported codec plus
/// parquet and avro projections carrying the same searchable text.
pub(crate) fn encoded_corpus() -> MemCorpus {
    use holys3_core::testutil::encode;
    let mut bodies: Vec<Vec<u8>> = bodies()
        .into_iter()
        .enumerate()
        .map(|(i, body)| match i % 7 {
            0 => body,
            1 => encode::gzip(&body),
            2 => encode::zstd(&body),
            3 => encode::bzip2(&body),
            4 => encode::snappy_frame(&body),
            5 => encode::lz4_frame(&body),
            _ => encode::xz(&body),
        })
        .collect();
    bodies.push(encode::parquet_of_lines(&[
        "the quick brown fox in parquet",
        "hello world from a parquet row",
    ]));
    bodies.push(encode::avro_of_lines(&[
        "EMAIL: avro@example.com",
        "second line with world in avro",
    ]));
    MemCorpus::new(docs_for(&bodies), bodies)
}

/// The corpus as plain text: what searches must behave as if they saw.
pub(crate) fn decoded_corpus(c: &dyn Corpus) -> MemCorpus {
    let docs = c.docs().to_vec();
    let bodies = docs
        .iter()
        .map(|&(id, ref key)| decode_body(key, c.fetch(id).unwrap()).unwrap())
        .collect();
    MemCorpus::new(docs, bodies)
}
