use criterion::{criterion_group, criterion_main, Criterion};
use holys3_core::{
    grams_index, grams_query, testutil::MemCorpus, Corpus, LocalBlobStore, Strategy,
};
use holys3_index::{update_index, IndexReader, SegmentedReader};
use holys3_query::plan;
use std::hint::black_box;

const SAMPLE: &[u8] = include_bytes!("fixtures/sample.txt");

fn mem_corpus() -> MemCorpus {
    let mut keys = Vec::new();
    let mut bodies = Vec::new();
    for id in 0..64_u32 {
        keys.push(format!("object-{id:06}.log"));
        let mut body = SAMPLE.to_vec();
        body.extend_from_slice(
            format!("\nobject_id={id} ERROR42 timeout handleClick\n").as_bytes(),
        );
        bodies.push(body);
    }
    MemCorpus::new(keys, bodies)
}

fn bench_grams(c: &mut Criterion) {
    c.bench_function("grams_index_trigram", |b| {
        b.iter(|| grams_index(black_box(SAMPLE), Strategy::Trigram));
    });
    c.bench_function("grams_index_sparse", |b| {
        b.iter(|| grams_index(black_box(SAMPLE), Strategy::Sparse));
    });
    c.bench_function("grams_query_trigram", |b| {
        b.iter(|| grams_query(black_box(b"customer_id=abc123"), Strategy::Trigram));
    });
    c.bench_function("grams_query_sparse", |b| {
        b.iter(|| grams_query(black_box(b"customer_id=abc123"), Strategy::Sparse));
    });
}

fn bench_plan(c: &mut Criterion) {
    for pattern in [
        "ERROR42",
        "customer_id=abc123 request_id=deadbeef",
        "(timeout|panicked|denied)",
        "^CRITICAL:",
        ".*",
    ] {
        c.bench_function(&format!("plan/{pattern}"), |b| {
            b.iter(|| plan(black_box(pattern), Strategy::Sparse).expect("benchmark setup failed"));
        });
    }
}

fn bench_index_reader(c: &mut Criterion) {
    let corpus = mem_corpus();
    let store_dir = tempfile::tempdir().expect("benchmark setup failed");
    let store = LocalBlobStore::new(store_dir.path());
    let cache_dir = tempfile::tempdir().expect("benchmark setup failed");
    let listing: Vec<(String, String, u64)> = corpus
        .docs()
        .iter()
        .map(|doc| (doc.key.clone(), format!("etag-{}", doc.key), doc.size))
        .collect();
    update_index(
        &store,
        cache_dir.path(),
        Strategy::Sparse,
        &listing,
        false,
        &|shard| {
            let keys: Vec<String> = shard.iter().map(|(key, _, _)| key.clone()).collect();
            let bodies = keys
                .iter()
                .map(|key| {
                    let idx = corpus
                        .docs()
                        .iter()
                        .position(|doc| doc.key == *key)
                        .expect("listed key exists");
                    corpus.fetch(idx)
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            Ok(Box::new(MemCorpus::new(keys, bodies)))
        },
    )
    .expect("benchmark setup failed");
    let store_reader = SegmentedReader::open(
        Box::new(LocalBlobStore::new(store_dir.path())),
        cache_dir.path(),
    )
    .expect("benchmark setup failed");
    let q = plan("ERROR42", store_reader.strategy()).expect("benchmark setup failed");
    c.bench_function("local_blob_store_index_reader_candidates", |b| {
        b.iter(|| {
            store_reader
                .candidate_keys(black_box(&q), None)
                .expect("benchmark setup failed");
        });
    });
}

criterion_group!(benches, bench_grams, bench_plan, bench_index_reader);
criterion_main!(benches);
