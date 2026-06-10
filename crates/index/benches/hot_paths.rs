use criterion::{criterion_group, criterion_main, Criterion};
use holys3_core::{
    grams_index, grams_query, testutil::MemCorpus, Corpus, LocalBlobStore, Strategy,
};
use holys3_index::{build_to_dir, update_index, IndexReader, MmapIndexReader, SegmentedReader};
use holys3_query::plan;
use std::hint::black_box;

const SAMPLE: &[u8] = include_bytes!("fixtures/sample.txt");

fn mem_corpus() -> MemCorpus {
    let mut docs = Vec::new();
    let mut bodies = Vec::new();
    for id in 0..64_u32 {
        docs.push((id, format!("object-{id:06}.log")));
        let mut body = SAMPLE.to_vec();
        body.extend_from_slice(
            format!("\nobject_id={id} ERROR42 timeout handleClick\n").as_bytes(),
        );
        bodies.push(body);
    }
    MemCorpus::new(docs, bodies)
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
    let dir = tempfile::tempdir().expect("benchmark setup failed");
    build_to_dir(&corpus, dir.path(), Strategy::Sparse).expect("benchmark setup failed");
    let mmap_reader = MmapIndexReader::open(dir.path()).expect("benchmark setup failed");
    let q = plan("ERROR42", mmap_reader.strategy()).expect("benchmark setup failed");
    c.bench_function("mmap_index_reader_candidates", |b| {
        b.iter(|| {
            mmap_reader
                .candidate_keys(black_box(&q), None)
                .expect("benchmark setup failed");
        });
    });

    let store_dir = tempfile::tempdir().expect("benchmark setup failed");
    let store = LocalBlobStore::new(store_dir.path());
    let cache_dir = tempfile::tempdir().expect("benchmark setup failed");
    let listing: Vec<(String, String)> = corpus
        .docs()
        .iter()
        .map(|(_, key)| (key.clone(), format!("etag-{key}")))
        .collect();
    update_index(
        &store,
        cache_dir.path(),
        Strategy::Sparse,
        &listing,
        false,
        &|keys| {
            let docs = keys
                .iter()
                .enumerate()
                .map(|(i, key)| (i as u32, key.clone()))
                .collect();
            let bodies = keys
                .iter()
                .map(|key| {
                    let (id, _) = corpus
                        .docs()
                        .iter()
                        .find(|(_, k)| k == key)
                        .expect("listed key exists");
                    corpus.fetch(*id)
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            Ok(Box::new(MemCorpus::new(docs, bodies)))
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
