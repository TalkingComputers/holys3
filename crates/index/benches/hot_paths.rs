use anyhow::Result;
use criterion::{criterion_group, criterion_main, Criterion};
use holys3_core::{grams_index, grams_query, Corpus, DocId, LocalBlobStore, Strategy};
use holys3_index::{
    build_to_dir, build_to_store, decode_postings_block, eval_query, IndexReader, MmapIndexReader,
    StoreIndexReader,
};
use holys3_query::{plan, Query};
use std::collections::{BTreeMap, BTreeSet};
use std::hint::black_box;

const SAMPLE: &[u8] = include_bytes!("fixtures/sample.txt");

struct MemCorpus {
    docs: Vec<(DocId, String)>,
    bodies: Vec<Vec<u8>>,
}

impl Corpus for MemCorpus {
    fn docs(&self) -> &[(DocId, String)] {
        &self.docs
    }

    fn fetch(&self, id: DocId) -> Result<Vec<u8>> {
        Ok(self.bodies[id as usize].clone())
    }
}

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
    MemCorpus { docs, bodies }
}

fn postings_fixture() -> (BTreeMap<Vec<u8>, u64>, Vec<u8>, BTreeSet<DocId>) {
    let mut postings = BTreeMap::new();
    let mut bytes = Vec::new();
    let mut all = BTreeSet::new();
    for id in 0..128_u32 {
        all.insert(id);
    }
    for (gram, docs) in [
        (b"ERR".to_vec(), vec![1_u32, 3, 5, 8, 13, 21, 34, 55]),
        (b"tim".to_vec(), vec![2_u32, 3, 5, 7, 11, 13, 17, 19]),
        (b"han".to_vec(), vec![1_u32, 2, 3, 5, 8, 13, 21]),
    ] {
        let offset = bytes.len() as u64;
        bytes.extend_from_slice(&(docs.len() as u32).to_le_bytes());
        for id in docs {
            bytes.extend_from_slice(&id.to_le_bytes());
        }
        postings.insert(gram, offset);
    }
    (postings, bytes, all)
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

fn bench_eval_query(c: &mut Criterion) {
    let (postings, bytes, all) = postings_fixture();
    let q = Query::And(vec![
        Query::Gram(b"ERR".to_vec()),
        Query::Gram(b"han".to_vec()),
    ]);
    c.bench_function("eval_query_in_memory_postings", |b| {
        b.iter(|| {
            eval_query(
                black_box(&q),
                black_box(&all),
                &|gram| postings.get(gram).copied(),
                &|offset| decode_postings_block(&bytes, offset),
            )
            .expect("benchmark setup failed");
        });
    });
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
                .candidates(black_box(&q))
                .expect("benchmark setup failed");
        });
    });

    let store_dir = tempfile::tempdir().expect("benchmark setup failed");
    let store = LocalBlobStore::new(store_dir.path());
    build_to_store(&corpus, &store, Strategy::Sparse, "bench-build")
        .expect("benchmark setup failed");
    let cache_dir = tempfile::tempdir().expect("benchmark setup failed");
    let store_reader = StoreIndexReader::open(
        Box::new(LocalBlobStore::new(store_dir.path())),
        cache_dir.path(),
    )
    .expect("benchmark setup failed");
    let q = plan("ERROR42", store_reader.strategy()).expect("benchmark setup failed");
    c.bench_function("local_blob_store_index_reader_candidates", |b| {
        b.iter(|| {
            store_reader
                .candidates(black_box(&q))
                .expect("benchmark setup failed");
        });
    });
}

fn bench_postings_decode(c: &mut Criterion) {
    let (_postings, bytes, _all) = postings_fixture();
    c.bench_function("postings_block_decode", |b| {
        b.iter(|| {
            decode_postings_block(black_box(&bytes), black_box(0)).expect("benchmark setup failed");
        });
    });
}

criterion_group!(
    benches,
    bench_grams,
    bench_plan,
    bench_eval_query,
    bench_index_reader,
    bench_postings_decode
);
criterion_main!(benches);
