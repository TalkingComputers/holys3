use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use holys3_core::{
    grams_index, grams_query, pack_trigram_grams, testutil::MemCorpus, Corpus, LocalBlobStore,
    Strategy,
};
use holys3_index::{update_index, IndexReader, SegmentedReader, SourceIdentity, UpdateOptions};
use holys3_query::plan;
use std::collections::HashMap;
use std::hint::black_box;
use std::time::Duration;

const SAMPLE: &[u8] = include_bytes!("fixtures/sample.txt");

fn source_identity() -> SourceIdentity {
    SourceIdentity::Local {
        prefix: "/benchmark/".into(),
    }
}

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

fn generated_corpus(objects: usize, size: usize) -> MemCorpus {
    let mut keys = Vec::with_capacity(objects);
    let mut bodies = Vec::with_capacity(objects);
    for id in 0..objects {
        let record = format!(
            "2026-07-10T12:34:56.789Z level={} service=checkout request_id={id:016x} customer_id={} message=\"{}\"\n",
            if id % 97 == 0 { "ERROR" } else { "INFO" },
            id % 10_000,
            if id % 97 == 0 {
                "payment timeout while contacting upstream"
            } else {
                "request completed successfully"
            }
        );
        let mut body = Vec::with_capacity(size);
        while body.len() < size {
            body.extend_from_slice(record.as_bytes());
        }
        body.truncate(size);
        keys.push(format!("logs/2026/07/10/object-{id:06}.log"));
        bodies.push(body);
    }
    MemCorpus::new(keys, bodies)
}

fn source_positions(corpus: &dyn Corpus) -> HashMap<String, usize> {
    corpus
        .sources()
        .iter()
        .enumerate()
        .map(|(index, source)| (source.key.clone(), index))
        .collect()
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

fn bench_packed_sort_crossover(c: &mut Criterion) {
    for windows in [128, 256, 512, 1024, 4096, 65_536] {
        let mut state = 0x9e37_79b9_u32;
        let input = (0..windows + 2)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 17;
                state ^= state << 5;
                state as u8
            })
            .collect::<Vec<_>>();
        c.bench_function(&format!("packed_sort_control/{windows}"), |b| {
            b.iter(|| {
                let mut grams = black_box(input.as_slice())
                    .windows(3)
                    .map(|window| {
                        u32::from(window[0]) << 16
                            | u32::from(window[1]) << 8
                            | u32::from(window[2])
                    })
                    .collect::<Vec<_>>();
                grams.sort_unstable();
                grams.dedup();
                black_box(grams)
            });
        });
        c.bench_function(&format!("packed_sort_hybrid/{windows}"), |b| {
            b.iter(|| black_box(pack_trigram_grams(black_box(&input))));
        });
    }
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
        .sources()
        .iter()
        .map(|source| {
            (
                source.key.clone(),
                source.version.clone(),
                source.encoded_size,
            )
        })
        .collect();
    let positions = source_positions(&corpus);
    update_index(
        &store,
        cache_dir.path(),
        &source_identity(),
        Some(Strategy::Sparse),
        &listing,
        UpdateOptions::default(),
        &|shard| {
            let keys: Vec<String> = shard.iter().map(|(key, _, _)| key.clone()).collect();
            let bodies = keys
                .iter()
                .map(|key| {
                    let idx = positions.get(key).copied().expect("listed key exists");
                    Ok(corpus.fetch(idx)?.to_vec())
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            Ok(Box::new(MemCorpus::new(keys, bodies)))
        },
    )
    .expect("benchmark setup failed");
    let store_reader = SegmentedReader::open(
        Box::new(LocalBlobStore::new(store_dir.path())),
        cache_dir.path(),
        &source_identity(),
    )
    .expect("benchmark setup failed");
    let q = plan("ERROR42", store_reader.strategy()).expect("benchmark setup failed");
    c.bench_function("local_blob_store_segmented_reader_candidate_docs", |b| {
        b.iter(|| {
            store_reader
                .candidate_docs(black_box(&q), None)
                .expect("benchmark setup failed");
        });
    });
}

fn bench_index_build(c: &mut Criterion) {
    for (name, strategy) in [
        ("index_build_1024x4096_trigram", Strategy::Trigram),
        ("index_build_1024x4096_sparse", Strategy::Sparse),
    ] {
        c.bench_function(name, |b| {
            b.iter_batched(
                || {
                    let corpus = generated_corpus(1024, 4096);
                    let listing = corpus
                        .sources()
                        .iter()
                        .map(|source| {
                            (
                                source.key.clone(),
                                source.version.clone(),
                                source.encoded_size,
                            )
                        })
                        .collect::<Vec<_>>();
                    let positions = source_positions(&corpus);
                    (
                        corpus,
                        listing,
                        positions,
                        tempfile::tempdir().expect("benchmark setup failed"),
                        tempfile::tempdir().expect("benchmark setup failed"),
                    )
                },
                |(corpus, listing, positions, store_dir, cache_dir)| {
                    update_index(
                        &LocalBlobStore::new(store_dir.path()),
                        cache_dir.path(),
                        &source_identity(),
                        Some(strategy),
                        &listing,
                        UpdateOptions::default(),
                        &|shard| {
                            let keys = shard
                                .iter()
                                .map(|(key, _, _)| key.clone())
                                .collect::<Vec<_>>();
                            let bodies = keys
                                .iter()
                                .map(|key| {
                                    let idx = positions
                                        .get(key)
                                        .copied()
                                        .expect("benchmark setup failed");
                                    Ok(corpus.fetch(idx)?.to_vec())
                                })
                                .collect::<anyhow::Result<Vec<_>>>()?;
                            Ok(Box::new(MemCorpus::new(keys, bodies)))
                        },
                    )
                    .expect("benchmark setup failed");
                },
                BatchSize::LargeInput,
            );
        });
    }
}

criterion_group!(benches, bench_grams, bench_plan, bench_index_reader);
criterion_group! {
    name = sort_crossover;
    config = Criterion::default()
        .sample_size(20)
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(1));
    targets = bench_packed_sort_crossover
}
criterion_group! {
    name = index_build;
    config = Criterion::default().sample_size(10);
    targets = bench_index_build
}
criterion_main!(benches, sort_crossover, index_build);
