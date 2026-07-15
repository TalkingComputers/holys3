use crate::gen::{
    churn_run_path, doc_path, local_index_dir, objects_dir, read_manifest, reports_dir,
};
use crate::{dir_cache_dir, dir_source_identity, percentile_ms};
use anyhow::{Context, Result};
use holys3_core::{BlobStore, LocalBlobStore, MatchOptions, Strategy};
use holys3_index::{
    search_streaming, update_index, IndexReader, KeyScope, LocalCorpus, NullSink, SegmentedReader,
    UpdateOptions,
};
use serde::{Deserialize, Serialize};
use std::cell::Cell;
use std::collections::{BTreeSet, VecDeque};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Instant;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ChurnSummary {
    pub cycles: usize,
    pub changes_per_cycle: usize,
    pub total_docs: usize,
    pub listing_p50_ms: f64,
    pub listing_p95_ms: f64,
    pub update_p50_ms: f64,
    pub update_p95_ms: f64,
    pub update_max_ms: f64,
    pub pack_bytes_written: u64,
    pub final_segments: usize,
}

struct ChurnStore {
    inner: LocalBlobStore,
    pack_bytes_written: Cell<u64>,
}

impl ChurnStore {
    fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            inner: LocalBlobStore::new(root),
            pack_bytes_written: Cell::new(0),
        }
    }

    fn record_pack_write(&self, name: &str, len: u64) -> Result<()> {
        if name.starts_with("packs/") {
            self.pack_bytes_written.set(
                self.pack_bytes_written
                    .get()
                    .checked_add(len)
                    .context("churn pack write count overflows")?,
            );
        }
        Ok(())
    }

    fn read_pack_bytes_written(&self) -> u64 {
        self.pack_bytes_written.get()
    }
}

impl BlobStore for ChurnStore {
    fn put(&self, name: &str, bytes: &[u8]) -> Result<()> {
        self.inner.put(name, bytes)?;
        self.record_pack_write(name, u64::try_from(bytes.len())?)
    }

    fn put_file(&self, name: &str, path: &Path) -> Result<()> {
        self.inner.put_file(name, path)?;
        self.record_pack_write(name, std::fs::metadata(path)?.len())
    }

    fn get(&self, name: &str) -> Result<Option<Vec<u8>>> {
        self.inner.get(name)
    }

    fn get_range(&self, name: &str, start: u64, len: u64) -> Result<Vec<u8>> {
        self.inner.get_range(name, start, len)
    }

    fn delete(&self, name: &str) -> Result<()> {
        self.inner.delete(name)
    }

    fn get_versioned(&self, name: &str) -> Result<Option<(Vec<u8>, String)>> {
        self.inner.get_versioned(name)
    }

    fn put_if(&self, name: &str, bytes: &[u8], expected: Option<&str>) -> Result<bool> {
        self.inner.put_if(name, bytes, expected)
    }

    fn put_streaming<'a>(&'a self, name: &str) -> Result<Box<dyn holys3_core::StreamingPut + 'a>> {
        self.inner.put_streaming(name)
    }
}

#[derive(Serialize)]
struct ChurnRecord {
    timestamp: String,
    level: &'static str,
    service: String,
    request_id: String,
    message: String,
}

pub(crate) fn run(cycles: usize, changes: usize) -> Result<ChurnSummary> {
    anyhow::ensure!(cycles > 0, "cycles must be greater than 0");
    anyhow::ensure!(changes > 0, "changes must be greater than 0");
    let manifest = read_manifest().context("reading seed manifest")?;
    anyhow::ensure!(
        manifest.docs.len() == manifest.objects,
        "manifest declares {} objects but contains {} documents",
        manifest.objects,
        manifest.docs.len()
    );
    anyhow::ensure!(
        changes <= manifest.objects,
        "changes must not exceed {} objects",
        manifest.objects
    );
    let total_changes = cycles
        .checked_mul(changes)
        .context("churn change count overflow")?;
    let mut live_paths = manifest.docs.iter().map(doc_path).collect::<VecDeque<_>>();
    for path in &live_paths {
        let metadata = std::fs::metadata(path)
            .with_context(|| format!("reading source metadata for {}", path.display()))?;
        anyhow::ensure!(
            metadata.is_file(),
            "source is not a file: {}",
            path.display()
        );
    }
    let initial_corpus = LocalCorpus::new(&objects_dir()).context("listing initial corpus")?;
    let initial_listing = initial_corpus
        .listing()
        .context("reading initial listing")?;
    anyhow::ensure!(
        initial_listing.len() == manifest.objects,
        "manifest has {} objects but source contains {}",
        manifest.objects,
        initial_listing.len()
    );
    let initial_reader = SegmentedReader::open(
        Box::new(LocalBlobStore::new(local_index_dir())),
        &dir_cache_dir(),
        &dir_source_identity()?,
    )
    .context("opening local benchmark index")?;
    anyhow::ensure!(
        initial_reader.total_docs() == manifest.objects,
        "manifest has {} objects but index contains {}",
        manifest.objects,
        initial_reader.total_docs()
    );
    drop(initial_reader);

    let mut listing_times = Vec::with_capacity(cycles);
    let mut update_times = Vec::with_capacity(cycles);
    let mut churn_paths = BTreeSet::new();
    let mut sequence = manifest.objects;
    let mut final_segments = None;
    let store = ChurnStore::new(local_index_dir());
    for cycle in 1..=cycles {
        for _ in 0..changes {
            let old_path = live_paths
                .pop_front()
                .context("live source queue is empty")?;
            std::fs::remove_file(&old_path)
                .with_context(|| format!("deleting churn source {}", old_path.display()))?;
            churn_paths.remove(&old_path);
            let new_path = build_churn_path(sequence);
            let body = build_churn_body(manifest.seed, sequence, manifest.size)?;
            write_churn_source(&new_path, &body)?;
            churn_paths.insert(new_path.clone());
            live_paths.push_back(new_path);
            sequence = sequence.checked_add(1).context("churn sequence overflow")?;
        }

        let listing_started = Instant::now();
        let corpus = LocalCorpus::new(&objects_dir()).context("listing churn corpus")?;
        let listing = corpus.listing().context("reading churn listing")?;
        listing_times.push(listing_started.elapsed());
        anyhow::ensure!(
            listing.len() == manifest.objects,
            "cycle {cycle} listed {} objects, expected {}",
            listing.len(),
            manifest.objects
        );

        let update_started = Instant::now();
        let report = update_index(
            &store,
            &dir_cache_dir(),
            &dir_source_identity()?,
            Some(Strategy::Trigram),
            &listing,
            UpdateOptions::default(),
            &|shard| Ok(Box::new(LocalCorpus::from_listing(shard))),
        )
        .with_context(|| format!("updating churn cycle {cycle}"))?;
        update_times.push(update_started.elapsed());
        anyhow::ensure!(
            report.added == changes,
            "cycle {cycle} added {}, expected {changes}",
            report.added
        );
        anyhow::ensure!(
            report.removed == changes,
            "cycle {cycle} removed {}, expected {changes}",
            report.removed
        );
        anyhow::ensure!(
            report.total_docs == manifest.objects,
            "cycle {cycle} indexed {} documents, expected {}",
            report.total_docs,
            manifest.objects
        );
        final_segments = Some(report.segments);
    }

    let reader = SegmentedReader::open(
        Box::new(LocalBlobStore::new(local_index_dir())),
        &dir_cache_dir(),
        &dir_source_identity()?,
    )?;
    let stats = search_streaming(
        &reader,
        "CHURN_NEEDLE",
        KeyScope::default(),
        MatchOptions::default(),
        &NullSink,
    )?;
    let expected_count = total_changes.min(manifest.objects);
    anyhow::ensure!(
        churn_paths.len() == expected_count,
        "tracked {} churn sources, expected {expected_count}",
        churn_paths.len()
    );
    let mut expected_hits = churn_paths
        .iter()
        .map(|path| {
            Ok(path
                .to_str()
                .with_context(|| format!("churn source is not UTF-8: {}", path.display()))?
                .replace('\\', "/"))
        })
        .collect::<Result<Vec<_>>>()?;
    let mut actual_hits = stats.hits;
    expected_hits.sort_unstable();
    actual_hits.sort_unstable();
    anyhow::ensure!(
        actual_hits == expected_hits,
        "CHURN_NEEDLE indexed hit keys differ from live churn sources"
    );

    listing_times.sort_unstable();
    update_times.sort_unstable();
    let summary = ChurnSummary {
        cycles,
        changes_per_cycle: changes,
        total_docs: manifest.objects,
        listing_p50_ms: percentile_ms(&listing_times, 50),
        listing_p95_ms: percentile_ms(&listing_times, 95),
        update_p50_ms: percentile_ms(&update_times, 50),
        update_p95_ms: percentile_ms(&update_times, 95),
        update_max_ms: percentile_ms(&update_times, 100),
        pack_bytes_written: store.read_pack_bytes_written(),
        final_segments: final_segments.context("missing final segment count")?,
    };
    std::fs::create_dir_all(reports_dir()).context("creating benchmark reports directory")?;
    let mut output = std::io::BufWriter::new(
        std::fs::File::create(churn_run_path()).context("creating churn summary")?,
    );
    serde_json::to_writer_pretty(&mut output, &summary).context("writing churn summary")?;
    output
        .write_all(b"\n")
        .context("writing churn summary newline")?;
    output.flush().context("flushing churn summary")?;
    Ok(summary)
}

fn build_churn_path(sequence: usize) -> PathBuf {
    objects_dir()
        .join("year=2026/month=07/day=12")
        .join(format!("hour={:02}", sequence % 24))
        .join(format!("churn-{sequence:08}.jsonl"))
}

fn build_churn_body(seed: u64, sequence: usize, size: usize) -> Result<Vec<u8>> {
    let mut record = ChurnRecord {
        timestamp: format!("2026-07-12T{:02}:00:00Z", sequence % 24),
        level: "INFO",
        service: format!("holys3-bench-{:04x}", seed & 0xffff),
        request_id: format!("{seed:016x}-{sequence:016x}"),
        message: format!("CHURN_NEEDLE sequence={sequence}"),
    };
    let encoded = serde_json::to_vec(&record).context("encoding churn record")?;
    let encoded_len = encoded
        .len()
        .checked_add(1)
        .context("churn record length overflow")?;
    let padding = size.checked_sub(encoded_len).with_context(|| {
        format!("object size {size} is shorter than churn record size {encoded_len}")
    })?;
    record
        .message
        .try_reserve(padding)
        .context("reserving churn message padding")?;
    record.message.extend(std::iter::repeat_n(' ', padding));
    let mut body = serde_json::to_vec(&record).context("encoding padded churn record")?;
    body.try_reserve(1)
        .context("reserving churn record newline")?;
    body.push(b'\n');
    anyhow::ensure!(body.len() == size, "churn record size mismatch");
    Ok(body)
}

fn write_churn_source(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path.parent().context("churn source path has no parent")?;
    std::fs::create_dir_all(parent)
        .with_context(|| format!("creating churn directory {}", parent.display()))?;
    std::fs::write(path, bytes).with_context(|| format!("writing churn source {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use std::path::Path;

    #[test]
    fn churn_body_is_deterministic_and_exact() -> Result<()> {
        let first = build_churn_body(7, 42, 4096)?;
        let second = build_churn_body(7, 42, 4096)?;
        assert_eq!(first, second);
        assert_eq!(first.len(), 4096);
        assert!(std::str::from_utf8(&first)?.starts_with('{'));
        assert!(first.ends_with(b"}\n"));
        assert!(first
            .windows(b"CHURN_NEEDLE".len())
            .any(|bytes| bytes == b"CHURN_NEEDLE"));
        Ok(())
    }

    #[test]
    fn churn_paths_are_date_partitioned() {
        assert!(build_churn_path(42).ends_with(Path::new(
            "year=2026/month=07/day=12/hour=18/churn-00000042.jsonl"
        )));
    }

    #[test]
    fn churn_store_counts_only_pack_writes() -> Result<()> {
        let root = tempfile::tempdir()?;
        let first = root.path().join("first");
        let second = root.path().join("second");
        std::fs::write(&first, b"abc")?;
        std::fs::write(&second, b"other")?;
        let store = ChurnStore::new(root.path().join("store"));

        BlobStore::put_file(&store, "packs/a.pack", &first)?;
        BlobStore::put(&store, "packs/b.pack", b"de")?;
        BlobStore::put_file(&store, "segments/a/docs.bin", &second)?;

        assert_eq!(store.read_pack_bytes_written(), 5);
        Ok(())
    }
}
