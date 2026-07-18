//! Segmented incremental index over a `BlobStore`.
//!
//! Layout under the store root:
//!
//! ```text
//! segments.bin                  root pointer (SegmentList), rewritten per index run
//! segments/<id>/terms.fst
//! segments/<id>/postings.bin
//! segments/<id>/docs.bin
//! segments/<id>/dead-<hash>.bin immutable dead-id sets, referenced by hash
//! packs/<hash>.pack             immutable canonical decoded content frames
//! ```
//!
//! `seagrep index` becomes a diff: list the bucket, compare (key, etag)
//! against the union of segment doc tables, build bounded segments over the
//! changes, tombstone superseded documents, periodically repack, and atomically
//! swap segments.bin.

#[cfg(test)]
use crate::format::DocEntry;
use crate::format::{parse_dead, parse_tables, DeadSet, SegmentTables, SourceEntry};
use crate::pack::{PackFile, PackMeta};
use crate::terms::TermMap;
use crate::{candidates_with, INDEX_FORMAT};
use anyhow::{Context, Result};
use cache::{cached_blob, cached_bytes, cached_file, map_file};
use compact::{maybe_compact, merge_segments};
use seagrep_core::{BlobStore, Corpus, DocAddress, IndexAddress, ProgressSender, Strategy};
use seagrep_query::Query;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

pub(crate) mod cache;
mod compact;

/// Per-segment doc cap: keeps every per-gram posting list far below the
/// 2^24 `pack_posting` ceiling, and bounds build memory.
const SEGMENT_DOC_CAP: usize = 4_000_000;
/// Compact (merge two adjacent segments) when more live segments than this.
const SEGMENT_COUNT_TARGET: usize = 8;
/// Never merge segments whose combined postings exceed this many bytes.
const MERGE_POSTINGS_CAP: u64 = 256 * 1024 * 1024;
const MERGE_TERMS_CAP: u64 = 64 * 1024 * 1024;
const MERGE_DOCS_CAP: u64 = 64 * 1024 * 1024;
const REPACK_DEAD_FRACTION: usize = 4;

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct SegmentMeta {
    pub seg_id: String,
    pub doc_count: u32,
    pub terms_fst_len: u64,
    pub terms_fst_hash: String,
    /// SHA-256 of the sparse table's index+footer tail; empty for trigram.
    /// Lets remote readers trust a ranged fetch of just the block index.
    pub terms_tail_hash: String,
    pub postings_len: u64,
    pub postings_hash: String,
    /// Length of postings.bin's data region; the remainder is the per-block
    /// verification table + footer.
    pub postings_data_len: u64,
    /// SHA-256 of the verification table + footer tail — lets readers trust
    /// a ranged fetch of just the table.
    pub postings_tail_hash: String,
    pub docs_len: u64,
    pub docs_hash: String,
    pub min_key: String,
    pub max_key: String,
    pub dead_hash: String,
    pub dead_len: u64,
    /// Sources excluded at build time (undecodable objects). Queries surface
    /// this so completeness gaps are visible without the build log.
    pub failed_source_count: u32,
    pub packs: Vec<PackMeta>,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct SegmentList {
    pub format: u32,
    pub source: SourceIdentity,
    pub strategy: Strategy,
    pub segments: Vec<SegmentMeta>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceIdentity {
    Local {
        prefix: String,
    },
    S3 {
        endpoint: String,
        bucket: String,
        prefix: String,
    },
}

impl SourceIdentity {
    fn validate(&self) -> Result<()> {
        match self {
            Self::Local { prefix } => anyhow::ensure!(
                !prefix.is_empty() && prefix.ends_with('/'),
                "local source identity must be a non-empty directory prefix"
            ),
            Self::S3 {
                endpoint,
                bucket,
                prefix,
            } => {
                anyhow::ensure!(!endpoint.is_empty(), "S3 source endpoint is empty");
                anyhow::ensure!(!bucket.is_empty(), "S3 source bucket is empty");
                anyhow::ensure!(
                    prefix.is_empty() || prefix.ends_with('/'),
                    "S3 source prefix must be empty or end with /"
                );
            }
        }
        Ok(())
    }

    fn can_search(&self, requested: &Self) -> bool {
        match (self, requested) {
            (Self::Local { prefix }, Self::Local { prefix: requested }) => {
                requested.starts_with(prefix)
            }
            (
                Self::S3 {
                    endpoint,
                    bucket,
                    prefix,
                },
                Self::S3 {
                    endpoint: requested_endpoint,
                    bucket: requested_bucket,
                    prefix: requested_prefix,
                },
            ) => {
                endpoint == requested_endpoint
                    && bucket == requested_bucket
                    && requested_prefix.starts_with(prefix)
            }
            _ => false,
        }
    }
}

impl std::fmt::Display for SourceIdentity {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Local { prefix } => write!(formatter, "local directory {prefix}"),
            Self::S3 {
                endpoint,
                bucket,
                prefix,
            } => write!(formatter, "s3://{bucket}/{prefix} at {endpoint}"),
        }
    }
}

fn sha256_hex(parts: &[&[u8]]) -> String {
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update(part);
    }
    hex_encode(&hasher.finalize())
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn segment_blob(seg_id: &str, name: &str) -> String {
    format!("segments/{seg_id}/{name}")
}

fn pack_blob(hash: &str) -> String {
    format!("packs/{hash}.pack")
}

fn parse_segment_list(bytes: &[u8]) -> Result<SegmentList> {
    let list: SegmentList = postcard::from_bytes(bytes).context("segments.bin unreadable")?;
    anyhow::ensure!(
        list.format == INDEX_FORMAT,
        "index format {} is not the current {INDEX_FORMAT}",
        list.format
    );
    list.source.validate()?;
    let mut segment_ids = std::collections::HashSet::with_capacity(list.segments.len());
    for segment in &list.segments {
        anyhow::ensure!(
            is_sha256(&segment.seg_id),
            "segment ID is not a SHA-256 hash"
        );
        anyhow::ensure!(
            is_sha256(&segment.terms_fst_hash)
                && is_sha256(&segment.postings_hash)
                && is_sha256(&segment.postings_tail_hash)
                && is_sha256(&segment.docs_hash),
            "segment blob hash is not a SHA-256 hash"
        );
        anyhow::ensure!(
            segment.postings_data_len <= segment.postings_len,
            "postings data region exceeds its blob"
        );
        anyhow::ensure!(
            segment.terms_tail_hash.is_empty() || is_sha256(&segment.terms_tail_hash),
            "segment term tail hash is invalid"
        );
        anyhow::ensure!(
            segment_ids.insert(segment.seg_id.as_str()),
            "segment ID is duplicated"
        );
        anyhow::ensure!(
            segment.min_key <= segment.max_key,
            "segment key bounds are reversed"
        );
        anyhow::ensure!(
            (segment.dead_hash.is_empty() && segment.dead_len == 0)
                || (is_sha256(&segment.dead_hash) && segment.dead_len > 0),
            "segment dead-set metadata is invalid"
        );
        anyhow::ensure!(
            segment
                .packs
                .iter()
                .all(|pack| is_sha256(&pack.hash) && pack.len > 0),
            "segment pack metadata is invalid"
        );
    }
    Ok(list)
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn validate_segment_tables(meta: &SegmentMeta, tables: &SegmentTables) -> Result<()> {
    anyhow::ensure!(
        tables.documents.len() == meta.doc_count as usize,
        "segment document count does not match its metadata"
    );
    let first = tables.sources.first().context("segment has no sources")?;
    let last = tables.sources.last().context("segment has no sources")?;
    anyhow::ensure!(
        first.key == meta.min_key && last.key == meta.max_key,
        "segment key bounds do not match its source table"
    );
    let pack_count = tables
        .blocks
        .last()
        .map_or(0usize, |block| block.pack as usize + 1);
    anyhow::ensure!(
        pack_count == meta.packs.len(),
        "segment pack count does not match its metadata"
    );
    for (pack_id, pack) in meta.packs.iter().enumerate() {
        let end = tables
            .blocks
            .iter()
            .rev()
            .find(|block| block.pack as usize == pack_id)
            .map(|block| block.offset + u64::from(block.compressed_len))
            .context("segment pack has no blocks")?;
        anyhow::ensure!(
            end == pack.len,
            "segment pack length does not match its blocks"
        );
    }
    Ok(())
}

enum RootState {
    Loaded(SegmentList),
    Absent,
    /// Present but undecodable (old format, corruption): a definitive
    /// rebuild signal, unlike a transient store failure which is `Err`.
    Unreadable(String),
}

/// A failing store is an error so a transient outage can never silently
/// trigger a full rebuild; absence and unreadability are first-class states.
/// Loads the root plus its version token, the CAS expectation for the swap
/// at the end of an index run.
fn load_segment_list(store: &dyn BlobStore) -> Result<(RootState, Option<String>)> {
    match store
        .get_versioned("segments.bin")
        .context("reading segments.bin")?
    {
        None => Ok((RootState::Absent, None)),
        Some((bytes, version)) => match parse_segment_list(&bytes) {
            Ok(list) => Ok((RootState::Loaded(list), Some(version))),
            Err(err) => Ok((RootState::Unreadable(format!("{err:#}")), Some(version))),
        },
    }
}

/// What an index run did; everything the CLI needs to report.
#[derive(Debug)]
pub struct UpdateReport {
    pub added: usize,
    pub removed: usize,
    pub total_docs: usize,
    pub segments: usize,
    pub compacted: bool,
    pub up_to_date: bool,
}

#[derive(Debug, Clone, Default)]
pub struct UpdateOptions {
    pub rebuild: bool,
    pub purge_deleted: bool,
    pub progress: Option<ProgressSender>,
}

#[derive(Debug)]
pub struct IndexChanged;

impl std::fmt::Display for IndexChanged {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("index changed during search; reopen it and retry")
    }
}

impl std::error::Error for IndexChanged {}

/// Builds a fetchable corpus over the given listing slice ((key, etag, size)
/// triples; ids = positions).
pub type CorpusFactory<'a> = dyn Fn(&[(String, String, u64)]) -> Result<Box<dyn Corpus>> + 'a;

/// Incrementally update the segmented index to match `listing`
/// ((key, etag, size) triples). `make_corpus` builds a fetchable corpus over
/// a given listing slice, with ids equal to positions in the slice.
/// `strategy: None` selects automatically: an existing index keeps its
/// recorded strategy; a fresh build (or `--rebuild`) samples decoded content
/// and picks sparse for natural-language prose, trigram otherwise.
pub fn update_index(
    store: &dyn BlobStore,
    cache_dir: &Path,
    source: &SourceIdentity,
    strategy: Option<Strategy>,
    listing: &[(String, String, u64)],
    options: UpdateOptions,
    make_corpus: &CorpusFactory<'_>,
) -> Result<UpdateReport> {
    let UpdateOptions {
        rebuild,
        purge_deleted,
        ref progress,
    } = options;
    source.validate()?;
    let mut listing_keys = std::collections::HashSet::with_capacity(listing.len());
    for (key, _, _) in listing {
        anyhow::ensure!(
            listing_keys.insert(key.as_str()),
            "duplicate listing key {key}"
        );
    }
    let mut forced = rebuild;
    let mut replaced: Vec<SegmentMeta> = Vec::new();
    if rebuild {
        eprintln!("note: --rebuild requested; re-ingesting everything");
    }
    let (root, root_version) = load_segment_list(store)?;
    // Reject a source mismatch before strategy detection: auto-selection may
    // sample the target, and an invalid index/target pairing must fail
    // without any fetching.
    if let (RootState::Loaded(list), false) = (&root, rebuild) {
        anyhow::ensure!(
            list.source == *source,
            "index was built for {}, not {source}; use --rebuild to replace it",
            list.source
        );
    }
    let strategy = match strategy {
        Some(strategy) => strategy,
        None => match (&root, rebuild) {
            // Follow the recorded strategy only once the index holds real
            // content: an empty first build (e.g. watch mode on an empty
            // bucket) must re-detect when documents finally arrive.
            (RootState::Loaded(list), false) if !list.segments.is_empty() => list.strategy,
            _ => detect_strategy(listing, make_corpus)?,
        },
    };
    let existing = if rebuild {
        if let RootState::Loaded(list) = root {
            replaced = list.segments;
        }
        Vec::new()
    } else {
        match root {
            RootState::Loaded(list) => {
                if list.strategy == strategy {
                    list.segments
                } else {
                    eprintln!("note: index strategy changed; rebuilding from scratch");
                    forced = true;
                    replaced = list.segments;
                    Vec::new()
                }
            }
            RootState::Absent => {
                eprintln!("note: no existing index; building from scratch");
                Vec::new()
            }
            RootState::Unreadable(reason) => {
                eprintln!("note: {reason}; rebuilding from scratch");
                forced = true;
                Vec::new()
            }
        }
    };
    replaced.extend(existing.iter().cloned());

    // Newest entry per key wins; dead ids are already gone from `live`.
    let mut tables: Vec<SegmentTables> = Vec::with_capacity(existing.len());
    let mut dead_sets: Vec<DeadSet> = Vec::with_capacity(existing.len());
    for meta in &existing {
        let table = parse_tables(&cached_blob(
            store,
            cache_dir,
            &meta.seg_id,
            "docs.bin",
            meta.docs_len,
            &meta.docs_hash,
        )?)?;
        anyhow::ensure!(
            table.documents.len() == meta.doc_count as usize,
            "segment document count does not match its metadata"
        );
        let dead = load_dead(store, cache_dir, meta)?;
        dead.validate(&table)?;
        tables.push(table);
        dead_sets.push(dead);
    }
    let mut live: HashMap<&str, (usize, u32, &SourceEntry)> = HashMap::new();
    for (seg_idx, (table, dead)) in tables.iter().zip(&dead_sets).enumerate() {
        for (source_id, entry) in table.sources.iter().enumerate() {
            let source_id = source_id as u32;
            if dead.sources.binary_search(&source_id).is_ok() {
                continue;
            }
            live.insert(entry.key.as_str(), (seg_idx, source_id, entry));
        }
    }

    let mut to_add: Vec<(String, String, u64)> = listing
        .iter()
        .filter(|(key, version, _)| {
            live.get(key.as_str())
                .is_none_or(|(_, _, entry)| entry.version != *version || entry.retry)
        })
        .cloned()
        .collect();
    to_add.sort_unstable();
    let listed: HashMap<&str, &str> = listing
        .iter()
        .map(|(key, version, _)| (key.as_str(), version.as_str()))
        .collect();
    let mut newly_dead: Vec<(usize, u32)> = live
        .iter()
        .filter(|(key, (_, _, entry))| match listed.get(*key) {
            Some(listed_version) => entry.version != **listed_version || entry.retry,
            None => true,
        })
        .map(|(_, &(seg_idx, local_id, _))| (seg_idx, local_id))
        .collect();
    newly_dead.sort_unstable();

    let root_missing = root_version.is_none();
    let needs_compaction = existing.len() > SEGMENT_COUNT_TARGET;
    let needs_repack =
        dead_sets
            .iter()
            .zip(&tables)
            .zip(&existing)
            .any(|((dead, tables), meta)| {
                !dead.sources.is_empty() && (purge_deleted || should_repack(meta, tables, dead))
            });
    if to_add.is_empty()
        && newly_dead.is_empty()
        && !forced
        && !needs_compaction
        && !needs_repack
        && !root_missing
    {
        return Ok(UpdateReport {
            added: 0,
            removed: 0,
            total_docs: live_doc_count(&live),
            segments: existing.len(),
            compacted: false,
            up_to_date: true,
        });
    }
    let added = to_add.len();
    let removed = newly_dead.len();
    if let Some(progress) = progress {
        progress.emit(seagrep_core::ProgressEvent::DiffComputed {
            to_add: added as u64,
            to_remove: removed as u64,
        });
    }

    let mut metas = existing;
    let mut changed_dead = vec![false; metas.len()];
    // Exclusion counts stay live: a failed source that is deleted or
    // re-decoded goes dead here, and its segment's count must drop with it.
    let mut failed_dead = vec![0u32; metas.len()];
    for group in newly_dead.chunk_by(|a, b| a.0 == b.0) {
        let seg_idx = group[0].0;
        let mut dead = dead_sets[seg_idx].clone();
        for &(_, source_id) in group {
            dead.sources.push(source_id);
            let source = &tables[seg_idx].sources[source_id as usize];
            if source.failed {
                failed_dead[seg_idx] += 1;
            }
            dead.documents
                .extend(source.first_doc..source.first_doc + source.doc_count);
        }
        dead.sources.sort_unstable();
        dead.sources.dedup();
        dead.documents.sort_unstable();
        dead.documents.dedup();
        dead_sets[seg_idx] = dead;
        changed_dead[seg_idx] = true;
    }
    let mut keep = Vec::with_capacity(metas.len());
    let mut repacked = false;
    for (seg_idx, (mut meta, dead)) in metas.drain(..).zip(dead_sets).enumerate() {
        meta.failed_source_count = meta
            .failed_source_count
            .saturating_sub(failed_dead[seg_idx]);
        if dead.sources.len() == tables[seg_idx].sources.len() {
            continue;
        }
        if dead.sources.is_empty() {
            keep.push((meta, dead));
        } else if purge_deleted || should_repack(&meta, &tables[seg_idx], &dead) {
            let rewritten = merge_segments(store, cache_dir, strategy, &[(meta, dead)])?;
            replaced.push(rewritten.clone());
            keep.push((rewritten, DeadSet::default()));
            repacked = true;
        } else {
            dead.validate(&tables[seg_idx])?;
            if changed_dead[seg_idx] {
                let (hash, len) = write_dead(store, &meta.seg_id, &dead)?;
                meta.dead_hash = hash;
                meta.dead_len = len;
                replaced.push(meta.clone());
            }
            keep.push((meta, dead));
        }
    }

    // Build the new segment(s) over the changes, capped.
    for shard in to_add.chunks(SEGMENT_DOC_CAP) {
        for meta in write_bounded_segments(
            store,
            strategy,
            shard,
            SEGMENT_DOC_CAP,
            make_corpus,
            progress.as_ref(),
        )? {
            // newborns are GC candidates too: a segment born and compacted away
            // in the SAME run would otherwise be in neither before nor after
            replaced.push(meta.clone());
            keep.push((meta, DeadSet::default()));
        }
    }

    let compacted = maybe_compact(store, cache_dir, strategy, &mut keep)? || repacked;

    if added == 0 && removed == 0 && !forced && !root_missing && !compacted {
        return Ok(UpdateReport {
            added: 0,
            removed: 0,
            total_docs: live_doc_count(&live),
            segments: keep.len(),
            compacted: false,
            up_to_date: true,
        });
    }

    let total_docs = live_after_update(store, cache_dir, &keep)?;
    let segments: Vec<SegmentMeta> = keep.into_iter().map(|(meta, _)| meta).collect();
    let count = segments.len();
    let list = SegmentList {
        format: INDEX_FORMAT,
        source: source.clone(),
        strategy,
        segments,
    };
    // Compare-and-swap on the root: a concurrent index run that swapped
    // first wins; overwriting it would orphan its segments and then GC
    // would delete blobs its root still references.
    anyhow::ensure!(
        store.put_if(
            "segments.bin",
            &postcard::to_allocvec(&list)?,
            root_version.as_deref()
        )?,
        "another seagrep index run updated this index concurrently; rerun to pick up its result"
    );
    collect_garbage(store, &replaced, &list.segments);
    Ok(UpdateReport {
        added,
        removed,
        total_docs,
        segments: count,
        compacted,
        up_to_date: false,
    })
}

fn meta_blobs(meta: &SegmentMeta) -> Vec<String> {
    let mut blobs = vec![
        segment_blob(&meta.seg_id, "terms.fst"),
        segment_blob(&meta.seg_id, "postings.bin"),
        segment_blob(&meta.seg_id, "docs.bin"),
    ];
    if !meta.dead_hash.is_empty() {
        blobs.push(segment_blob(
            &meta.seg_id,
            &format!("dead-{}.bin", meta.dead_hash),
        ));
    }
    blobs.extend(meta.packs.iter().map(|pack| pack_blob(&pack.hash)));
    blobs
}

/// Delete store blobs the new root no longer references: compaction victims,
/// rebuilt-over segments, and superseded dead-sets. Best-effort — a failed
/// delete only leaks storage, never correctness — and immediate: a reader
/// racing the swap errors loudly on the missing blob and just reruns.
fn collect_garbage(store: &dyn BlobStore, before: &[SegmentMeta], after: &[SegmentMeta]) {
    let kept: std::collections::HashSet<String> = after.iter().flat_map(meta_blobs).collect();
    let mut deleted = std::collections::HashSet::new();
    for meta in before {
        for blob in meta_blobs(meta) {
            if !kept.contains(&blob) && deleted.insert(blob.clone()) && store.delete(&blob).is_err()
            {
                eprintln!("warning: failed to delete unreferenced index blob {blob}");
            }
        }
    }
}

fn live_doc_count(live: &HashMap<&str, (usize, u32, &SourceEntry)>) -> usize {
    live.values()
        .filter(|(_, _, entry)| !entry.failed)
        .map(|(_, _, entry)| entry.doc_count as usize)
        .sum()
}

/// Live (non-failed) doc count over the final segment set.
fn live_after_update(
    store: &dyn BlobStore,
    cache_dir: &Path,
    keep: &[(SegmentMeta, DeadSet)],
) -> Result<usize> {
    let mut total = 0;
    for (meta, dead) in keep {
        let tables = parse_tables(&cached_blob(
            store,
            cache_dir,
            &meta.seg_id,
            "docs.bin",
            meta.docs_len,
            &meta.docs_hash,
        )?)?;
        total += tables
            .sources
            .iter()
            .enumerate()
            .filter(|(source_id, source)| {
                dead.sources.binary_search(&(*source_id as u32)).is_err() && !source.failed
            })
            .map(|(_, source)| source.doc_count as usize)
            .sum::<usize>();
    }
    Ok(total)
}

fn load_dead(store: &dyn BlobStore, cache_dir: &Path, meta: &SegmentMeta) -> Result<DeadSet> {
    if meta.dead_hash.is_empty() {
        return Ok(DeadSet::default());
    }
    let dead = parse_dead(&cached_blob(
        store,
        cache_dir,
        &meta.seg_id,
        &format!("dead-{}.bin", meta.dead_hash),
        meta.dead_len,
        &meta.dead_hash,
    )?)?;
    anyhow::ensure!(
        dead.documents
            .last()
            .is_none_or(|document| *document < meta.doc_count),
        "dead document ID is out of bounds"
    );
    Ok(dead)
}

fn write_dead(store: &dyn BlobStore, seg_id: &str, dead: &DeadSet) -> Result<(String, u64)> {
    let bytes = postcard::to_allocvec(dead)?;
    let hash = sha256_hex(&[&bytes]);
    store
        .put(&segment_blob(seg_id, &format!("dead-{hash}.bin")), &bytes)
        .context("failed to write segment dead set")?;
    Ok((hash, u64::try_from(bytes.len())?))
}

fn should_repack(meta: &SegmentMeta, tables: &SegmentTables, dead: &DeadSet) -> bool {
    if dead.documents.is_empty() {
        return false;
    }
    let dead_documents = dead.documents.len();
    let documents = usize::try_from(meta.doc_count).expect("document count fits usize");
    if dead_documents.saturating_mul(REPACK_DEAD_FRACTION) >= documents {
        return true;
    }
    let total_bytes = tables.documents.iter().fold(0u64, |total, document| {
        total.saturating_add(document.decoded_size)
    });
    let dead_bytes = dead.documents.iter().fold(0u64, |total, document| {
        let index = usize::try_from(*document).expect("document ID fits usize");
        total.saturating_add(tables.documents[index].decoded_size)
    });
    let fraction = u64::try_from(REPACK_DEAD_FRACTION).expect("repack fraction fits u64");
    total_bytes > 0 && dead_bytes.saturating_mul(fraction) >= total_bytes
}

/// Build and PUT one segment over `docs` ((key, listing-etag, size) triples,
/// sorted by key; corpus ids = positions). Returns its meta and the doc
/// table.
fn build_segment_files(
    corpus: &dyn Corpus,
    strategy: Strategy,
    docs: &[(String, String, u64)],
    document_cap: usize,
    progress: Option<&ProgressSender>,
) -> Result<crate::BuiltIndexFiles> {
    let mut built = crate::build_index_files(corpus, strategy, Some(document_cap), progress)?;
    anyhow::ensure!(
        built.tables.sources.len() == docs.len(),
        "corpus source count differs from its listing"
    );
    for (source, (key, version, encoded_size)) in built.tables.sources.iter_mut().zip(docs) {
        anyhow::ensure!(
            source.key == *key,
            "corpus source key differs from its listing"
        );
        source.version.clone_from(version);
        source.encoded_size = *encoded_size;
    }
    Ok(built)
}

fn write_bounded_segments(
    store: &dyn BlobStore,
    strategy: Strategy,
    docs: &[(String, String, u64)],
    doc_cap: usize,
    make_corpus: &CorpusFactory<'_>,
    progress: Option<&ProgressSender>,
) -> Result<Vec<SegmentMeta>> {
    anyhow::ensure!(doc_cap > 0, "segment document cap must be greater than 0");
    anyhow::ensure!(!docs.is_empty(), "refusing to build an empty segment shard");
    let corpus = make_corpus(docs)?;
    match build_segment_files(corpus.as_ref(), strategy, docs, doc_cap, progress) {
        Ok(built) => {
            let meta =
                merge_and_put_segment(store, strategy, built.runs, &built.tables, &built.packs)?;
            return Ok(vec![meta]);
        }
        Err(error) if error.is::<crate::DocumentCapExceeded>() => {}
        Err(error) => return Err(error),
    }
    anyhow::ensure!(
        docs.len() > 1,
        "source {} expands beyond the segment cap of {doc_cap}",
        docs[0].0
    );
    let split = docs.len() / 2;
    let mut segments = write_bounded_segments(
        store,
        strategy,
        &docs[..split],
        doc_cap,
        make_corpus,
        progress,
    )?;
    segments.extend(write_bounded_segments(
        store,
        strategy,
        &docs[split..],
        doc_cap,
        make_corpus,
        progress,
    )?);
    Ok(segments)
}

/// Segment IDs are random, not content-derived: every blob hash a reader
/// trusts is recorded in segments.bin, and a random ID is known before the
/// merge runs, so the dictionary and postings stream to their final keys
/// while the merge produces them.
fn random_seg_id() -> Result<String> {
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes)?;
    Ok(hex_encode(&bytes))
}

pub(crate) fn merge_and_put_segment(
    store: &dyn BlobStore,
    strategy: Strategy,
    runs: Vec<tempfile::TempPath>,
    tables: &SegmentTables,
    packs: &[PackFile],
) -> Result<SegmentMeta> {
    anyhow::ensure!(
        !tables.sources.is_empty(),
        "refusing to write a segment without sources"
    );
    tables.validate()?;
    let pack_metas = packs.iter().map(PackFile::meta).collect::<Vec<_>>();
    let pack_count = tables
        .blocks
        .last()
        .map_or(0usize, |block| block.pack as usize + 1);
    anyhow::ensure!(
        pack_count == packs.len(),
        "pack file count differs from block table"
    );
    for (pack_id, pack) in packs.iter().enumerate() {
        let end = tables
            .blocks
            .iter()
            .rev()
            .find(|block| block.pack as usize == pack_id)
            .map(|block| block.offset + u64::from(block.compressed_len))
            .context("pack file has no blocks")?;
        anyhow::ensure!(
            end == pack.len(),
            "pack file length differs from block table"
        );
    }
    let docs_bytes = postcard::to_allocvec(tables)?;
    let docs_hash = sha256_hex(&[&docs_bytes]);
    let seg_id = random_seg_id()?;
    for pack in packs {
        store.put_file(&pack_blob(pack.hash()), pack.path())?;
    }
    type Published = (
        crate::build::MergedBlob,
        crate::build::MergedBlob,
        String,
        crate::build::PostingsTail,
    );
    let publish = || -> Result<Published> {
        let terms_sink = store.put_streaming(&segment_blob(&seg_id, "terms.fst"))?;
        let postings_sink = store.put_streaming(&segment_blob(&seg_id, "postings.bin"))?;
        let merged = crate::build::merge_posting_runs(
            runs,
            strategy,
            u32::try_from(tables.documents.len())?,
            terms_sink,
            postings_sink,
        )?;
        store.put(&segment_blob(&seg_id, "docs.bin"), &docs_bytes)?;
        Ok(merged)
    };
    let (fst, postings, terms_tail_hash, postings_tail) = match publish() {
        Ok(merged) => merged,
        Err(error) => {
            // Random-keyed blobs from a failed publish are unreferenced and
            // invisible to readers; delete them so they don't wait for GC.
            // Packs stay: they are content-addressed and may be shared.
            for name in ["terms.fst", "postings.bin", "docs.bin"] {
                store.delete(&segment_blob(&seg_id, name)).ok();
            }
            return Err(error);
        }
    };
    let meta = SegmentMeta {
        seg_id,
        doc_count: u32::try_from(tables.documents.len())?,
        terms_fst_len: fst.len,
        terms_fst_hash: fst.hash,
        terms_tail_hash,
        postings_len: postings.len,
        postings_hash: postings.hash,
        postings_data_len: postings_tail.data_len,
        postings_tail_hash: postings_tail.tail_hash,
        docs_len: docs_bytes.len() as u64,
        docs_hash,
        min_key: tables.sources[0].key.clone(),
        max_key: tables.sources[tables.sources.len() - 1].key.clone(),
        dead_hash: String::new(),
        dead_len: 0,
        failed_source_count: u32::try_from(
            tables.sources.iter().filter(|source| source.failed).count(),
        )?,
        packs: pack_metas,
    };
    Ok(meta)
}

struct Segment {
    meta: SegmentMeta,
    map: TermMap,
    dead: DeadSet,
    tables: OnceLock<SegmentTables>,
    postings_table: OnceLock<std::sync::Arc<crate::postings_table::PostingsTableIndex>>,
}

/// Reader over a segmented index: per-segment candidate resolution with the
/// existing batched ranged-GET machinery; doc tables load lazily, only for
/// segments that actually produce candidates.
pub struct SegmentedReader {
    store: Box<dyn BlobStore>,
    cache_dir: PathBuf,
    root_version: String,
    strategy: Strategy,
    segments: Vec<Segment>,
    range_cache_max: u64,
    range_written: std::sync::atomic::AtomicU64,
}

impl SegmentedReader {
    fn range_cache(&self) -> bool {
        self.range_cache_max > 0
    }

    /// The open-time sweep bounds cache size across processes; this bounds
    /// it within one long-lived reader by re-sweeping after every quarter
    /// cap of fresh writes.
    fn note_range_written(&self, bytes: u64) {
        self.range_written
            .fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
        self.sweep_if_due();
    }

    fn sweep_if_due(&self) {
        use std::sync::atomic::Ordering;
        if self.range_cache_max == 0 {
            return;
        }
        if self.range_written.load(Ordering::Relaxed) >= self.range_cache_max / 4 {
            self.range_written.store(0, Ordering::Relaxed);
            evict_range_cache(&self.cache_dir, self.range_cache_max);
        }
    }
}

impl SegmentedReader {
    pub fn open(
        store: Box<dyn BlobStore>,
        cache_dir: &Path,
        source: &SourceIdentity,
    ) -> Result<SegmentedReader> {
        source.validate()?;
        Self::load(store, cache_dir, Some(source))
    }

    pub fn inspect(store: Box<dyn BlobStore>, cache_dir: &Path) -> Result<SegmentedReader> {
        Self::load(store, cache_dir, None)
    }

    fn load(
        store: Box<dyn BlobStore>,
        cache_dir: &Path,
        source: Option<&SourceIdentity>,
    ) -> Result<SegmentedReader> {
        let (bytes, root_version) = store
            .get_versioned("segments.bin")
            .context("reading segments.bin")?
            .context("no index found — run `seagrep index` first")?;
        let list = parse_segment_list(&bytes)
            .context("index is not usable as-is; run `seagrep index` to rebuild")?;
        if let Some(source) = source {
            anyhow::ensure!(
                list.source.can_search(source),
                "index was built for {}, which does not contain requested source {source}",
                list.source
            );
        }
        let strategy = list.strategy;
        let mut segments = Vec::with_capacity(list.segments.len());
        for meta in list.segments {
            // A corrupt cached blob (same length, damaged bytes) self-heals:
            // wipe this segment's cache and refetch once.
            let segment = match load_segment(store.as_ref(), cache_dir, &meta, strategy) {
                Ok(segment) => segment,
                Err(_) => {
                    std::fs::remove_dir_all(cache_dir.join(&meta.seg_id)).ok();
                    load_segment(store.as_ref(), cache_dir, &meta, strategy)?
                }
            };
            segments.push(segment);
        }
        evict_stale_segments(cache_dir, &segments);
        let range_cache_max = range_cache_max()?;
        if range_cache_max > 0 {
            evict_range_cache(cache_dir, range_cache_max);
        }
        Ok(SegmentedReader {
            store,
            cache_dir: cache_dir.to_path_buf(),
            root_version,
            strategy,
            segments,
            range_cache_max,
            range_written: std::sync::atomic::AtomicU64::new(0),
        })
    }

    fn segment_tables<'a>(&self, segment: &'a Segment) -> Result<&'a SegmentTables> {
        if let Some(tables) = segment.tables.get() {
            return Ok(tables);
        }
        let load = || -> Result<SegmentTables> {
            let loaded = parse_tables(&cached_blob(
                self.store.as_ref(),
                &self.cache_dir,
                &segment.meta.seg_id,
                "docs.bin",
                segment.meta.docs_len,
                &segment.meta.docs_hash,
            )?)?;
            validate_segment_tables(&segment.meta, &loaded)?;
            segment.dead.validate(&loaded)?;
            Ok(loaded)
        };
        let loaded = match load() {
            Ok(loaded) => loaded,
            Err(_) => {
                std::fs::remove_file(self.cache_dir.join(&segment.meta.seg_id).join("docs.bin"))
                    .ok();
                load()?
            }
        };
        Ok(segment.tables.get_or_init(|| loaded))
    }

    /// Can any key with `prefix` live in this segment's `[min_key, max_key]`?
    fn prefix_overlaps(meta: &SegmentMeta, prefix: &str) -> bool {
        if meta.max_key.as_str() < prefix {
            return false;
        }
        // The smallest string ABOVE every prefixed key: prefix with its last
        // byte incremented (dropping trailing 0xff bytes). No such string =>
        // unbounded above.
        let mut upper = prefix.as_bytes().to_vec();
        while let Some(&last) = upper.last() {
            if last == 0xff {
                upper.pop();
            } else {
                if let Some(last) = upper.last_mut() {
                    *last += 1;
                }
                break;
            }
        }
        upper.is_empty() || meta.min_key.as_bytes() < upper.as_slice()
    }

    fn has_changed_root(&self) -> Result<bool> {
        Ok(self
            .store
            .get_versioned("segments.bin")?
            .is_none_or(|(_, version)| version != self.root_version))
    }

    fn classify_index_result<T>(&self, result: Result<T>) -> Result<T> {
        match result {
            Ok(value) => Ok(value),
            Err(error) => match self.has_changed_root() {
                Ok(true) => Err(error.context(IndexChanged)),
                Ok(false) => Err(error),
                Err(root_error) => Err(error.context(format!(
                    "also failed to check whether the index root changed: {root_error:#}"
                ))),
            },
        }
    }

    fn read_candidate_batches(
        &self,
        q: &Query,
        key_prefix: Option<&str>,
        batch_size: usize,
        visit: &mut dyn FnMut(Vec<DocAddress>) -> Result<bool>,
    ) -> Result<()> {
        anyhow::ensure!(batch_size > 0, "candidate batch size must be positive");
        let source_prefix =
            key_prefix.map(|prefix| prefix.split_once("!/").map_or(prefix, |(source, _)| source));
        for (segment_id, segment) in self.segments.iter().enumerate() {
            if let Some(prefix) = source_prefix {
                self.classify_index_result(self.segment_tables(segment))?;
                if !Self::prefix_overlaps(&segment.meta, prefix) {
                    continue;
                }
            }
            let postings_name = segment_blob(&segment.meta.seg_id, "postings.bin");
            let remote_values = match &segment.map {
                TermMap::SparseRemote { index } => Some(self.classify_index_result(
                    crate::remote_terms::fetch_query_gram_values(
                        self.store.as_ref(),
                        &segment_blob(&segment.meta.seg_id, "terms.fst"),
                        index,
                        q,
                        &self.cache_dir,
                        &segment.meta.seg_id,
                    ),
                )?),
                _ => None,
            };
            let lookup = |gram: &[u8]| match &remote_values {
                Some(values) => Ok(values
                    .get(&seagrep_core::hash_ngram(gram))
                    .map(|&(packed, len)| crate::eval::TermValue { packed, len })),
                None => segment.map.get(gram),
            };
            let ids = self.classify_index_result(candidates_with(
                lookup,
                segment.meta.doc_count,
                q,
                |needed| {
                    let doc_count = segment.meta.doc_count;
                    let table = self.postings_table(segment)?;
                    let ranges = posting_ranges(needed, doc_count, table.data_len)?;
                    // Fetch at verification-block granularity: every block
                    // checks against the trusted table before any list is
                    // sliced out. Corruption aborts the query loudly —
                    // unverified bytes could hide documents.
                    let mut block_set = std::collections::BTreeSet::new();
                    for &(offset, len) in &ranges {
                        block_set.extend(table.blocks_covering(offset, len)?);
                    }
                    let block_path = |index: usize| {
                        self.cache_dir
                            .join(&segment.meta.seg_id)
                            .join(format!("postings-block-{index:08x}"))
                    };
                    let mut payloads = std::collections::BTreeMap::<usize, bytes::Bytes>::new();
                    let mut missing = Vec::new();
                    for &index in &block_set {
                        let hit = self.range_cache().then(|| {
                            cache::read_verified(
                                &block_path(index),
                                &crate::sparse_table::hex(table.block_hash(index)),
                            )
                        });
                        match hit.flatten() {
                            Some(bytes) => {
                                payloads.insert(index, bytes.into());
                            }
                            None => missing.push(index),
                        }
                    }
                    if !missing.is_empty() {
                        let block_ranges: Vec<(u64, u64)> = missing
                            .iter()
                            .map(|&index| table.block_range(index))
                            .collect();
                        let fetched = self.store.get_ranges(&postings_name, &block_ranges)?;
                        anyhow::ensure!(
                            fetched.len() == missing.len(),
                            "get_ranges returned {} blocks for {} ranges",
                            fetched.len(),
                            missing.len()
                        );
                        for (&index, bytes) in missing.iter().zip(fetched) {
                            table.verify(index, &bytes)?;
                            if self.range_cache() {
                                cache::write_back(&self.cache_dir, &block_path(index), &bytes).ok();
                                self.note_range_written(bytes.len() as u64);
                            }
                            payloads.insert(index, bytes);
                        }
                    }
                    needed
                        .iter()
                        .zip(&ranges)
                        .map(|((&offset, &(count, _)), &(range_offset, range_len))| {
                            let covering = table.blocks_covering(range_offset, range_len)?;
                            let single = covering.len() == 1;
                            let mut assembled = Vec::new();
                            let mut sliced = None;
                            for index in covering {
                                let (block_offset, block_len) = table.block_range(index);
                                let from = range_offset.max(block_offset);
                                let to = (range_offset + range_len).min(block_offset + block_len);
                                let payload = payloads
                                    .get(&index)
                                    .context("missing verified postings block")?;
                                let start = usize::try_from(from - block_offset)?;
                                let end = usize::try_from(to - block_offset)?;
                                if single {
                                    sliced = Some(payload.slice(start..end));
                                } else {
                                    assembled.extend_from_slice(&payload[start..end]);
                                }
                            }
                            let bytes = match sliced {
                                Some(bytes) => bytes,
                                None => assembled.into(),
                            };
                            let decoded = match self.strategy {
                                Strategy::Sparse => crate::delta_blocks::decode_delta_blocks(
                                    &bytes, count, doc_count,
                                )?,
                                Strategy::Trigram => {
                                    crate::decode_posting_block(&bytes, count, doc_count)?
                                }
                            };
                            Ok((offset, decoded))
                        })
                        .collect()
                },
            ))?;
            let mut live = ids
                .into_iter()
                .filter(|id| segment.dead.documents.binary_search(id).is_err())
                .peekable();
            if live.peek().is_none() {
                continue;
            }
            let tables = self.classify_index_result(self.segment_tables(segment))?;
            let capacity = batch_size.min(usize::try_from(segment.meta.doc_count)?);
            let mut batch = Vec::with_capacity(capacity);
            for id in live {
                let document = &tables.documents[id as usize];
                if batch.len() >= batch_size {
                    if !visit(std::mem::take(&mut batch))? {
                        return Ok(());
                    }
                    batch.reserve(capacity);
                }
                let source = &tables.sources[document.source_id as usize];
                batch.push(DocAddress {
                    display_key: document.display_key.clone(),
                    source_key: source.key.clone(),
                    source_version: source.version.clone(),
                    encoded_size: source.encoded_size,
                    encoding: source.encoding,
                    member_path: document.member_path.clone(),
                    index: Some(IndexAddress {
                        segment: u32::try_from(segment_id)?,
                        document: id,
                    }),
                });
            }
            if !batch.is_empty() && !visit(batch)? {
                return Ok(());
            }
        }
        Ok(())
    }

    fn read_candidate_docs(&self, q: &Query, key_prefix: Option<&str>) -> Result<Vec<DocAddress>> {
        let mut documents = Vec::new();
        self.read_candidate_batches(q, key_prefix, 16_384, &mut |batch| {
            documents.extend(batch);
            Ok(true)
        })?;
        documents.sort_unstable_by(|left, right| left.display_key.cmp(&right.display_key));
        Ok(documents)
    }
}

fn posting_ranges(
    needed: &std::collections::BTreeMap<u64, (u32, u64)>,
    doc_count: u32,
    postings_data_len: u64,
) -> Result<Vec<(u64, u64)>> {
    needed
        .iter()
        .map(|(&offset, &(count, len))| {
            anyhow::ensure!(count > 0, "term map contains an empty posting list");
            anyhow::ensure!(
                count <= doc_count,
                "term map posting count exceeds its segment document count"
            );
            anyhow::ensure!(len > 0, "term map posting length is zero");
            let end = offset
                .checked_add(len)
                .context("term map posting length overflows")?;
            anyhow::ensure!(
                end <= postings_data_len,
                "term map posting extends beyond the postings data region"
            );
            Ok((offset, len))
        })
        .collect()
}

/// Sparse dictionaries at or above this size open remotely: only the block
/// index downloads, and queries fetch just the blocks their grams need.
/// `SEAGREP_SPARSE_REMOTE_MIN` overrides the byte threshold (testing and
/// forced-mode verification); a malformed value fails loudly.
fn sparse_remote_terms_min() -> Result<u64> {
    parse_remote_terms_min(std::env::var("SEAGREP_SPARSE_REMOTE_MIN").ok().as_deref())
}

fn parse_remote_terms_min(configured: Option<&str>) -> Result<u64> {
    match configured {
        None => Ok(64 * 1024 * 1024),
        Some(value) => value
            .parse()
            .with_context(|| format!("SEAGREP_SPARSE_REMOTE_MIN is not a byte count: {value:?}")),
    }
}

/// Collects the decoded logical text of one sampled source — archives
/// expanded into member text, exactly as indexing ingests them — capped at
/// the sampling window. Reaching the cap raises `SampleWindowFull` so the
/// decoder stops immediately: a sampled source must never cost more than
/// its window, no matter how far its contents expand.
struct SampleWindow {
    window: Vec<u8>,
    cap: usize,
}

#[derive(Debug)]
struct SampleWindowFull;

impl std::fmt::Display for SampleWindowFull {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("sample window is full")
    }
}

impl std::error::Error for SampleWindowFull {}

impl seagrep_core::DecodeSink for SampleWindow {
    fn begin(&mut self, _: &seagrep_core::LogicalDocumentMeta) -> Result<()> {
        Ok(())
    }

    fn write(&mut self, bytes: &[u8]) -> Result<()> {
        let room = self.cap - self.window.len();
        self.window
            .extend_from_slice(&bytes[..bytes.len().min(room)]);
        if self.window.len() >= self.cap {
            return Err(anyhow::Error::new(SampleWindowFull));
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}

/// Sample a spread of small listing entries, decode them through the same
/// source expansion as indexing (archives included), and classify the
/// decoded text: the sparse strategy wins only when at least two thirds of
/// the sampled bytes read as prose. Objects too large to decode eagerly are
/// skipped; an unsampleable listing conservatively picks trigram.
fn detect_strategy(
    listing: &[(String, String, u64)],
    make_corpus: &CorpusFactory<'_>,
) -> Result<Strategy> {
    const SAMPLE_DOCS: usize = 16;
    const SAMPLE_MAX_ENCODED: u64 = 32 * 1024 * 1024;
    const SAMPLE_WINDOW: usize = 256 * 1024;
    let small: Vec<(String, String, u64)> = listing
        .iter()
        .filter(|(_, _, size)| *size <= SAMPLE_MAX_ENCODED)
        .cloned()
        .collect();
    let small_bytes: u64 = small.iter().map(|(_, _, size)| *size).sum();
    let listing_bytes: u64 = listing.iter().map(|(_, _, size)| *size).sum();
    // Sampleable objects must carry real weight: when almost all bytes live
    // in objects too large to sample, a tiny small-file minority must not
    // choose the strategy for a corpus it does not represent.
    if listing_bytes > 0 && small_bytes * 10 < listing_bytes {
        eprintln!(
            "note: content is dominated by objects too large to sample; using the trigram strategy (--strategy overrides)"
        );
        return Ok(Strategy::Trigram);
    }
    let step = small.len().div_ceil(SAMPLE_DOCS).max(1);
    let picks: Vec<(String, String, u64)> =
        small.into_iter().step_by(step).take(SAMPLE_DOCS).collect();
    let mut prose_bytes = 0u64;
    let mut classified_bytes = 0u64;
    let mut classified_docs = 0usize;
    if !picks.is_empty() {
        let corpus = make_corpus(&picks)?;
        for (idx, (key, _, _)) in picks.iter().enumerate() {
            let Ok(bytes) = corpus.fetch(idx) else {
                continue;
            };
            let mut sample = SampleWindow {
                window: Vec::new(),
                cap: SAMPLE_WINDOW,
            };
            if let Err(error) =
                seagrep_core::decode_source(key, bytes, seagrep_core::DECODE_LIMITS, &mut sample)
            {
                if !error.is::<SampleWindowFull>() {
                    continue;
                }
            }
            match seagrep_core::is_prose_like(&sample.window) {
                Some(true) => {
                    prose_bytes += sample.window.len() as u64;
                    classified_bytes += sample.window.len() as u64;
                    classified_docs += 1;
                }
                Some(false) => {
                    classified_bytes += sample.window.len() as u64;
                    classified_docs += 1;
                }
                None => {}
            }
        }
    }
    // A vote needs quorum: if most samples vanished or failed to decode,
    // the survivors do not speak for the corpus.
    if classified_bytes == 0 || classified_docs * 2 < picks.len() {
        eprintln!(
            "note: not enough content could be sampled; using the trigram strategy (--strategy overrides)"
        );
        return Ok(Strategy::Trigram);
    }
    let strategy = if prose_bytes * 3 >= classified_bytes * 2 {
        Strategy::Sparse
    } else {
        Strategy::Trigram
    };
    match strategy {
        Strategy::Sparse => eprintln!(
            "note: sampled content reads as natural-language prose; using the sparse strategy (--strategy overrides)"
        ),
        Strategy::Trigram => eprintln!(
            "note: sampled content reads as structured text; using the trigram strategy (--strategy overrides)"
        ),
    }
    Ok(strategy)
}

/// Byte cap for the on-disk pack/postings range cache. `SEAGREP_CACHE_MAX`
/// overrides; `0` disables range caching entirely (gram-block and whole-file
/// caches are unaffected — they are bounded and load-bearing).
fn range_cache_max() -> Result<u64> {
    const DEFAULT_MAX: u64 = 4 * 1024 * 1024 * 1024;
    match std::env::var("SEAGREP_CACHE_MAX") {
        Ok(value) => value
            .trim()
            .parse::<u64>()
            .with_context(|| format!("SEAGREP_CACHE_MAX is not a byte count: {value:?}")),
        Err(std::env::VarError::NotPresent) => Ok(DEFAULT_MAX),
        Err(error) => Err(error.into()),
    }
}

/// Best-effort size cap for range-cache files (`pack-*`, `postings-*`):
/// when they exceed the cap, the oldest-modified files go first until usage
/// drops under three quarters of it. Whole-file and gram-block caches are
/// never touched; stale-segment eviction remains the primary cleaner.
fn evict_range_cache(cache_dir: &Path, max_bytes: u64) {
    let Ok(segments) = std::fs::read_dir(cache_dir) else {
        return;
    };
    let mut files = Vec::new();
    let mut total = 0u64;
    for segment in segments.flatten() {
        // Never follow symlinks: a link planted in the cache dir must not
        // let the sweep enumerate or delete files outside it.
        if !segment.file_type().is_ok_and(|kind| kind.is_dir()) {
            continue;
        }
        let Ok(entries) = std::fs::read_dir(segment.path()) else {
            continue;
        };
        for entry in entries.flatten() {
            if !entry.file_type().is_ok_and(|kind| kind.is_file()) {
                continue;
            }
            let name = entry.file_name();
            let name = name.to_string_lossy();
            let evictable = name.starts_with("pack-")
                || name.starts_with("postings-")
                || name.starts_with(".tmp");
            if !evictable {
                continue;
            }
            let Ok(meta) = entry.metadata() else {
                continue;
            };
            let modified = meta.modified().unwrap_or(std::time::UNIX_EPOCH);
            total = total.saturating_add(meta.len());
            files.push((modified, meta.len(), entry.path()));
        }
    }
    if total <= max_bytes {
        return;
    }
    files.sort_unstable_by_key(|(modified, _, _)| *modified);
    let target = max_bytes / 4 * 3;
    for (_, len, path) in files {
        if total <= target {
            break;
        }
        if std::fs::remove_file(&path).is_ok() {
            total = total.saturating_sub(len);
        }
    }
}

impl SegmentedReader {
    /// The segment's postings verification table, fetched once through the
    /// read-through tail cache and trusted via `postings_tail_hash`.
    fn postings_table(
        &self,
        segment: &Segment,
    ) -> Result<std::sync::Arc<crate::postings_table::PostingsTableIndex>> {
        if let Some(table) = segment.postings_table.get() {
            return Ok(table.clone());
        }
        let meta = &segment.meta;
        let block_count = meta
            .postings_data_len
            .div_ceil(crate::postings_table::VERIFY_BLOCK_BYTES as u64);
        let tail_len = block_count * 32 + crate::postings_table::FOOTER_BYTES as u64;
        anyhow::ensure!(
            meta.postings_data_len
                .checked_add(tail_len)
                .is_some_and(|total| total == meta.postings_len),
            "postings verification table does not fit its blob"
        );
        let tail = cached_bytes(
            &self.cache_dir,
            &meta.seg_id,
            "postings.tail",
            &meta.postings_tail_hash,
            &|| {
                let name = segment_blob(&meta.seg_id, "postings.bin");
                let mut ranges = self
                    .store
                    .get_ranges(&name, &[(meta.postings_data_len, tail_len)])?;
                anyhow::ensure!(ranges.len() == 1, "postings tail fetch returned no range");
                Ok(ranges.remove(0).to_vec())
            },
        )?;
        let table = crate::postings_table::PostingsTableIndex::parse(meta.postings_len, &tail)?;
        anyhow::ensure!(
            table.data_len == meta.postings_data_len,
            "postings table data length does not match segment metadata"
        );
        let _ = segment.postings_table.set(std::sync::Arc::new(table));
        Ok(segment
            .postings_table
            .get()
            .expect("table was just set")
            .clone())
    }
}

fn load_segment(
    store: &dyn BlobStore,
    cache_dir: &Path,
    meta: &SegmentMeta,
    strategy: Strategy,
) -> Result<Segment> {
    if strategy == Strategy::Sparse
        && !meta.terms_tail_hash.is_empty()
        && meta.terms_fst_len >= sparse_remote_terms_min()?
    {
        let tail = cached_bytes(
            cache_dir,
            &meta.seg_id,
            "terms.tail",
            &meta.terms_tail_hash,
            &|| {
                crate::remote_terms::fetch_index_tail(
                    store,
                    &segment_blob(&meta.seg_id, "terms.fst"),
                    meta.terms_fst_len,
                )
            },
        )?;
        let index = crate::remote_terms::parse_index_tail(
            meta.terms_fst_len,
            &tail,
            &meta.terms_tail_hash,
        )?;
        return Ok(Segment {
            map: TermMap::SparseRemote { index },
            dead: load_dead(store, cache_dir, meta)?,
            tables: OnceLock::new(),
            postings_table: OnceLock::new(),
            meta: meta.clone(),
        });
    }
    let path = cached_file(
        store,
        cache_dir,
        &meta.seg_id,
        "terms.fst",
        meta.terms_fst_len,
        &meta.terms_fst_hash,
    )?;
    let dead = load_dead(store, cache_dir, meta)?;
    let bytes = map_file(&path)?;
    #[cfg(unix)]
    bytes.advise(memmap2::Advice::Random)?;
    let map = TermMap::open(bytes, strategy)?;
    Ok(Segment {
        map,
        dead,
        tables: OnceLock::new(),
        postings_table: OnceLock::new(),
        meta: meta.clone(),
    })
}

fn evict_stale_segments(cache_dir: &Path, segments: &[Segment]) {
    let current: std::collections::HashSet<&str> = segments
        .iter()
        .map(|segment| segment.meta.seg_id.as_str())
        .collect();
    let Ok(entries) = std::fs::read_dir(cache_dir) else {
        return;
    };
    for entry in entries.flatten() {
        if !current.contains(entry.file_name().to_string_lossy().as_ref()) {
            std::fs::remove_dir_all(entry.path()).ok();
        }
    }
}

impl crate::IndexReader for SegmentedReader {
    fn excluded_objects(&self) -> usize {
        self.segments
            .iter()
            .map(|segment| segment.meta.failed_source_count as usize)
            .sum()
    }

    fn strategy(&self) -> Strategy {
        self.strategy
    }

    fn total_docs(&self) -> usize {
        self.segments
            .iter()
            .map(|segment| segment.meta.doc_count as usize - segment.dead.documents.len())
            .sum()
    }

    fn candidate_docs(&self, q: &Query, key_prefix: Option<&str>) -> Result<Vec<DocAddress>> {
        self.read_candidate_docs(q, key_prefix)
    }

    fn visit_candidates(
        &self,
        q: &Query,
        key_prefix: Option<&str>,
        batch_size: usize,
        visit: &mut dyn FnMut(Vec<DocAddress>) -> Result<bool>,
    ) -> Result<()> {
        self.read_candidate_batches(q, key_prefix, batch_size, visit)
    }

    fn stats(&self) -> crate::IndexStats {
        crate::IndexStats {
            distinct_grams: self.segments.iter().map(|s| s.map.len() as u64).sum(),
            terms_fst_bytes: self.segments.iter().map(|s| s.meta.terms_fst_len).sum(),
            postings_bytes: self.segments.iter().map(|s| s.meta.postings_len).sum(),
        }
    }
}

impl seagrep_core::DocFetcher for SegmentedReader {
    fn fetch_each(
        &self,
        documents: &[DocAddress],
        consume: &mut dyn FnMut(usize, seagrep_core::DocumentBody) -> Result<()>,
    ) -> Result<()> {
        let mut grouped = std::collections::BTreeMap::<u32, Vec<(usize, u32)>>::new();
        for (index, document) in documents.iter().enumerate() {
            let address = document
                .index
                .as_ref()
                .context("candidate has no index snapshot address")?;
            grouped
                .entry(address.segment)
                .or_default()
                .push((index, address.document));
        }
        for (segment_id, addresses) in grouped {
            let segment = self
                .segments
                .get(usize::try_from(segment_id)?)
                .context("candidate segment is out of bounds")?;
            let tables = self.classify_index_result(self.segment_tables(segment))?;
            let requests = addresses
                .iter()
                .map(|(index, document_id)| {
                    let document = tables
                        .documents
                        .get(usize::try_from(*document_id)?)
                        .context("candidate document is out of bounds")?;
                    anyhow::ensure!(
                        document.display_key == documents[*index].display_key,
                        "candidate display key differs from its index entry"
                    );
                    Ok(crate::pack::PackRequest {
                        index: *index,
                        slice: crate::pack::PackSlice {
                            first_block: document.first_block,
                            block_offset: document.block_offset,
                        },
                        decoded_size: document.decoded_size,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let pack_cache = crate::pack::PackBlockCache {
                cache_dir: &self.cache_dir,
                seg_id: &segment.meta.seg_id,
                note_written: &|bytes| self.note_range_written(bytes),
            };
            let fetched = crate::pack::fetch_documents(
                self.store.as_ref(),
                self.range_cache().then_some(&pack_cache),
                &segment.meta.packs,
                &tables.blocks,
                &requests,
                consume,
            );
            self.classify_index_result(fetched)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_source() -> SourceIdentity {
        SourceIdentity::Local {
            prefix: "/test/".into(),
        }
    }

    fn segment() -> SegmentMeta {
        SegmentMeta {
            seg_id: "a".repeat(64),
            doc_count: 1,
            terms_fst_len: 1,
            terms_fst_hash: "b".repeat(64),
            terms_tail_hash: String::new(),
            postings_len: 1,
            postings_hash: "c".repeat(64),
            postings_data_len: 0,
            postings_tail_hash: "e".repeat(64),
            docs_len: 1,
            docs_hash: "d".repeat(64),
            min_key: "a".into(),
            max_key: "z".into(),
            dead_hash: String::new(),
            dead_len: 0,
            failed_source_count: 0,
            packs: vec![PackMeta {
                hash: "e".repeat(64),
                len: 1,
            }],
        }
    }

    fn encoded(segments: Vec<SegmentMeta>) -> Vec<u8> {
        postcard::to_allocvec(&SegmentList {
            format: INDEX_FORMAT,
            source: test_source(),
            strategy: Strategy::Trigram,
            segments,
        })
        .unwrap()
    }

    #[test]
    fn remote_terms_threshold_rejects_malformed_configuration() {
        assert_eq!(parse_remote_terms_min(None).unwrap(), 64 * 1024 * 1024);
        assert_eq!(parse_remote_terms_min(Some("1")).unwrap(), 1);
        let error = parse_remote_terms_min(Some("64MB")).unwrap_err();
        assert!(
            error.to_string().contains("SEAGREP_SPARSE_REMOTE_MIN"),
            "{error:#}"
        );
    }

    #[test]
    fn unreadable_root_advises_reindex_only_on_the_search_path() {
        let parse_error = match parse_segment_list(b"garbage") {
            Ok(_) => panic!("garbage must not parse"),
            Err(error) => error,
        };
        assert!(
            !format!("{parse_error:#}").contains("run `seagrep index`"),
            "index-path notes embed this message next to 'rebuilding from scratch', so remediation advice would contradict: {parse_error:#}"
        );

        let dir = tempfile::tempdir().unwrap();
        let store = seagrep_core::LocalBlobStore::new(dir.path());
        store.put("segments.bin", b"garbage").unwrap();
        let open_error = match SegmentedReader::inspect(Box::new(store), dir.path()) {
            Ok(_) => panic!("corrupt root must not open"),
            Err(error) => error,
        };
        assert!(
            format!("{open_error:#}").contains("run `seagrep index` to rebuild"),
            "{open_error:#}"
        );
    }

    #[test]
    fn segment_list_rejects_unsafe_and_inconsistent_metadata() {
        parse_segment_list(&encoded(vec![segment()])).unwrap();

        let mut unsafe_id = segment();
        unsafe_id.seg_id = "../outside".into();
        assert!(parse_segment_list(&encoded(vec![unsafe_id])).is_err());

        let duplicate = segment();
        assert!(parse_segment_list(&encoded(vec![duplicate.clone(), duplicate])).is_err());

        let mut reversed = segment();
        reversed.min_key = "z".into();
        reversed.max_key = "a".into();
        assert!(parse_segment_list(&encoded(vec![reversed])).is_err());

        let mut invalid_dead = segment();
        invalid_dead.dead_hash = "b".repeat(64);
        assert!(parse_segment_list(&encoded(vec![invalid_dead])).is_err());
    }

    #[test]
    fn source_identity_allows_only_same_backend_subtrees() {
        let local = test_source();
        assert!(local.can_search(&SourceIdentity::Local {
            prefix: "/test/child/".into()
        }));
        assert!(!local.can_search(&SourceIdentity::Local {
            prefix: "/other/".into()
        }));

        let s3 = SourceIdentity::S3 {
            endpoint: "https://s3.us-east-1.amazonaws.com".into(),
            bucket: "source".into(),
            prefix: "logs/".into(),
        };
        assert!(s3.can_search(&SourceIdentity::S3 {
            endpoint: "https://s3.us-east-1.amazonaws.com".into(),
            bucket: "source".into(),
            prefix: "logs/app/".into(),
        }));
        assert!(!s3.can_search(&SourceIdentity::S3 {
            endpoint: "http://127.0.0.1:9000".into(),
            bucket: "source".into(),
            prefix: "logs/".into(),
        }));
    }

    #[test]
    fn index_update_rejects_source_change_without_rebuild() {
        let store_dir = tempfile::tempdir().unwrap();
        let cache_dir = tempfile::tempdir().unwrap();
        let store = seagrep_core::LocalBlobStore::new(store_dir.path());
        store.put("segments.bin", &encoded(Vec::new())).unwrap();
        let other = SourceIdentity::Local {
            prefix: "/other/".into(),
        };
        let error = update_index(
            &store,
            cache_dir.path(),
            &other,
            Some(Strategy::Trigram),
            &[],
            UpdateOptions::default(),
            &|_| anyhow::bail!("source mismatch must fail before fetching"),
        )
        .expect_err("source mismatch must fail");
        assert!(error.to_string().contains("use --rebuild to replace it"));
    }

    #[test]
    fn segment_root_references_uploaded_content_packs() {
        let store_dir = tempfile::tempdir().unwrap();
        let cache_dir = tempfile::tempdir().unwrap();
        let store = seagrep_core::LocalBlobStore::new(store_dir.path());
        let listing = vec![("a.txt".to_owned(), "v1".to_owned(), 5)];
        update_index(
            &store,
            cache_dir.path(),
            &test_source(),
            Some(Strategy::Trigram),
            &listing,
            UpdateOptions::default(),
            &|_| {
                Ok(Box::new(seagrep_core::testutil::MemCorpus::new(
                    vec!["a.txt".to_owned()],
                    vec![b"alpha".to_vec()],
                )))
            },
        )
        .unwrap();

        let root = store.get("segments.bin").unwrap().unwrap();
        let list = parse_segment_list(&root).unwrap();
        let pack = &list.segments[0].packs[0];
        let bytes = store
            .get(&format!("packs/{}.pack", pack.hash))
            .unwrap()
            .unwrap();
        assert_eq!(bytes.len() as u64, pack.len);
        assert_eq!(sha256_hex(&[&bytes]), pack.hash);
    }

    #[test]
    fn segment_tables_reject_mismatched_key_bounds() {
        let tables = SegmentTables {
            sources: vec![SourceEntry {
                key: "actual".into(),
                version: "v1".into(),
                encoded_size: 1,
                encoding: seagrep_core::SourceEncoding::Raw,
                first_doc: 0,
                doc_count: 1,
                failed: false,
                retry: false,
            }],
            documents: vec![DocEntry {
                display_key: "actual".into(),
                source_id: 0,
                member_path: None,
                decoded_size: 1,
                first_block: 0,
                block_offset: 0,
            }],
            blocks: vec![crate::pack::PackBlock {
                pack: 0,
                offset: 0,
                compressed_len: 1,
                decoded_len: 1,
                hash: [0; 32],
            }],
        };
        let mut meta = segment();
        meta.min_key = "wrong".into();
        meta.max_key = "wrong".into();
        assert!(validate_segment_tables(&meta, &tables).is_err());
        meta.min_key = "actual".into();
        meta.max_key = "actual".into();
        validate_segment_tables(&meta, &tables).unwrap();
    }

    #[test]
    fn cached_blob_repairs_same_length_corruption() {
        let store_dir = tempfile::tempdir().unwrap();
        let cache_dir = tempfile::tempdir().unwrap();
        let store = seagrep_core::LocalBlobStore::new(store_dir.path());
        let segment_id = "a".repeat(64);
        let name = "docs.bin";
        store
            .put(&segment_blob(&segment_id, name), b"good")
            .unwrap();
        let cached = cache_dir.path().join(&segment_id).join(name);
        std::fs::create_dir_all(cached.parent().unwrap()).unwrap();
        std::fs::write(&cached, b"baad").unwrap();
        let hash = sha256_hex(&[b"good"]);
        assert_eq!(
            cached_blob(&store, cache_dir.path(), &segment_id, name, 4, &hash).unwrap(),
            b"good"
        );
        assert_eq!(std::fs::read(cached).unwrap(), b"good");

        let name = "terms.fst";
        store
            .put(&segment_blob(&segment_id, name), b"good")
            .unwrap();
        let cached = cache_dir.path().join(&segment_id).join(name);
        std::fs::write(&cached, b"baad").unwrap();
        let path = cached_file(&store, cache_dir.path(), &segment_id, name, 4, &hash).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"good");
        assert!(path.with_file_name("terms.fst.verified").is_file());
    }

    #[test]
    fn posting_ranges_reject_impossible_metadata() {
        let needed = std::collections::BTreeMap::from([(0u64, (2u32, 1u64))]);
        assert!(posting_ranges(&needed, 1, 1).is_err());
    }

    #[test]
    fn unmergeable_segment_set_converges_without_root_rewrite() {
        let store_dir = tempfile::tempdir().unwrap();
        let cache_dir = tempfile::tempdir().unwrap();
        let store = seagrep_core::LocalBlobStore::new(store_dir.path());
        let mut segments = Vec::new();
        let mut listing = Vec::new();
        for index in 0..=SEGMENT_COUNT_TARGET {
            let key = format!("doc-{index}");
            let tables = SegmentTables {
                sources: vec![SourceEntry {
                    key: key.clone(),
                    version: "v1".into(),
                    encoded_size: 1,
                    encoding: seagrep_core::SourceEncoding::Raw,
                    first_doc: 0,
                    doc_count: 1,
                    failed: false,
                    retry: false,
                }],
                documents: vec![DocEntry {
                    display_key: key.clone(),
                    source_id: 0,
                    member_path: None,
                    decoded_size: 1,
                    first_block: 0,
                    block_offset: 0,
                }],
                blocks: vec![crate::pack::PackBlock {
                    pack: 0,
                    offset: 0,
                    compressed_len: 1,
                    decoded_len: 1,
                    hash: [0; 32],
                }],
            };
            let mut builder = crate::pack::PackBuilder::production().unwrap();
            builder.append(std::io::Cursor::new([0]), 1).unwrap();
            let packed = builder.finish().unwrap();
            let mut tables = tables;
            tables.blocks = packed.blocks;
            let mut meta = merge_and_put_segment(
                &store,
                Strategy::Trigram,
                Vec::new(),
                &tables,
                &packed.packs,
            )
            .unwrap();
            meta.postings_len = MERGE_POSTINGS_CAP + 1;
            segments.push(meta);
            listing.push((key, "v1".to_owned(), 1));
        }
        let root = postcard::to_allocvec(&SegmentList {
            format: INDEX_FORMAT,
            source: test_source(),
            strategy: Strategy::Trigram,
            segments,
        })
        .unwrap();
        store.put("segments.bin", &root).unwrap();
        let before = store.get_versioned("segments.bin").unwrap().unwrap().1;
        let report = update_index(
            &store,
            cache_dir.path(),
            &test_source(),
            Some(Strategy::Trigram),
            &listing,
            UpdateOptions::default(),
            &|_| anyhow::bail!("unchanged index should not fetch"),
        )
        .unwrap();
        let after = store.get_versioned("segments.bin").unwrap().unwrap().1;
        assert!(report.up_to_date);
        assert_eq!(before, after);
    }

    #[test]
    fn compaction_rejects_overflowing_segment_sizes_without_panicking() {
        let store_dir = tempfile::tempdir().unwrap();
        let cache_dir = tempfile::tempdir().unwrap();
        let store = seagrep_core::LocalBlobStore::new(store_dir.path());
        for (terms_fst_len, postings_len, docs_len) in
            [(u64::MAX, 0, 0), (0, u64::MAX, 0), (0, 0, u64::MAX)]
        {
            let mut segments = (0..=SEGMENT_COUNT_TARGET)
                .map(|_| {
                    let mut meta = segment();
                    meta.terms_fst_len = terms_fst_len;
                    meta.postings_len = postings_len;
                    meta.docs_len = docs_len;
                    (meta, DeadSet::default())
                })
                .collect();
            assert!(
                !maybe_compact(&store, cache_dir.path(), Strategy::Trigram, &mut segments).unwrap()
            );
        }
    }

    #[test]
    fn segment_build_splits_on_logical_document_count() {
        let store_dir = tempfile::tempdir().unwrap();
        let store = seagrep_core::LocalBlobStore::new(store_dir.path());
        let docs = (0..5)
            .map(|index| (format!("doc-{index}"), "v1".to_owned(), 1))
            .collect::<Vec<_>>();
        let factory = |shard: &[(String, String, u64)]| -> Result<Box<dyn Corpus>> {
            let keys = shard
                .iter()
                .map(|entry| entry.0.clone())
                .collect::<Vec<_>>();
            let bodies = keys
                .iter()
                .map(|key| format!("body {key}").into_bytes())
                .collect::<Vec<_>>();
            Ok(Box::new(seagrep_core::testutil::MemCorpus::new(
                keys, bodies,
            )))
        };
        let segments =
            write_bounded_segments(&store, Strategy::Trigram, &docs, 2, &factory, None).unwrap();
        assert_eq!(
            segments
                .iter()
                .map(|segment| segment.doc_count)
                .collect::<Vec<_>>(),
            vec![2, 1, 2]
        );
        assert!(segments.iter().all(|segment| segment.doc_count <= 2));
    }

    #[test]
    fn segment_cap_stops_before_later_archive_failure() {
        let store_dir = tempfile::tempdir().unwrap();
        let store = seagrep_core::LocalBlobStore::new(store_dir.path());
        let docs = vec![("bundle.zip".to_owned(), "v1".to_owned(), 1)];
        let body = seagrep_core::testutil::encode::zip(&[
            ("a.log", b"a"),
            ("b.log", b"b"),
            ("c.log", b"c"),
            ("../invalid.log", b"invalid"),
        ]);
        let factory = |shard: &[(String, String, u64)]| -> Result<Box<dyn Corpus>> {
            Ok(Box::new(seagrep_core::testutil::MemCorpus::new(
                vec![shard[0].0.clone()],
                vec![body.clone()],
            )))
        };
        let error = write_bounded_segments(&store, Strategy::Trigram, &docs, 2, &factory, None)
            .err()
            .expect("one source exceeds the cap");
        assert!(error.to_string().contains("segment cap of 2"), "{error:#}");
    }
}
