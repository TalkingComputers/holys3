//! Segmented incremental index over a `BlobStore`.
//!
//! Layout under the store root (`<id>` = sha256 of the three blobs' bytes,
//! so identical ids imply identical bytes — blobs are write-once and
//! cache-forever):
//!
//! ```text
//! segments.bin                  root pointer (SegmentList), rewritten per index run
//! segments/<id>/terms.fst
//! segments/<id>/postings.bin
//! segments/<id>/docs.bin
//! segments/<id>/dead-<hash>.bin immutable dead-id sets, referenced by hash
//! ```
//!
//! `holys3 index` becomes a diff: list the bucket, compare (key, etag)
//! against the union of segment doc tables, build bounded segments over the
//! changes, mark superseded entries dead, and atomically swap segments.bin.

use crate::format::{parse_dead, parse_tables, DeadSet, DocEntry, SegmentTables, SourceEntry};
use crate::{candidates_with, INDEX_FORMAT};
use anyhow::{Context, Result};
use holys3_core::{BlobStore, Corpus, DocAddress, DocId, Strategy};
use holys3_query::Query;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

/// Per-segment doc cap: keeps every per-gram posting list far below the
/// 2^24 `pack_posting` ceiling, and bounds build memory.
const SEGMENT_DOC_CAP: usize = 4_000_000;
/// Compact (merge two adjacent segments) when more live segments than this.
const SEGMENT_COUNT_TARGET: usize = 8;
/// Never merge segments whose combined postings exceed this many bytes.
const MERGE_POSTINGS_CAP: u64 = 256 * 1024 * 1024;

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct SegmentMeta {
    pub seg_id: String,
    pub doc_count: u32,
    pub terms_fst_len: u64,
    pub terms_fst_hash: String,
    pub postings_len: u64,
    pub postings_hash: String,
    pub docs_len: u64,
    pub docs_hash: String,
    pub min_key: String,
    pub max_key: String,
    pub dead_hash: String,
    pub dead_len: u64,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct SegmentList {
    pub format: u32,
    pub strategy: Strategy,
    pub segments: Vec<SegmentMeta>,
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

fn parse_segment_list(bytes: &[u8]) -> Result<SegmentList> {
    let list: SegmentList = postcard::from_bytes(bytes)
        .context("segments.bin unreadable; run `holys3 index` to rebuild")?;
    anyhow::ensure!(
        list.format == INDEX_FORMAT,
        "index format {} is not the current {INDEX_FORMAT}; run `holys3 index` to rebuild",
        list.format
    );
    let mut segment_ids = std::collections::HashSet::with_capacity(list.segments.len());
    for segment in &list.segments {
        anyhow::ensure!(
            is_sha256(&segment.seg_id),
            "segment ID is not a SHA-256 hash"
        );
        anyhow::ensure!(
            is_sha256(&segment.terms_fst_hash)
                && is_sha256(&segment.postings_hash)
                && is_sha256(&segment.docs_hash),
            "segment blob hash is not a SHA-256 hash"
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

/// Read a segment blob through the local content-addressed cache. Cache
/// entries are immutable by construction (the path embeds a content hash),
/// so a cache hit never refetches; writes are atomic (temp + rename).
fn cached_blob(
    store: &dyn BlobStore,
    cache_dir: &Path,
    seg_id: &str,
    name: &str,
    expected_len: u64,
    expected_hash: &str,
) -> Result<Vec<u8>> {
    let cache_path = cache_dir.join(seg_id).join(name);
    if let Ok(bytes) = std::fs::read(&cache_path) {
        set_cache_path_mode(&cache_path).ok();
        if bytes.len() as u64 == expected_len && sha256_hex(&[&bytes]) == expected_hash {
            return Ok(bytes);
        }
        std::fs::remove_file(&cache_path).ok();
    }
    let bytes = store
        .get(&segment_blob(seg_id, name))?
        .with_context(|| format!("segment blob {name} of {seg_id} missing from the store"))?;
    anyhow::ensure!(
        bytes.len() as u64 == expected_len,
        "segment blob {name} of {seg_id} is {} bytes, expected {expected_len}",
        bytes.len()
    );
    anyhow::ensure!(
        sha256_hex(&[&bytes]) == expected_hash,
        "segment blob {name} of {seg_id} failed its SHA-256 check"
    );
    // Cache population is best-effort: a concurrent eviction (another search
    // process opening a newer index) may yank this directory mid-write, and
    // that must not fail a search that already holds the bytes.
    let cache = || -> std::io::Result<()> {
        if let Some(parent) = cache_path.parent() {
            std::fs::create_dir_all(parent)?;
            set_cache_dir_mode(cache_dir)?;
            set_cache_dir_mode(parent)?;
        }
        let tmp = cache_path.with_file_name(format!("{name}.tmp.{}", std::process::id()));
        let mut file = std::fs::File::create(&tmp)?;
        set_cache_file_mode(&file)?;
        std::io::Write::write_all(&mut file, &bytes)?;
        std::fs::rename(&tmp, &cache_path)
    };
    cache().ok();
    Ok(bytes)
}

#[cfg(unix)]
fn set_cache_dir_mode(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
}

#[cfg(not(unix))]
fn set_cache_dir_mode(path: &Path) -> std::io::Result<()> {
    let _ = path;
    Ok(())
}

#[cfg(unix)]
fn set_cache_file_mode(file: &std::fs::File) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    file.set_permissions(std::fs::Permissions::from_mode(0o600))
}

#[cfg(not(unix))]
fn set_cache_file_mode(file: &std::fs::File) -> std::io::Result<()> {
    let _ = file;
    Ok(())
}

fn set_cache_path_mode(path: &Path) -> std::io::Result<()> {
    let file = std::fs::OpenOptions::new().read(true).open(path)?;
    set_cache_file_mode(&file)
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
pub fn update_index(
    store: &dyn BlobStore,
    cache_dir: &Path,
    strategy: Strategy,
    listing: &[(String, String, u64)],
    rebuild: bool,
    make_corpus: &CorpusFactory<'_>,
) -> Result<UpdateReport> {
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
    let existing = if rebuild {
        if let RootState::Loaded(list) = root {
            replaced = list.segments;
        }
        Vec::new()
    } else {
        match root {
            RootState::Loaded(list) if list.strategy == strategy => list.segments,
            RootState::Loaded(list) => {
                eprintln!("note: index strategy changed; rebuilding from scratch");
                forced = true;
                replaced = list.segments;
                Vec::new()
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
    if to_add.is_empty() && newly_dead.is_empty() && !forced && !needs_compaction && !root_missing {
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

    // Fold the new deaths into per-segment dead sets, then drop fully-dead
    // segments (collect_garbage deletes their blobs after the root swap).
    let mut metas = existing;
    for group in newly_dead.chunk_by(|a, b| a.0 == b.0) {
        let seg_idx = group[0].0;
        let mut dead = dead_sets[seg_idx].clone();
        for &(_, source_id) in group {
            dead.sources.push(source_id);
            let source = &tables[seg_idx].sources[source_id as usize];
            dead.documents
                .extend(source.first_doc..source.first_doc + source.doc_count);
        }
        dead.sources.sort_unstable();
        dead.sources.dedup();
        dead.documents.sort_unstable();
        dead.documents.dedup();
        write_dead(store, &mut metas[seg_idx], &dead)?;
        dead_sets[seg_idx] = dead;
    }
    // Snapshot AFTER the dead-set rewrites: a segment that just got a fresh
    // dead blob and then drops out (fully dead, or merged away) must have
    // that fresh blob GC'd too, not only its pre-run one.
    replaced.extend(metas.iter().cloned());
    let mut keep: Vec<(SegmentMeta, DeadSet)> = metas
        .into_iter()
        .zip(dead_sets)
        .enumerate()
        .filter(|(seg_idx, (_, dead))| dead.sources.len() < tables[*seg_idx].sources.len())
        .map(|(_, item)| item)
        .collect();

    // Build the new segment(s) over the changes, capped.
    for shard in to_add.chunks(SEGMENT_DOC_CAP) {
        for meta in write_bounded_segments(store, strategy, shard, SEGMENT_DOC_CAP, make_corpus)? {
            // newborns are GC candidates too: a segment born and compacted away
            // in the SAME run would otherwise be in neither before nor after
            replaced.push(meta.clone());
            keep.push((meta, DeadSet::default()));
        }
    }

    let compacted = maybe_compact(store, cache_dir, &mut keep)?;

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
        "another holys3 index run updated this index concurrently; rerun to pick up its result"
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
    blobs
}

/// Delete store blobs the new root no longer references: compaction victims,
/// rebuilt-over segments, and superseded dead-sets. Best-effort — a failed
/// delete only leaks storage, never correctness — and immediate: a reader
/// racing the swap errors loudly on the missing blob and just reruns.
fn collect_garbage(store: &dyn BlobStore, before: &[SegmentMeta], after: &[SegmentMeta]) {
    let kept: std::collections::HashSet<String> = after.iter().flat_map(meta_blobs).collect();
    for meta in before {
        for blob in meta_blobs(meta) {
            if !kept.contains(&blob) && store.delete(&blob).is_err() {
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

fn write_dead(store: &dyn BlobStore, meta: &mut SegmentMeta, dead: &DeadSet) -> Result<()> {
    let bytes = postcard::to_allocvec(dead)?;
    let hash = sha256_hex(&[&bytes]);
    store.put(
        &segment_blob(&meta.seg_id, &format!("dead-{hash}.bin")),
        &bytes,
    )?;
    meta.dead_hash = hash;
    meta.dead_len = bytes.len() as u64;
    Ok(())
}

/// Build and PUT one segment over `docs` ((key, listing-etag, size) triples,
/// sorted by key; corpus ids = positions). Returns its meta and the doc
/// table.
fn build_segment_files(
    corpus: &dyn Corpus,
    strategy: Strategy,
    docs: &[(String, String, u64)],
) -> Result<crate::BuiltIndexFiles> {
    let mut built = crate::build_index_files(corpus, strategy)?;
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
) -> Result<Vec<SegmentMeta>> {
    anyhow::ensure!(doc_cap > 0, "segment document cap must be greater than 0");
    anyhow::ensure!(!docs.is_empty(), "refusing to build an empty segment shard");
    let corpus = make_corpus(docs)?;
    let built = build_segment_files(corpus.as_ref(), strategy, docs)?;
    if built.tables.documents.len() <= doc_cap {
        let meta = put_segment_files(store, &built.fst, &built.postings, &built.tables)?;
        return Ok(vec![meta]);
    }
    anyhow::ensure!(
        docs.len() > 1,
        "source {} expands to {} documents, exceeding the segment cap of {doc_cap}",
        docs[0].0,
        built.tables.documents.len()
    );
    drop(built);
    let split = docs.len() / 2;
    let mut segments =
        write_bounded_segments(store, strategy, &docs[..split], doc_cap, make_corpus)?;
    segments.extend(write_bounded_segments(
        store,
        strategy,
        &docs[split..],
        doc_cap,
        make_corpus,
    )?);
    Ok(segments)
}

fn put_segment_files(
    store: &dyn BlobStore,
    fst: &crate::TempBlob,
    postings: &crate::TempBlob,
    tables: &SegmentTables,
) -> Result<SegmentMeta> {
    anyhow::ensure!(
        !tables.sources.is_empty(),
        "refusing to write a segment without sources"
    );
    tables.validate()?;
    let docs_bytes = postcard::to_allocvec(tables)?;
    let terms_fst_hash = fst.hash().to_owned();
    let postings_hash = postings.hash().to_owned();
    let docs_hash = sha256_hex(&[&docs_bytes]);
    let seg_id = sha256_hex(&[
        terms_fst_hash.as_bytes(),
        postings_hash.as_bytes(),
        docs_hash.as_bytes(),
    ]);
    store.put_file(&segment_blob(&seg_id, "terms.fst"), fst.path())?;
    store.put_file(&segment_blob(&seg_id, "postings.bin"), postings.path())?;
    store.put(&segment_blob(&seg_id, "docs.bin"), &docs_bytes)?;
    let meta = SegmentMeta {
        seg_id,
        doc_count: u32::try_from(tables.documents.len())?,
        terms_fst_len: fst.len(),
        terms_fst_hash,
        postings_len: postings.len(),
        postings_hash,
        docs_len: docs_bytes.len() as u64,
        docs_hash,
        min_key: tables.sources[0].key.clone(),
        max_key: tables.sources[tables.sources.len() - 1].key.clone(),
        dead_hash: String::new(),
        dead_len: 0,
    };
    Ok(meta)
}

/// Content-address and PUT a segment's three blobs. Shared by fresh builds
/// and compaction merges.
fn put_segment_blobs(
    store: &dyn BlobStore,
    fst_bytes: &[u8],
    postings_buf: &[u8],
    tables: &SegmentTables,
) -> Result<SegmentMeta> {
    anyhow::ensure!(
        !tables.sources.is_empty(),
        "refusing to write a segment without sources"
    );
    tables.validate()?;
    let docs_bytes = postcard::to_allocvec(tables)?;
    let terms_fst_hash = sha256_hex(&[fst_bytes]);
    let postings_hash = sha256_hex(&[postings_buf]);
    let docs_hash = sha256_hex(&[&docs_bytes]);
    let seg_id = sha256_hex(&[
        terms_fst_hash.as_bytes(),
        postings_hash.as_bytes(),
        docs_hash.as_bytes(),
    ]);
    store.put(&segment_blob(&seg_id, "terms.fst"), fst_bytes)?;
    store.put(&segment_blob(&seg_id, "postings.bin"), postings_buf)?;
    store.put(&segment_blob(&seg_id, "docs.bin"), &docs_bytes)?;
    let meta = SegmentMeta {
        seg_id,
        doc_count: u32::try_from(tables.documents.len())?,
        terms_fst_len: fst_bytes.len() as u64,
        terms_fst_hash,
        postings_len: postings_buf.len() as u64,
        postings_hash,
        docs_len: docs_bytes.len() as u64,
        docs_hash,
        min_key: tables.sources[0].key.clone(),
        max_key: tables.sources[tables.sources.len() - 1].key.clone(),
        dead_hash: String::new(),
        dead_len: 0,
    };
    Ok(meta)
}

/// At most one merge per run: the two smallest ADJACENT segments whose
/// combined size fits the caps. Compaction exists only to bound segment
/// count — dead ids in large segments cost almost nothing at search time.
fn maybe_compact(
    store: &dyn BlobStore,
    cache_dir: &Path,
    segments: &mut Vec<(SegmentMeta, DeadSet)>,
) -> Result<bool> {
    if segments.len() <= SEGMENT_COUNT_TARGET {
        return Ok(false);
    }
    let live =
        |entry: &(SegmentMeta, DeadSet)| entry.0.doc_count as usize - entry.1.documents.len();
    let Some(victim) = (0..segments.len() - 1)
        .filter(|&i| {
            segments[i]
                .0
                .postings_len
                .saturating_add(segments[i + 1].0.postings_len)
                <= MERGE_POSTINGS_CAP
                && live(&segments[i]).saturating_add(live(&segments[i + 1])) <= SEGMENT_DOC_CAP
        })
        .min_by_key(|&i| live(&segments[i]).saturating_add(live(&segments[i + 1])))
    else {
        return Ok(false);
    };
    let (first_meta, first_dead) = segments[victim].clone();
    let (second_meta, second_dead) = segments[victim + 1].clone();
    let merged = merge_segments(
        store,
        cache_dir,
        &[(first_meta, first_dead), (second_meta, second_dead)],
    )?;
    segments.splice(victim..=victim + 1, [(merged, DeadSet::default())]);
    Ok(true)
}

/// Merge segments WITHOUT refetching any objects: decode every gram's
/// posting list, drop dead ids, remap survivors into one combined table.
fn merge_segments(
    store: &dyn BlobStore,
    cache_dir: &Path,
    victims: &[(SegmentMeta, DeadSet)],
) -> Result<SegmentMeta> {
    type MergedSource = (SourceEntry, Vec<(DocEntry, u32)>, usize);

    let mut tables = SegmentTables {
        sources: Vec::new(),
        documents: Vec::new(),
    };
    let mut remaps: Vec<Vec<Option<u32>>> = Vec::with_capacity(victims.len());
    let mut entries: Vec<MergedSource> = Vec::new();
    for (seg_idx, (meta, dead)) in victims.iter().enumerate() {
        let victim_tables = parse_tables(&cached_blob(
            store,
            cache_dir,
            &meta.seg_id,
            "docs.bin",
            meta.docs_len,
            &meta.docs_hash,
        )?)?;
        remaps.push(vec![None; victim_tables.documents.len()]);
        for (source_id, source) in victim_tables.sources.into_iter().enumerate() {
            if dead.sources.binary_search(&(source_id as u32)).is_ok() {
                continue;
            }
            let start = source.first_doc as usize;
            let end = start + source.doc_count as usize;
            let documents = victim_tables.documents[start..end]
                .iter()
                .cloned()
                .enumerate()
                .filter_map(|(offset, document)| {
                    let old_id = u32::try_from(start + offset).ok()?;
                    dead.documents
                        .binary_search(&old_id)
                        .is_err()
                        .then_some((document, old_id))
                })
                .collect();
            entries.push((source, documents, seg_idx));
        }
    }
    entries.sort_unstable_by(|(left, _, _), (right, _, _)| left.key.cmp(&right.key));
    for (mut source, documents, seg_idx) in entries {
        let source_id = u32::try_from(tables.sources.len())?;
        source.first_doc = u32::try_from(tables.documents.len())?;
        source.doc_count = u32::try_from(documents.len())?;
        for (mut document, old_id) in documents {
            let new_id = u32::try_from(tables.documents.len())?;
            remaps[seg_idx][old_id as usize] = Some(new_id);
            document.source_id = source_id;
            tables.documents.push(document);
        }
        tables.sources.push(source);
    }

    let mut postings: std::collections::BTreeMap<Vec<u8>, Vec<DocId>> =
        std::collections::BTreeMap::new();
    for (seg_idx, (meta, _)) in victims.iter().enumerate() {
        let fst_bytes = cached_blob(
            store,
            cache_dir,
            &meta.seg_id,
            "terms.fst",
            meta.terms_fst_len,
            &meta.terms_fst_hash,
        )?;
        let postings_bytes = store
            .get(&segment_blob(&meta.seg_id, "postings.bin"))?
            .with_context(|| format!("postings.bin of {} missing from the store", meta.seg_id))?;
        anyhow::ensure!(
            postings_bytes.len() as u64 == meta.postings_len,
            "postings.bin of {} is {} bytes, expected {}",
            meta.seg_id,
            postings_bytes.len(),
            meta.postings_len
        );
        anyhow::ensure!(
            sha256_hex(&[&postings_bytes]) == meta.postings_hash,
            "postings.bin of {} failed its SHA-256 check",
            meta.seg_id
        );
        let map = fst::Map::new(fst_bytes)?;
        let mut stream = map.stream();
        while let Some((gram, packed)) = fst::Streamer::next(&mut stream) {
            let (offset, count) = crate::eval::unpack_posting(packed);
            let start = usize::try_from(offset)?;
            let end = start + usize::try_from(crate::posting_block_len(count, meta.doc_count))?;
            let block = postings_bytes
                .get(start..end)
                .context("truncated postings.bin during merge")?;
            let ids = crate::decode_posting_block(block, count, meta.doc_count)?;
            let remap = &remaps[seg_idx];
            postings
                .entry(gram.to_vec())
                .or_default()
                .extend(ids.into_iter().filter_map(|id| remap[id as usize]));
        }
    }
    tables.validate()?;
    let (fst_bytes, postings_buf) =
        crate::serialize_postings(postings, u32::try_from(tables.documents.len())?)?;
    put_segment_blobs(store, &fst_bytes, &postings_buf, &tables)
}

struct Segment {
    meta: SegmentMeta,
    map: fst::Map<Vec<u8>>,
    dead: DeadSet,
    tables: OnceLock<SegmentTables>,
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
}

impl SegmentedReader {
    pub fn open(store: Box<dyn BlobStore>, cache_dir: &Path) -> Result<SegmentedReader> {
        let (bytes, root_version) = store
            .get_versioned("segments.bin")
            .context("reading segments.bin")?
            .context("no index found — run `holys3 index` first")?;
        let list = parse_segment_list(&bytes)?;
        let mut segments = Vec::with_capacity(list.segments.len());
        for meta in list.segments {
            // A corrupt cached blob (same length, damaged bytes) self-heals:
            // wipe this segment's cache and refetch once.
            let segment = match load_segment(store.as_ref(), cache_dir, &meta) {
                Ok(segment) => segment,
                Err(_) => {
                    std::fs::remove_dir_all(cache_dir.join(&meta.seg_id)).ok();
                    load_segment(store.as_ref(), cache_dir, &meta)?
                }
            };
            segments.push(segment);
        }
        evict_stale_segments(cache_dir, &segments);
        Ok(SegmentedReader {
            store,
            cache_dir: cache_dir.to_path_buf(),
            root_version,
            strategy: list.strategy,
            segments,
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
        for segment in &self.segments {
            if let Some(prefix) = source_prefix {
                self.classify_index_result(self.segment_tables(segment))?;
                if !Self::prefix_overlaps(&segment.meta, prefix) {
                    continue;
                }
            }
            let postings_name = segment_blob(&segment.meta.seg_id, "postings.bin");
            let ids = self.classify_index_result(candidates_with(
                &segment.map,
                segment.meta.doc_count,
                q,
                |needed| {
                    let doc_count = segment.meta.doc_count;
                    let ranges = needed
                        .iter()
                        .map(|(&offset, &count)| {
                            (offset, crate::posting_block_len(count, doc_count))
                        })
                        .collect::<Vec<_>>();
                    let blocks = self.store.get_ranges(&postings_name, &ranges)?;
                    anyhow::ensure!(
                        blocks.len() == ranges.len(),
                        "get_ranges returned {} blocks for {} ranges",
                        blocks.len(),
                        ranges.len()
                    );
                    needed
                        .iter()
                        .zip(blocks)
                        .map(|((&offset, &count), bytes)| {
                            Ok((
                                offset,
                                crate::decode_posting_block(&bytes, count, doc_count)?,
                            ))
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
            let mut batch_source = None;
            for id in live {
                let document = &tables.documents[id as usize];
                if batch.len() >= batch_size && batch_source != Some(document.source_id) {
                    if !visit(std::mem::take(&mut batch))? {
                        return Ok(());
                    }
                    batch.reserve(capacity);
                }
                batch_source = Some(document.source_id);
                let source = &tables.sources[document.source_id as usize];
                batch.push(DocAddress {
                    display_key: document.display_key.clone(),
                    source_key: source.key.clone(),
                    source_version: source.version.clone(),
                    encoded_size: source.encoded_size,
                    encoding: source.encoding,
                    member_path: document.member_path.clone(),
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

fn validate_term_map(meta: &SegmentMeta, map: &fst::Map<Vec<u8>>) -> Result<()> {
    let mut expected_offset = 0u64;
    let mut stream = map.stream();
    while let Some((_, packed)) = fst::Streamer::next(&mut stream) {
        let (offset, count) = crate::eval::unpack_posting(packed);
        anyhow::ensure!(count > 0, "term map contains an empty posting list");
        anyhow::ensure!(
            count <= meta.doc_count,
            "term map posting count exceeds its segment document count"
        );
        anyhow::ensure!(
            offset == expected_offset,
            "term map posting offsets are not contiguous"
        );
        expected_offset = expected_offset
            .checked_add(crate::posting_block_len(count, meta.doc_count))
            .context("term map posting length overflows")?;
        anyhow::ensure!(
            expected_offset <= meta.postings_len,
            "term map posting extends beyond postings.bin"
        );
    }
    anyhow::ensure!(
        expected_offset == meta.postings_len,
        "term map does not account for all of postings.bin"
    );
    Ok(())
}

fn load_segment(store: &dyn BlobStore, cache_dir: &Path, meta: &SegmentMeta) -> Result<Segment> {
    let fst_bytes = cached_blob(
        store,
        cache_dir,
        &meta.seg_id,
        "terms.fst",
        meta.terms_fst_len,
        &meta.terms_fst_hash,
    )?;
    let dead = load_dead(store, cache_dir, meta)?;
    let map = fst::Map::new(fst_bytes)?;
    validate_term_map(meta, &map)?;
    Ok(Segment {
        map,
        dead,
        tables: OnceLock::new(),
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

#[cfg(test)]
mod tests {
    use super::*;

    fn segment() -> SegmentMeta {
        SegmentMeta {
            seg_id: "a".repeat(64),
            doc_count: 1,
            terms_fst_len: 1,
            terms_fst_hash: "b".repeat(64),
            postings_len: 1,
            postings_hash: "c".repeat(64),
            docs_len: 1,
            docs_hash: "d".repeat(64),
            min_key: "a".into(),
            max_key: "z".into(),
            dead_hash: String::new(),
            dead_len: 0,
        }
    }

    fn encoded(segments: Vec<SegmentMeta>) -> Vec<u8> {
        postcard::to_allocvec(&SegmentList {
            format: INDEX_FORMAT,
            strategy: Strategy::Trigram,
            segments,
        })
        .unwrap()
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
    fn segment_tables_reject_mismatched_key_bounds() {
        let tables = SegmentTables {
            sources: vec![SourceEntry {
                key: "actual".into(),
                version: "v1".into(),
                encoded_size: 1,
                encoding: holys3_core::SourceEncoding::Raw,
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
        let store = holys3_core::LocalBlobStore::new(store_dir.path());
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
    }

    #[test]
    fn term_map_rejects_impossible_posting_metadata() {
        let mut builder = fst::MapBuilder::memory();
        builder
            .insert(b"abc", crate::eval::pack_posting(0, 2).unwrap())
            .unwrap();
        let map = fst::Map::new(builder.into_inner().unwrap()).unwrap();
        assert!(validate_term_map(&segment(), &map).is_err());
    }

    #[test]
    fn unmergeable_segment_set_converges_without_root_rewrite() {
        let store_dir = tempfile::tempdir().unwrap();
        let cache_dir = tempfile::tempdir().unwrap();
        let store = holys3_core::LocalBlobStore::new(store_dir.path());
        let mut segments = Vec::new();
        let mut listing = Vec::new();
        for index in 0..=SEGMENT_COUNT_TARGET {
            let key = format!("doc-{index}");
            let tables = SegmentTables {
                sources: vec![SourceEntry {
                    key: key.clone(),
                    version: "v1".into(),
                    encoded_size: 1,
                    encoding: holys3_core::SourceEncoding::Raw,
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
                }],
            };
            let mut meta = put_segment_blobs(&store, &[], &[], &tables).unwrap();
            meta.postings_len = MERGE_POSTINGS_CAP + 1;
            segments.push(meta);
            listing.push((key, "v1".to_owned(), 1));
        }
        let root = postcard::to_allocvec(&SegmentList {
            format: INDEX_FORMAT,
            strategy: Strategy::Trigram,
            segments,
        })
        .unwrap();
        store.put("segments.bin", &root).unwrap();
        let before = store.get_versioned("segments.bin").unwrap().unwrap().1;
        let report = update_index(
            &store,
            cache_dir.path(),
            Strategy::Trigram,
            &listing,
            false,
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
        let store = holys3_core::LocalBlobStore::new(store_dir.path());
        let mut segments = (0..=SEGMENT_COUNT_TARGET)
            .map(|_| {
                let mut meta = segment();
                meta.postings_len = u64::MAX;
                (meta, DeadSet::default())
            })
            .collect();
        assert!(!maybe_compact(&store, cache_dir.path(), &mut segments).unwrap());
    }

    #[test]
    fn segment_build_splits_on_logical_document_count() {
        let store_dir = tempfile::tempdir().unwrap();
        let store = holys3_core::LocalBlobStore::new(store_dir.path());
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
            Ok(Box::new(holys3_core::testutil::MemCorpus::new(
                keys, bodies,
            )))
        };
        let segments =
            write_bounded_segments(&store, Strategy::Trigram, &docs, 2, &factory).unwrap();
        assert_eq!(
            segments
                .iter()
                .map(|segment| segment.doc_count)
                .collect::<Vec<_>>(),
            vec![2, 1, 2]
        );
        assert!(segments.iter().all(|segment| segment.doc_count <= 2));
    }
}
