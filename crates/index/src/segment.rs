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
//! against the union of segment doc tables, build ONE new segment over the
//! changes, tombstone superseded entries, and atomically swap segments.bin.

use crate::{candidates_with, INDEX_FORMAT};
use anyhow::{Context, Result};
use holys3_core::{BlobStore, Corpus, DocId, Strategy};
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

/// Etag recorded for docs that contributed no grams (vanished mid-build or
/// undecodable): never matches a real listing etag, so the next run retries.
const TOMBSTONE_ETAG: &str = "";

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct SegmentMeta {
    pub seg_id: String,
    pub doc_count: u32,
    pub terms_fst_len: u64,
    pub postings_len: u64,
    pub docs_len: u64,
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
    Ok(list)
}

fn load_segment_list(store: &dyn BlobStore) -> Result<Option<SegmentList>> {
    match store.get("segments.bin") {
        Ok(bytes) => Ok(Some(parse_segment_list(&bytes)?)),
        Err(_) => Ok(None),
    }
}

type DocsTable = Vec<(String, String)>;

fn parse_docs(bytes: &[u8]) -> Result<DocsTable> {
    postcard::from_bytes(bytes).context("segment docs.bin unreadable")
}

fn parse_dead(bytes: &[u8]) -> Result<Vec<u32>> {
    postcard::from_bytes(bytes).context("segment dead set unreadable")
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
) -> Result<Vec<u8>> {
    let cache_path = cache_dir.join(seg_id).join(name);
    if let Ok(bytes) = std::fs::read(&cache_path) {
        if bytes.len() as u64 == expected_len {
            return Ok(bytes);
        }
    }
    let bytes = store.get(&segment_blob(seg_id, name))?;
    anyhow::ensure!(
        bytes.len() as u64 == expected_len,
        "segment blob {name} of {seg_id} is {} bytes, expected {expected_len}",
        bytes.len()
    );
    if let Some(parent) = cache_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp = cache_path.with_file_name(format!("{name}.tmp.{}", std::process::id()));
    std::fs::write(&tmp, &bytes)?;
    std::fs::rename(&tmp, &cache_path)?;
    Ok(bytes)
}

/// What an index run did; everything the CLI needs to report.
pub struct UpdateReport {
    pub added: usize,
    pub removed: usize,
    pub total_docs: usize,
    pub segments: usize,
    pub compacted: bool,
    pub up_to_date: bool,
}

/// Builds a fetchable corpus over the given keys (ids = positions).
pub type CorpusFactory<'a> = dyn Fn(&[String]) -> Result<Box<dyn Corpus>> + 'a;

/// Incrementally update the segmented index to match `listing`
/// ((key, etag) pairs). `make_corpus` builds a fetchable corpus over a given
/// subset of keys, with ids equal to positions in the slice.
pub fn update_index(
    store: &dyn BlobStore,
    cache_dir: &Path,
    strategy: Strategy,
    listing: &[(String, String)],
    make_corpus: &CorpusFactory<'_>,
) -> Result<UpdateReport> {
    let existing = match load_segment_list(store) {
        Ok(Some(list)) if list.strategy == strategy => list.segments,
        Ok(Some(_)) => {
            eprintln!("note: index strategy changed; rebuilding from scratch");
            Vec::new()
        }
        Ok(None) => Vec::new(),
        Err(err) => {
            eprintln!("note: {err:#}; rebuilding from scratch");
            Vec::new()
        }
    };

    // Newest entry per key wins; dead ids are already gone from `live`.
    let mut tables: Vec<DocsTable> = Vec::with_capacity(existing.len());
    let mut dead_sets: Vec<Vec<u32>> = Vec::with_capacity(existing.len());
    for meta in &existing {
        tables.push(parse_docs(&cached_blob(
            store,
            cache_dir,
            &meta.seg_id,
            "docs.bin",
            meta.docs_len,
        )?)?);
        dead_sets.push(load_dead(store, cache_dir, meta)?);
    }
    let mut live: HashMap<&str, (usize, u32, &str)> = HashMap::new();
    for (seg_idx, (table, dead)) in tables.iter().zip(&dead_sets).enumerate() {
        for (local_id, (key, etag)) in table.iter().enumerate() {
            let local_id = local_id as u32;
            if dead.binary_search(&local_id).is_ok() {
                continue;
            }
            // Later segments overwrite earlier entries for the same key.
            live.insert(key.as_str(), (seg_idx, local_id, etag.as_str()));
        }
    }

    let mut to_add: Vec<(String, String)> = listing
        .iter()
        .filter(|(key, etag)| {
            live.get(key.as_str())
                .is_none_or(|(_, _, indexed_etag)| *indexed_etag != etag.as_str())
        })
        .cloned()
        .collect();
    to_add.sort_unstable();
    let listed: HashMap<&str, &str> = listing
        .iter()
        .map(|(key, etag)| (key.as_str(), etag.as_str()))
        .collect();
    let mut newly_dead: Vec<(usize, u32)> = live
        .iter()
        .filter(|(key, (_, _, etag))| listed.get(*key) != Some(etag))
        .map(|(_, &(seg_idx, local_id, _))| (seg_idx, local_id))
        .collect();
    newly_dead.sort_unstable();

    if to_add.is_empty() && newly_dead.is_empty() {
        return Ok(UpdateReport {
            added: 0,
            removed: 0,
            total_docs: live.len(),
            segments: existing.len(),
            compacted: false,
            up_to_date: true,
        });
    }
    let added = to_add.len();
    let removed = newly_dead.len();

    // Fold new tombstones into per-segment dead sets, then drop fully-dead
    // segments (their blobs stay as orphans for racing readers).
    let mut metas = existing;
    for (seg_idx, group) in &newly_dead.iter().copied().fold(
        Vec::<(usize, Vec<u32>)>::new(),
        |mut groups, (seg_idx, id)| {
            match groups.last_mut() {
                Some((last_idx, ids)) if *last_idx == seg_idx => ids.push(id),
                _ => groups.push((seg_idx, vec![id])),
            }
            groups
        },
    ) {
        let mut dead = dead_sets[*seg_idx].clone();
        dead.extend(group);
        dead.sort_unstable();
        dead.dedup();
        write_dead(store, &mut metas[*seg_idx], &dead)?;
        dead_sets[*seg_idx] = dead;
    }
    let mut keep: Vec<(SegmentMeta, Vec<u32>)> = metas
        .into_iter()
        .zip(dead_sets)
        .filter(|(meta, dead)| (dead.len() as u32) < meta.doc_count)
        .collect();

    // Build the new segment(s) over the changes, capped.
    for shard in to_add.chunks(SEGMENT_DOC_CAP) {
        let keys: Vec<String> = shard.iter().map(|(key, _)| key.clone()).collect();
        let corpus = make_corpus(&keys)?;
        let (meta, _) = write_segment(store, corpus.as_ref(), strategy, shard)?;
        keep.push((meta, Vec::new()));
    }

    let compacted = maybe_compact(store, cache_dir, &mut keep)?;

    let total_docs = keep
        .iter()
        .map(|(meta, dead)| meta.doc_count as usize - dead.len())
        .sum();
    let segments: Vec<SegmentMeta> = keep.into_iter().map(|(meta, _)| meta).collect();
    let count = segments.len();
    let list = SegmentList {
        format: INDEX_FORMAT,
        strategy,
        segments,
    };
    store.put("segments.bin", &postcard::to_allocvec(&list)?)?;
    Ok(UpdateReport {
        added,
        removed,
        total_docs,
        segments: count,
        compacted,
        up_to_date: false,
    })
}

fn load_dead(store: &dyn BlobStore, cache_dir: &Path, meta: &SegmentMeta) -> Result<Vec<u32>> {
    if meta.dead_hash.is_empty() {
        return Ok(Vec::new());
    }
    parse_dead(&cached_blob(
        store,
        cache_dir,
        &meta.seg_id,
        &format!("dead-{}.bin", meta.dead_hash),
        meta.dead_len,
    )?)
}

fn write_dead(store: &dyn BlobStore, meta: &mut SegmentMeta, dead: &[u32]) -> Result<()> {
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

/// Build and PUT one segment over `docs` ((key, listing-etag) pairs, sorted
/// by key; corpus ids = positions). Returns its meta and the doc table.
fn write_segment(
    store: &dyn BlobStore,
    corpus: &dyn Corpus,
    strategy: Strategy,
    docs: &[(String, String)],
) -> Result<(SegmentMeta, DocsTable)> {
    let (fst_bytes, postings_buf, ungrammed) = crate::build_index_bytes(corpus, strategy)?;
    let mut table: DocsTable = docs.to_vec();
    for id in &ungrammed {
        table[*id as usize].1 = TOMBSTONE_ETAG.to_owned();
    }
    put_segment_blobs(store, &fst_bytes, &postings_buf, &table)
}

/// Content-address and PUT a segment's three blobs. Shared by fresh builds
/// and compaction merges.
fn put_segment_blobs(
    store: &dyn BlobStore,
    fst_bytes: &[u8],
    postings_buf: &[u8],
    table: &DocsTable,
) -> Result<(SegmentMeta, DocsTable)> {
    anyhow::ensure!(!table.is_empty(), "refusing to write an empty segment");
    let docs_bytes = postcard::to_allocvec(table)?;
    let seg_id = sha256_hex(&[fst_bytes, postings_buf, &docs_bytes]);
    store.put(&segment_blob(&seg_id, "terms.fst"), fst_bytes)?;
    store.put(&segment_blob(&seg_id, "postings.bin"), postings_buf)?;
    store.put(&segment_blob(&seg_id, "docs.bin"), &docs_bytes)?;
    let meta = SegmentMeta {
        seg_id,
        doc_count: u32::try_from(table.len())?,
        terms_fst_len: fst_bytes.len() as u64,
        postings_len: postings_buf.len() as u64,
        docs_len: docs_bytes.len() as u64,
        min_key: table[0].0.clone(),
        max_key: table[table.len() - 1].0.clone(),
        dead_hash: String::new(),
        dead_len: 0,
    };
    Ok((meta, table.clone()))
}

/// At most one merge per run: the two smallest ADJACENT segments whose
/// combined size fits the caps. Compaction exists only to bound segment
/// count — dead ids in large segments cost almost nothing at search time.
fn maybe_compact(
    store: &dyn BlobStore,
    cache_dir: &Path,
    segments: &mut Vec<(SegmentMeta, Vec<u32>)>,
) -> Result<bool> {
    if segments.len() <= SEGMENT_COUNT_TARGET {
        return Ok(false);
    }
    let live = |entry: &(SegmentMeta, Vec<u32>)| entry.0.doc_count as usize - entry.1.len();
    let Some(victim) = (0..segments.len() - 1)
        .filter(|&i| {
            segments[i].0.postings_len + segments[i + 1].0.postings_len <= MERGE_POSTINGS_CAP
                && live(&segments[i]) + live(&segments[i + 1]) <= SEGMENT_DOC_CAP
        })
        .min_by_key(|&i| live(&segments[i]) + live(&segments[i + 1]))
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
    segments.splice(victim..=victim + 1, [(merged, Vec::new())]);
    Ok(true)
}

/// Merge segments WITHOUT refetching any objects: decode every gram's
/// posting list, drop dead ids, remap survivors into one combined table.
fn merge_segments(
    store: &dyn BlobStore,
    cache_dir: &Path,
    victims: &[(SegmentMeta, Vec<u32>)],
) -> Result<SegmentMeta> {
    let mut table: DocsTable = Vec::new();
    let mut remaps: Vec<Vec<Option<u32>>> = Vec::with_capacity(victims.len());
    let mut entries: Vec<(String, String, usize, u32)> = Vec::new();
    for (seg_idx, (meta, dead)) in victims.iter().enumerate() {
        let docs = parse_docs(&cached_blob(
            store,
            cache_dir,
            &meta.seg_id,
            "docs.bin",
            meta.docs_len,
        )?)?;
        remaps.push(vec![None; docs.len()]);
        for (local_id, (key, etag)) in docs.into_iter().enumerate() {
            if dead.binary_search(&(local_id as u32)).is_err() {
                entries.push((key, etag, seg_idx, local_id as u32));
            }
        }
    }
    entries.sort_unstable();
    for (new_id, (key, etag, seg_idx, old_id)) in entries.into_iter().enumerate() {
        remaps[seg_idx][old_id as usize] = Some(new_id as u32);
        table.push((key, etag));
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
        )?;
        let postings_bytes = store.get(&segment_blob(&meta.seg_id, "postings.bin"))?;
        anyhow::ensure!(
            postings_bytes.len() as u64 == meta.postings_len,
            "postings.bin of {} is {} bytes, expected {}",
            meta.seg_id,
            postings_bytes.len(),
            meta.postings_len
        );
        let map = fst::Map::new(fst_bytes)?;
        let mut stream = map.stream();
        while let Some((gram, packed)) = fst::Streamer::next(&mut stream) {
            let (offset, count) = crate::eval::unpack_posting(packed);
            let start = usize::try_from(offset)?;
            let end = start + usize::try_from(count)? * 4;
            let block = postings_bytes
                .get(start..end)
                .context("truncated postings.bin during merge")?;
            let ids = crate::decode_ids(block, count)?;
            let remap = &remaps[seg_idx];
            postings
                .entry(gram.to_vec())
                .or_default()
                .extend(ids.into_iter().filter_map(|id| remap[id as usize]));
        }
    }
    let mut postings_buf: Vec<u8> = Vec::new();
    let mut builder = fst::MapBuilder::new(Vec::new())?;
    for (gram, mut ids) in postings {
        ids.sort_unstable();
        ids.dedup();
        let offset = postings_buf.len() as u64;
        for id in &ids {
            postings_buf.extend_from_slice(&id.to_le_bytes());
        }
        builder.insert(gram, crate::eval::pack_posting(offset, ids.len())?)?;
    }
    let (meta, _) = put_segment_blobs(store, &builder.into_inner()?, &postings_buf, &table)?;
    Ok(meta)
}

struct Segment {
    meta: SegmentMeta,
    map: fst::Map<Vec<u8>>,
    dead: Vec<u32>,
    docs: OnceLock<DocsTable>,
}

/// Reader over a segmented index: per-segment candidate resolution with the
/// existing batched ranged-GET machinery; doc tables load lazily, only for
/// segments that actually produce candidates.
pub struct SegmentedReader {
    store: Box<dyn BlobStore>,
    cache_dir: PathBuf,
    strategy: Strategy,
    segments: Vec<Segment>,
}

impl SegmentedReader {
    pub fn open(store: Box<dyn BlobStore>, cache_dir: &Path) -> Result<SegmentedReader> {
        let bytes = store
            .get("segments.bin")
            .context("no index found — run `holys3 index` first")?;
        let list = parse_segment_list(&bytes)?;
        let mut segments = Vec::with_capacity(list.segments.len());
        for meta in list.segments {
            let fst_bytes = cached_blob(
                store.as_ref(),
                cache_dir,
                &meta.seg_id,
                "terms.fst",
                meta.terms_fst_len,
            )?;
            let dead = load_dead(store.as_ref(), cache_dir, &meta)?;
            segments.push(Segment {
                map: fst::Map::new(fst_bytes)?,
                dead,
                docs: OnceLock::new(),
                meta,
            });
        }
        evict_stale_segments(cache_dir, &segments);
        Ok(SegmentedReader {
            store,
            cache_dir: cache_dir.to_path_buf(),
            strategy: list.strategy,
            segments,
        })
    }

    fn segment_docs<'a>(&self, segment: &'a Segment) -> Result<&'a DocsTable> {
        if let Some(docs) = segment.docs.get() {
            return Ok(docs);
        }
        let loaded = parse_docs(&cached_blob(
            self.store.as_ref(),
            &self.cache_dir,
            &segment.meta.seg_id,
            "docs.bin",
            segment.meta.docs_len,
        )?)?;
        Ok(segment.docs.get_or_init(|| loaded))
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
            .map(|segment| segment.meta.doc_count as usize - segment.dead.len())
            .sum()
    }

    fn candidate_keys(&self, q: &Query, key_prefix: Option<&str>) -> Result<Vec<String>> {
        let mut keys = Vec::new();
        for segment in &self.segments {
            if let Some(prefix) = key_prefix {
                if !Self::prefix_overlaps(&segment.meta, prefix) {
                    continue;
                }
            }
            let postings_name = segment_blob(&segment.meta.seg_id, "postings.bin");
            let ids = candidates_with(&segment.map, segment.meta.doc_count, q, |needed| {
                let ranges = needed
                    .iter()
                    .map(|(&offset, &count)| (offset, u64::from(count) * 4))
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
                        Ok((offset, crate::decode_ids(&bytes, count)?))
                    })
                    .collect()
            })?;
            let live: Vec<DocId> = ids
                .into_iter()
                .filter(|id| segment.dead.binary_search(id).is_err())
                .collect();
            if live.is_empty() {
                continue;
            }
            let docs = self.segment_docs(segment)?;
            keys.extend(live.into_iter().filter_map(|id| {
                let (key, etag) = &docs[id as usize];
                if etag == TOMBSTONE_ETAG {
                    return None;
                }
                if key_prefix.is_some_and(|prefix| !key.starts_with(prefix)) {
                    return None;
                }
                Some(key.clone())
            }));
        }
        Ok(keys)
    }

    fn stats(&self) -> crate::IndexStats {
        crate::IndexStats {
            distinct_grams: self.segments.iter().map(|s| s.map.len() as u64).sum(),
            terms_fst_bytes: self.segments.iter().map(|s| s.meta.terms_fst_len).sum(),
            postings_bytes: self.segments.iter().map(|s| s.meta.postings_len).sum(),
        }
    }
}
