use super::cache::{cached_blob, cached_file, map_file};
use super::{
    put_segment_files, validate_segment_tables, SegmentMeta, MERGE_DOCS_CAP, MERGE_POSTINGS_CAP,
    MERGE_TERMS_CAP, SEGMENT_COUNT_TARGET, SEGMENT_DOC_CAP,
};
use crate::format::{DeadSet, DocEntry, SegmentTables, SourceEntry};
use anyhow::{Context, Result};
use holys3_core::{BlobStore, DocId, Strategy};
use std::io::{BufWriter, Write};
use std::path::Path;

pub(super) fn maybe_compact(
    store: &dyn BlobStore,
    cache_dir: &Path,
    strategy: Strategy,
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
                && segments[i]
                    .0
                    .terms_fst_len
                    .saturating_add(segments[i + 1].0.terms_fst_len)
                    <= MERGE_TERMS_CAP
                && segments[i]
                    .0
                    .docs_len
                    .saturating_add(segments[i + 1].0.docs_len)
                    <= MERGE_DOCS_CAP
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
        strategy,
        &[(first_meta, first_dead), (second_meta, second_dead)],
    )?;
    segments.splice(victim..=victim + 1, [(merged, DeadSet::default())]);
    Ok(true)
}

fn write_compaction_run(
    store: &dyn BlobStore,
    cache_dir: &Path,
    strategy: Strategy,
    meta: &SegmentMeta,
    remap: &[Option<DocId>],
) -> Result<tempfile::TempPath> {
    let terms_path = cached_file(
        store,
        cache_dir,
        &meta.seg_id,
        "terms.fst",
        meta.terms_fst_len,
        &meta.terms_fst_hash,
    )?;
    let postings_path = cached_file(
        store,
        cache_dir,
        &meta.seg_id,
        "postings.bin",
        meta.postings_len,
        &meta.postings_hash,
    )?;
    let terms = map_file(&terms_path)?;
    let postings = map_file(&postings_path)?;
    #[cfg(unix)]
    {
        terms.advise(memmap2::Advice::Sequential)?;
        postings.advise(memmap2::Advice::Sequential)?;
    }
    let map = fst::Map::new(terms)?;
    let mut stream = map.stream();
    let mut file = tempfile::NamedTempFile::new()?;
    let mut writer = BufWriter::new(file.as_file_mut());
    while let Some((gram, packed)) = fst::Streamer::next(&mut stream) {
        let (offset, count) = crate::eval::unpack_posting(packed);
        anyhow::ensure!(count > 0, "term map contains an empty posting list");
        anyhow::ensure!(
            count <= meta.doc_count,
            "term map posting count exceeds its segment document count"
        );
        let len = crate::posting_block_len(count, meta.doc_count);
        let end = offset
            .checked_add(len)
            .context("term map posting length overflows")?;
        anyhow::ensure!(
            end <= meta.postings_len,
            "term map posting extends beyond postings.bin"
        );
        let block = postings
            .get(usize::try_from(offset)?..usize::try_from(end)?)
            .context("truncated postings.bin during merge")?;
        let mut ids = Vec::new();
        for id in crate::decode_posting_block(block, count, meta.doc_count)? {
            if let Some(id) = remap
                .get(usize::try_from(id)?)
                .context("posting document ID is out of bounds")?
            {
                ids.push(*id);
            }
        }
        ids.sort_unstable();
        ids.dedup();
        for id in ids {
            crate::build::write_posting_record(&mut writer, strategy, gram, id)?;
        }
    }
    writer.flush()?;
    drop(writer);
    Ok(file.into_temp_path())
}

fn merge_segments(
    store: &dyn BlobStore,
    cache_dir: &Path,
    strategy: Strategy,
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
        let victim_tables = crate::format::parse_tables(&cached_blob(
            store,
            cache_dir,
            &meta.seg_id,
            "docs.bin",
            meta.docs_len,
            &meta.docs_hash,
        )?)?;
        validate_segment_tables(meta, &victim_tables)?;
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

    tables.validate()?;
    let runs = victims
        .iter()
        .zip(&remaps)
        .map(|((meta, _), remap)| write_compaction_run(store, cache_dir, strategy, meta, remap))
        .collect::<Result<Vec<_>>>()?;
    let (fst, postings) =
        crate::build::merge_posting_runs(runs, strategy, u32::try_from(tables.documents.len())?)?;
    put_segment_files(store, &fst, &postings, &tables)
}
