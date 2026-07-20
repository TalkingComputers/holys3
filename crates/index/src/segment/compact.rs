use super::cache::{cached_blob, cached_file, map_file};
use super::{
    validate_segment_tables, SegmentMeta, MERGE_DOCS_CAP, MERGE_POSTINGS_CAP, MERGE_TERMS_CAP,
    SEGMENT_COUNT_TARGET, SEGMENT_DOC_CAP,
};
use crate::format::{DeadSet, DocEntry, SegmentTables, SourceEntry};
use crate::pack::{fetch_documents, request_windows, PackBuilder, PackRequest, PackSlice};
use crate::terms::TermMap;
use anyhow::{Context, Result};
use seagrep_core::{BlobStore, DocId, DocumentBody, Strategy};
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
                // The merged posting id space (candidate blocks for trigram)
                // must stay within the 24-bit `pack_posting` count ceiling.
                // Summing whole-segment block counts overstates the live
                // total, which only makes this check conservative.
                && segments[i]
                    .0
                    .block_count
                    .saturating_add(segments[i + 1].0.block_count)
                    <= crate::eval::MAX_POSTING_COUNT
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
    old_tables: &SegmentTables,
    new_bases: &[u32],
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
    let map = TermMap::open(terms, strategy)?;
    let old_bases = match strategy {
        Strategy::Trigram => {
            let bases = super::build_block_bases(old_tables)?;
            anyhow::ensure!(
                bases.last().copied().unwrap_or(0) as u64 == meta.block_count,
                "compaction source block count differs from its document table"
            );
            bases
        }
        Strategy::Sparse => Vec::new(),
    };
    let id_space = u32::try_from(meta.block_count)?;
    let mut file = tempfile::NamedTempFile::new()?;
    let mut writer = BufWriter::new(file.as_file_mut());
    map.visit(|gram, packed, len| {
        write_live_entry(
            &mut writer,
            strategy,
            id_space,
            meta.postings_data_len,
            &postings,
            remap,
            &old_bases,
            new_bases,
            gram,
            packed,
            len,
        )
    })?;
    writer.flush()?;
    drop(writer);
    Ok(file.into_temp_path())
}

#[allow(clippy::too_many_arguments)]
fn write_live_entry(
    writer: &mut impl Write,
    strategy: Strategy,
    id_space: u32,
    postings_len: u64,
    postings: &[u8],
    remap: &[Option<DocId>],
    old_bases: &[u32],
    new_bases: &[u32],
    gram: &[u8],
    packed: u64,
    len: Option<u64>,
) -> Result<()> {
    let (offset, count) = crate::eval::unpack_posting(packed);
    anyhow::ensure!(count > 0, "term map contains an empty posting list");
    anyhow::ensure!(
        gram.len() == crate::build::key_bytes(strategy),
        "term map gram width does not match the index strategy"
    );
    let mut padded = [0u8; 8];
    padded[8 - gram.len()..].copy_from_slice(gram);
    let key = u64::from_be_bytes(padded);
    if count == 1 {
        // Singleton grams inline their posting id in the offset field; no
        // posting block exists to read.
        let id = u32::try_from(offset).context("singleton posting id overflows u32")?;
        if let Some(new_id) = remap_posting_id(strategy, id, remap, old_bases, new_bases)? {
            crate::build::write_posting_record(writer, strategy, key, new_id)?;
        }
        return Ok(());
    }
    anyhow::ensure!(
        count <= id_space,
        "term map posting count exceeds its segment id space"
    );
    let len = match strategy {
        Strategy::Sparse => len.context("sparse entries must carry a posting length")?,
        Strategy::Trigram => {
            anyhow::ensure!(len.is_none(), "trigram lengths derive from counts");
            crate::posting_block_len(count, id_space)
        }
    };
    let end = offset
        .checked_add(len)
        .context("term map posting length overflows")?;
    anyhow::ensure!(
        end <= postings_len,
        "term map posting extends beyond postings.bin"
    );
    let block = postings
        .get(usize::try_from(offset)?..usize::try_from(end)?)
        .context("truncated postings.bin during merge")?;
    let decoded = match strategy {
        Strategy::Sparse => crate::delta_blocks::decode_delta_blocks(block, count, id_space)?,
        Strategy::Trigram => crate::decode_posting_block(block, count, id_space)?,
    };
    let mut ids = Vec::new();
    for id in decoded {
        if let Some(id) = remap_posting_id(strategy, id, remap, old_bases, new_bases)? {
            ids.push(id);
        }
    }
    ids.sort_unstable();
    ids.dedup();
    for id in ids {
        crate::build::write_posting_record(writer, strategy, key, id)?;
    }
    Ok(())
}

fn remap_posting_id(
    strategy: Strategy,
    id: u32,
    remap: &[Option<DocId>],
    old_bases: &[u32],
    new_bases: &[u32],
) -> Result<Option<u32>> {
    let old_document = match strategy {
        Strategy::Sparse => usize::try_from(id)?,
        Strategy::Trigram => super::block_document(id, old_bases),
    };
    let Some(new_document) = remap
        .get(old_document)
        .context("posting ID is outside its segment")?
    else {
        return Ok(None);
    };
    match strategy {
        Strategy::Sparse => Ok(Some(*new_document)),
        Strategy::Trigram => {
            let new_base = new_bases
                .get(usize::try_from(*new_document)?)
                .context("remapped document ID is out of bounds")?;
            Ok(Some(new_base + id - old_bases[old_document]))
        }
    }
}

pub(super) fn merge_segments(
    store: &dyn BlobStore,
    cache_dir: &Path,
    strategy: Strategy,
    victims: &[(SegmentMeta, DeadSet)],
) -> Result<SegmentMeta> {
    type MergedSource = (SourceEntry, Vec<(DocEntry, u32)>, usize);

    let mut tables = SegmentTables {
        sources: Vec::new(),
        documents: Vec::new(),
        blocks: Vec::new(),
    };
    let mut remaps: Vec<Vec<Option<u32>>> = Vec::with_capacity(victims.len());
    let mut entries: Vec<MergedSource> = Vec::new();
    let mut victim_tables = Vec::with_capacity(victims.len());
    for (seg_idx, (meta, dead)) in victims.iter().enumerate() {
        let loaded = crate::format::parse_tables(&cached_blob(
            store,
            cache_dir,
            &meta.seg_id,
            "docs.bin",
            meta.docs_len,
            &meta.docs_hash,
        )?)?;
        validate_segment_tables(meta, &loaded)?;
        remaps.push(vec![None; loaded.documents.len()]);
        for (source_id, source) in loaded.sources.iter().cloned().enumerate() {
            if dead.sources.binary_search(&(source_id as u32)).is_ok() {
                continue;
            }
            let start = source.first_doc as usize;
            let end = start + source.doc_count as usize;
            let documents = loaded.documents[start..end]
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
        victim_tables.push(loaded);
    }
    entries.sort_unstable_by(|(left, _, _), (right, _, _)| left.key.cmp(&right.key));
    let mut merged_documents = Vec::new();
    for (mut source, source_documents, seg_idx) in entries {
        let source_id = u32::try_from(tables.sources.len())?;
        source.first_doc = u32::try_from(merged_documents.len())?;
        source.doc_count = u32::try_from(source_documents.len())?;
        for (mut document, old_id) in source_documents {
            document.source_id = source_id;
            merged_documents.push((Some(document), old_id, seg_idx));
        }
        tables.sources.push(source);
    }

    let requests = merged_documents
        .iter()
        .enumerate()
        .map(|(index, (document, _, _))| {
            let document = document.as_ref().expect("document exists");
            PackRequest {
                index,
                slice: PackSlice {
                    first_block: document.first_block,
                    block_offset: document.block_offset,
                },
                decoded_size: document.decoded_size,
            }
        })
        .collect::<Vec<_>>();
    let mut pack_builder = PackBuilder::production()?;
    for window in request_windows(&requests) {
        let window_start = window.start;
        let mut fetched = std::iter::repeat_with(|| None)
            .take(window.len())
            .collect::<Vec<Option<DocumentBody>>>();
        for seg_idx in 0..victims.len() {
            let selected = requests[window.clone()]
                .iter()
                .filter(|request| merged_documents[request.index].2 == seg_idx)
                .copied()
                .collect::<Vec<_>>();
            if selected.is_empty() {
                continue;
            }
            fetch_documents(
                store,
                None,
                &victims[seg_idx].0.packs,
                &victim_tables[seg_idx].blocks,
                &selected,
                &mut |index, body| {
                    let slot = index
                        .checked_sub(window_start)
                        .filter(|slot| *slot < fetched.len())
                        .context("compaction document is outside its fetch window")?;
                    anyhow::ensure!(
                        fetched[slot].replace(body).is_none(),
                        "compaction document was fetched twice"
                    );
                    Ok(())
                },
            )?;
        }
        for index in window {
            let body = fetched[index - window_start]
                .take()
                .context("compaction did not fetch every live document")?;
            let (document, old_id, seg_idx) = &mut merged_documents[index];
            let mut document = document.take().context("compaction document was reused")?;
            let slice = pack_builder.append(body.into_reader(), document.decoded_size)?;
            document.first_block = slice.first_block;
            document.block_offset = slice.block_offset;
            let new_id = u32::try_from(tables.documents.len())?;
            remaps[*seg_idx][*old_id as usize] = Some(new_id);
            tables.documents.push(document);
        }
    }

    let packed = pack_builder.finish()?;
    tables.blocks = packed.blocks;
    tables.validate()?;
    let new_bases = match strategy {
        Strategy::Trigram => super::build_block_bases(&tables)?,
        Strategy::Sparse => Vec::new(),
    };
    let runs = victims
        .iter()
        .zip(&victim_tables)
        .zip(&remaps)
        .map(|(((meta, _), old_tables), remap)| {
            write_compaction_run(
                store, cache_dir, strategy, meta, old_tables, &new_bases, remap,
            )
        })
        .collect::<Result<Vec<_>>>()?;
    crate::segment::merge_and_put_segment(store, strategy, runs, &tables, &packed.packs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::eval::pack_posting;

    #[test]
    fn oversized_grams_error_instead_of_panicking() {
        // A gram wider than the strategy's key is a corrupt term map; it
        // must fail the width check, never underflow the key padding.
        let mut writer = Vec::new();
        let packed = pack_posting(0, 1).expect("test setup failed");
        let error = write_live_entry(
            &mut writer,
            Strategy::Trigram,
            4,
            0,
            &[],
            &[Some(0), Some(1), None, Some(2)],
            &[0, 1, 2, 3, 4],
            &[0, 1, 2, 3],
            b"ninebytes",
            packed,
            None,
        )
        .expect_err("oversized gram must error");
        assert!(error.to_string().contains("gram width"), "{error:#}");
    }

    #[test]
    fn singleton_entries_remap_without_touching_postings() {
        let mut writer = Vec::new();
        let packed = pack_posting(3, 1).expect("test setup failed");
        write_live_entry(
            &mut writer,
            Strategy::Trigram,
            4,
            0,
            &[],
            &[Some(0), Some(1), None, Some(2)],
            &[0, 1, 2, 3, 4],
            &[0, 1, 2, 3],
            b"abc",
            packed,
            None,
        )
        .expect("singleton entry");
        assert!(!writer.is_empty(), "remapped singleton must be written");

        writer.clear();
        let out_of_bounds = pack_posting(9, 1).expect("test setup failed");
        assert!(write_live_entry(
            &mut writer,
            Strategy::Trigram,
            4,
            0,
            &[],
            &[Some(0), Some(1), None, Some(2)],
            &[0, 1, 2, 3, 4],
            &[0, 1, 2, 3],
            b"abc",
            out_of_bounds,
            None,
        )
        .is_err());
    }

    #[test]
    fn trigram_block_ids_keep_document_offsets_when_remapped() {
        let remap = [Some(1), None, Some(0)];
        let old_bases = [0, 2, 5, 6];
        let new_bases = [0, 1, 3];
        assert_eq!(
            remap_posting_id(Strategy::Trigram, 1, &remap, &old_bases, &new_bases).unwrap(),
            Some(2)
        );
        assert_eq!(
            remap_posting_id(Strategy::Trigram, 4, &remap, &old_bases, &new_bases).unwrap(),
            None
        );
        assert_eq!(
            remap_posting_id(Strategy::Trigram, 5, &remap, &old_bases, &new_bases).unwrap(),
            Some(0)
        );
    }

    #[test]
    fn sparse_ids_remap_without_block_bases() {
        assert_eq!(
            remap_posting_id(Strategy::Sparse, 1, &[Some(2), Some(0)], &[], &[]).unwrap(),
            Some(0)
        );
    }
}
