use super::{HashWriter, TempBlob};
use crate::terms::TermBuilder;
use crate::{encode_posting_block, eval};
use anyhow::{Context, Result};
use holys3_core::{iterate_sparse_grams, start_sparse_gram_ranges, DocId, Strategy};
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::mem::size_of;
use std::ops::Range;
use tempfile::TempPath;

pub(crate) const SPARSE_FILE_CHUNK: usize = 1024 * 1024;
const SPARSE_TRIGRAM_BITMAP_MIN: usize = 512 * 1024;
const TRIGRAM_RADIX_ENTRIES_CAP: usize = 4 * 1024 * 1024;
const TRIGRAM_BITMAP_WORDS: usize = (1 << 24) / 64;
const TRIGRAM_RUN_BUDGET_BYTES: usize = 256 * 1024 * 1024;
const TRIGRAM_SHARD_RUN_BYTES: u64 = 7 * 1024 * 1024;

fn uses_sharded_terms(strategy: Strategy, run_bytes: u64) -> bool {
    strategy == Strategy::Trigram && run_bytes >= TRIGRAM_SHARD_RUN_BYTES
}

fn read_file_trigrams(
    file: &mut File,
    start: u64,
    len: u64,
    mut visit: impl FnMut(u32),
) -> Result<()> {
    const CHUNK_BYTES: usize = 1024 * 1024;
    file.seek(SeekFrom::Start(start))?;
    let chunk_bytes = usize::try_from(len.min(u64::try_from(CHUNK_BYTES)?))?;
    let mut chunk = vec![0u8; chunk_bytes + 2];
    let mut carry = 0usize;
    let mut remaining = len;
    while remaining > 0 {
        let read = usize::try_from(remaining.min(u64::try_from(chunk_bytes)?))?;
        file.read_exact(&mut chunk[carry..carry + read])?;
        let end = carry + read;
        for window in chunk[..end].windows(3) {
            visit(u32::from(window[0]) << 16 | u32::from(window[1]) << 8 | u32::from(window[2]));
        }
        carry = end.min(2);
        chunk.copy_within(end - carry..end, 0);
        remaining -= u64::try_from(read)?;
    }
    Ok(())
}

fn uses_trigram_bitmap(len: u64) -> Result<bool> {
    let threshold = u64::try_from(TRIGRAM_BITMAP_WORDS * size_of::<u64>() / size_of::<u32>())?;
    Ok(len.saturating_sub(2) > threshold)
}

fn read_trigram_bitmap(file: &mut File, start: u64, len: u64) -> Result<Vec<u64>> {
    let mut bitmap = vec![0u64; TRIGRAM_BITMAP_WORDS];
    read_file_trigrams(file, start, len, |gram| {
        let gram = usize::try_from(gram).expect("u32 fits usize");
        bitmap[gram / 64] |= 1u64 << (gram % 64);
    })?;
    Ok(bitmap)
}

fn pack_trigram_bitmap(bitmap: Vec<u64>) -> Vec<u32> {
    let count = bitmap
        .iter()
        .map(|word| usize::try_from(word.count_ones()).expect("u32 fits usize"))
        .sum();
    let mut packed = Vec::with_capacity(count);
    for (word_index, mut word) in bitmap.into_iter().enumerate() {
        while word != 0 {
            let bit = usize::try_from(word.trailing_zeros()).expect("u32 fits usize");
            packed.push(u32::try_from(word_index * 64 + bit).expect("trigram fits u32"));
            word &= word - 1;
        }
    }
    packed
}

pub(crate) fn pack_file_trigrams(file: &mut File, start: u64, len: u64) -> Result<Vec<u32>> {
    if uses_trigram_bitmap(len)? {
        return Ok(pack_trigram_bitmap(read_trigram_bitmap(file, start, len)?));
    }
    let mut packed = Vec::with_capacity(usize::try_from(len.saturating_sub(2))?);
    read_file_trigrams(file, start, len, |gram| packed.push(gram))?;
    if packed.len() < 512 {
        packed.sort_unstable();
    } else {
        radsort::sort(&mut packed);
    }
    packed.dedup();
    Ok(packed)
}

pub(crate) fn collect_file_trigrams(file: &mut File, start: u64, len: u64) -> Result<IndexedGrams> {
    if uses_trigram_bitmap(len)? {
        Ok(IndexedGrams::TrigramBitmap(read_trigram_bitmap(
            file, start, len,
        )?))
    } else {
        Ok(IndexedGrams::Trigram(pack_file_trigrams(file, start, len)?))
    }
}

struct SparseRunWriter {
    id: DocId,
    entries: rapidhash::RapidHashSet<Vec<u8>>,
    bytes: usize,
    run_bytes: usize,
    runs: Vec<TempPath>,
}

struct SparseFileReader {
    file: File,
    base: u64,
    len: usize,
    chunk_start: usize,
    chunk: Vec<u8>,
    left: Vec<u8>,
    right: Vec<u8>,
}

impl SparseFileReader {
    fn open(file: File) -> Result<Self> {
        let len = file.metadata()?.len();
        Self::open_range(file, 0, len)
    }

    fn open_range(file: File, start: u64, len: u64) -> Result<Self> {
        let end = start
            .checked_add(len)
            .context("sparse gram file range overflows")?;
        anyhow::ensure!(
            end <= file.metadata()?.len(),
            "sparse gram file range is out of bounds"
        );
        Ok(Self {
            file,
            base: start,
            len: usize::try_from(len)?,
            chunk_start: 0,
            chunk: Vec::new(),
            left: vec![0; 64 * 1024],
            right: vec![0; 64 * 1024],
        })
    }

    fn file_offset(&self, index: usize) -> Result<u64> {
        self.base
            .checked_add(u64::try_from(index)?)
            .context("sparse gram file offset overflows")
    }

    fn load_chunk(&mut self, index: usize) -> Result<()> {
        anyhow::ensure!(index < self.len, "sparse gram byte is out of bounds");
        let start = index / SPARSE_FILE_CHUNK * SPARSE_FILE_CHUNK;
        let len = (self.len - start).min(SPARSE_FILE_CHUNK);
        self.chunk.resize(len, 0);
        let offset = self.file_offset(start)?;
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.read_exact(&mut self.chunk)?;
        self.chunk_start = start;
        Ok(())
    }

    fn byte_len(&self) -> usize {
        self.len
    }

    fn read_byte(&mut self, index: usize) -> Result<u8> {
        let chunk_end = self.chunk_start + self.chunk.len();
        if index < self.chunk_start || index >= chunk_end {
            self.load_chunk(index)?;
        }
        Ok(self.chunk[index - self.chunk_start])
    }

    fn read_bytes(&mut self, range: &Range<usize>, bytes: &mut [u8]) -> Result<()> {
        anyhow::ensure!(
            range.end <= self.len && range.len() == bytes.len(),
            "sparse gram range is out of bounds"
        );
        let chunk_end = self
            .chunk_start
            .checked_add(self.chunk.len())
            .context("sparse file chunk range overflows")?;
        if range.start >= self.chunk_start && range.end <= chunk_end {
            let range = range.start - self.chunk_start..range.end - self.chunk_start;
            bytes.copy_from_slice(&self.chunk[range]);
            return Ok(());
        }
        let offset = self.file_offset(range.start)?;
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.read_exact(bytes)?;
        Ok(())
    }

    fn has_equal_bytes(&mut self, left: &Range<usize>, right: &Range<usize>) -> Result<bool> {
        anyhow::ensure!(
            left.end <= self.len && right.end <= self.len,
            "sparse gram range is out of bounds"
        );
        if left.len() != right.len() {
            return Ok(false);
        }
        if left == right {
            return Ok(true);
        }
        let chunk_end = self
            .chunk_start
            .checked_add(self.chunk.len())
            .context("sparse file chunk range overflows")?;
        if left.start >= self.chunk_start
            && right.start >= self.chunk_start
            && left.end <= chunk_end
            && right.end <= chunk_end
        {
            let left = left.start - self.chunk_start..left.end - self.chunk_start;
            let right = right.start - self.chunk_start..right.end - self.chunk_start;
            return Ok(self.chunk[left] == self.chunk[right]);
        }
        if left.len() <= 64 {
            for at in 0..left.len() {
                if self.read_byte(left.start + at)? != self.read_byte(right.start + at)? {
                    return Ok(false);
                }
            }
            return Ok(true);
        }
        let mut at = 0usize;
        while at < left.len() {
            let len = (left.len() - at).min(self.left.len());
            let left_offset = self.file_offset(left.start + at)?;
            let right_offset = self.file_offset(right.start + at)?;
            self.file.seek(SeekFrom::Start(left_offset))?;
            self.file.read_exact(&mut self.left[..len])?;
            self.file.seek(SeekFrom::Start(right_offset))?;
            self.file.read_exact(&mut self.right[..len])?;
            if self.left[..len] != self.right[..len] {
                return Ok(false);
            }
            at += len;
        }
        Ok(true)
    }
}

struct RecentSparseGram {
    range: Range<usize>,
    inline: Option<u64>,
}

fn mark_short_gram(bitmap: &mut [u64], gram: usize) -> bool {
    let word = &mut bitmap[gram / 64];
    let bit = 1u64 << (gram % 64);
    let seen = *word & bit != 0;
    *word |= bit;
    seen
}

impl SparseRunWriter {
    fn new(idx: usize, run_bytes: usize) -> Result<Self> {
        anyhow::ensure!(run_bytes > 0, "sparse posting run size must be positive");
        Ok(Self {
            id: DocId::try_from(idx)?,
            entries: rapidhash::RapidHashSet::default(),
            bytes: 0,
            run_bytes,
            runs: Vec::new(),
        })
    }

    fn add(&mut self, text: &[u8]) -> Result<()> {
        let mut recent: [Option<&[u8]>; 2] = [None, None];
        let mut recent_index = 0usize;
        for gram in iterate_sparse_grams(text) {
            if recent.iter().flatten().any(|previous| *previous == gram) {
                continue;
            }
            recent[recent_index] = Some(gram);
            recent_index = (recent_index + 1) % recent.len();
            if !self.entries.contains(gram) {
                self.add_entry(gram.to_vec())?;
            }
        }
        Ok(())
    }

    fn add_file(&mut self, file: File) -> Result<()> {
        self.add_reader(&mut SparseFileReader::open(file)?)
    }

    fn add_range(&mut self, file: File, start: u64, len: u64) -> Result<()> {
        self.add_reader(&mut SparseFileReader::open_range(file, start, len)?)
    }

    fn add_reader(&mut self, text: &mut SparseFileReader) -> Result<()> {
        let mut recent: [Option<RecentSparseGram>; 2] = [None, None];
        let mut recent_index = 0usize;
        let mut ranges = start_sparse_gram_ranges(text.byte_len());
        let mut pairs = [0u64; (1 << 16) / 64];
        let mut trigrams =
            (text.byte_len() >= SPARSE_TRIGRAM_BITMAP_MIN).then(|| vec![0u64; (1 << 24) / 64]);
        let mut inline = [0u8; 8];
        while let Some(range) = ranges.next_with(|index| text.read_byte(index))? {
            let is_inline = range.len() <= inline.len();
            let inline_value = if is_inline {
                inline.fill(0);
                text.read_bytes(&range, &mut inline[..range.len()])?;
                Some(u64::from_le_bytes(inline))
            } else {
                None
            };
            let seen = match range.len() {
                2 => mark_short_gram(
                    &mut pairs,
                    usize::from(inline[0]) << 8 | usize::from(inline[1]),
                ),
                3 => trigrams.as_mut().is_some_and(|trigrams| {
                    mark_short_gram(
                        trigrams,
                        usize::from(inline[0]) << 16
                            | usize::from(inline[1]) << 8
                            | usize::from(inline[2]),
                    )
                }),
                _ => false,
            };
            if seen {
                continue;
            }
            let mut duplicate = false;
            if range.len() > 3 || range.len() == 3 && trigrams.is_none() {
                for previous in recent.iter().flatten() {
                    if previous.range.len() != range.len() {
                        continue;
                    }
                    let equal = match previous.inline {
                        Some(previous) if is_inline => Some(previous) == inline_value,
                        _ => text.has_equal_bytes(&previous.range, &range)?,
                    };
                    if equal {
                        duplicate = true;
                        break;
                    }
                }
                if !duplicate {
                    recent[recent_index] = Some(RecentSparseGram {
                        range: range.clone(),
                        inline: inline_value,
                    });
                    recent_index = (recent_index + 1) % recent.len();
                }
            }
            if duplicate {
                continue;
            }
            let gram = if is_inline {
                inline[..range.len()].to_vec()
            } else {
                let mut gram = vec![0u8; range.len()];
                text.read_bytes(&range, &mut gram)?;
                gram
            };
            self.add_entry(gram)?;
        }
        Ok(())
    }

    fn add_entry(&mut self, gram: Vec<u8>) -> Result<()> {
        let entry_bytes = size_of::<Vec<u8>>()
            .saturating_add(size_of::<usize>())
            .saturating_add(gram.len());
        if self.entries.insert(gram) {
            self.bytes = self.bytes.saturating_add(entry_bytes);
            if self.bytes >= self.run_bytes {
                self.flush()?;
            }
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if self.entries.is_empty() {
            return Ok(());
        }
        self.runs.push(write_sparse_run(&self.entries, self.id)?);
        self.entries.clear();
        self.bytes = 0;
        Ok(())
    }

    fn finish(mut self) -> Result<Vec<TempPath>> {
        self.flush()?;
        Ok(self.runs)
    }
}

pub(crate) enum IndexedGrams {
    Trigram(Vec<u32>),
    TrigramBitmap(Vec<u64>),
    Sparse(bytes::Bytes),
    SparseFile(File),
    SparsePath(TempPath),
    TrigramSpool { offset: u64, len: u64 },
    SparseSpool { offset: u64, len: u64 },
}

pub(crate) fn write_posting_runs(
    grammed: Vec<(usize, IndexedGrams)>,
    strategy: Strategy,
    sparse_run_bytes: usize,
    spool: Option<&File>,
) -> Result<Vec<TempPath>> {
    match strategy {
        Strategy::Trigram => write_trigram_runs(grammed, spool),
        Strategy::Sparse => {
            let mut runs = Vec::new();
            for (idx, grams) in grammed {
                let mut writer = SparseRunWriter::new(idx, sparse_run_bytes)?;
                match grams {
                    IndexedGrams::Sparse(text) => writer.add(&text)?,
                    IndexedGrams::SparseFile(file) => writer.add_file(file)?,
                    IndexedGrams::SparsePath(path) => {
                        let file = File::open(&path)?;
                        writer.add_file(file)?;
                    }
                    IndexedGrams::SparseSpool { offset, len } => writer.add_range(
                        spool
                            .context("spooled grams have no content spool")?
                            .try_clone()?,
                        offset,
                        len,
                    )?,
                    IndexedGrams::Trigram(_)
                    | IndexedGrams::TrigramBitmap(_)
                    | IndexedGrams::TrigramSpool { .. } => {
                        anyhow::bail!("mixed gram strategies in build chunk");
                    }
                }
                runs.extend(writer.finish()?);
            }
            Ok(runs)
        }
    }
}

fn write_trigram_runs(
    grammed: Vec<(usize, IndexedGrams)>,
    spool: Option<&File>,
) -> Result<Vec<TempPath>> {
    let mut documents = Vec::new();
    let mut runs = Vec::new();
    let mut pending_bytes = 0usize;
    for (idx, grams) in grammed {
        let grams = match grams {
            IndexedGrams::TrigramSpool { offset, len } => collect_file_trigrams(
                &mut spool
                    .context("spooled grams have no content spool")?
                    .try_clone()?,
                offset,
                len,
            )?,
            grams => grams,
        };
        match grams {
            IndexedGrams::Trigram(grams) => {
                let bytes = grams
                    .len()
                    .checked_mul(size_of::<u32>())
                    .context("trigram run memory size overflows")?;
                if !documents.is_empty()
                    && bytes > TRIGRAM_RUN_BUDGET_BYTES.saturating_sub(pending_bytes)
                {
                    runs.push(write_trigram_documents(std::mem::take(&mut documents))?);
                    pending_bytes = 0;
                }
                pending_bytes = pending_bytes
                    .checked_add(bytes)
                    .context("trigram run memory size overflows")?;
                documents.push((idx, grams));
            }
            IndexedGrams::TrigramBitmap(bitmap) => {
                runs.push(write_trigram_bitmap_run(idx, bitmap)?);
            }
            IndexedGrams::Sparse(_)
            | IndexedGrams::SparseFile(_)
            | IndexedGrams::SparsePath(_)
            | IndexedGrams::SparseSpool { .. } => {
                anyhow::bail!("mixed gram strategies in build chunk");
            }
            IndexedGrams::TrigramSpool { .. } => unreachable!(),
        }
    }
    if !documents.is_empty() {
        runs.push(write_trigram_documents(documents)?);
    }
    Ok(runs)
}

fn write_trigram_documents(documents: Vec<(usize, Vec<u32>)>) -> Result<TempPath> {
    let entries = documents.iter().try_fold(0usize, |total, (_, grams)| {
        total
            .checked_add(grams.len())
            .context("trigram run size overflows")
    })?;
    if entries <= TRIGRAM_RADIX_ENTRIES_CAP {
        write_trigram_run_radix(documents)
    } else {
        write_trigram_run_merge(documents)
    }
}

fn write_trigram_bitmap_run(idx: usize, bitmap: Vec<u64>) -> Result<TempPath> {
    let id = DocId::try_from(idx)?;
    let mut file = tempfile::NamedTempFile::new()?;
    let mut writer = BufWriter::new(file.as_file_mut());
    for (word_index, mut word) in bitmap.into_iter().enumerate() {
        while word != 0 {
            let bit = usize::try_from(word.trailing_zeros()).expect("u32 fits usize");
            let gram = u32::try_from(word_index * 64 + bit).expect("trigram fits u32");
            writer.write_all(&gram.to_be_bytes()[1..])?;
            writer.write_all(&id.to_be_bytes())?;
            word &= word - 1;
        }
    }
    writer.flush()?;
    drop(writer);
    Ok(file.into_temp_path())
}

pub(crate) fn write_trigram_run_radix(grammed: Vec<(usize, Vec<u32>)>) -> Result<TempPath> {
    let mut file = tempfile::NamedTempFile::new()?;
    let mut writer = BufWriter::new(file.as_file_mut());
    let mut entries = Vec::new();
    for (idx, grams) in grammed {
        let id = DocId::try_from(idx)?;
        entries.extend(
            grams
                .into_iter()
                .map(|gram| u64::from(gram) << 32 | u64::from(id)),
        );
    }
    radsort::sort(&mut entries);
    entries.dedup();
    for entry in entries {
        let gram = (entry >> 32) as u32;
        let id = entry as DocId;
        writer.write_all(&gram.to_be_bytes()[1..])?;
        writer.write_all(&id.to_be_bytes())?;
    }
    writer.flush()?;
    drop(writer);
    Ok(file.into_temp_path())
}

pub(crate) fn write_trigram_run_merge(grammed: Vec<(usize, Vec<u32>)>) -> Result<TempPath> {
    let mut documents = grammed
        .into_iter()
        .map(|(idx, grams)| Ok((DocId::try_from(idx)?, grams)))
        .collect::<Result<Vec<_>>>()?;
    documents.sort_unstable_by_key(|(id, _)| *id);
    let mut pending = BinaryHeap::new();
    for (document_index, (id, grams)) in documents.iter().enumerate() {
        if let Some(&gram) = grams.first() {
            pending.push(Reverse((gram, *id, document_index, 0usize)));
        }
    }
    let mut file = tempfile::NamedTempFile::new()?;
    let mut writer = BufWriter::new(file.as_file_mut());
    let mut previous = None;
    while let Some(Reverse((gram, id, document_index, gram_index))) = pending.pop() {
        let record = (gram, id);
        if previous != Some(record) {
            writer.write_all(&gram.to_be_bytes()[1..])?;
            writer.write_all(&id.to_be_bytes())?;
            previous = Some(record);
        }
        let next_index = gram_index + 1;
        if let Some(&next_gram) = documents[document_index].1.get(next_index) {
            pending.push(Reverse((next_gram, id, document_index, next_index)));
        }
    }
    writer.flush()?;
    drop(writer);
    Ok(file.into_temp_path())
}

fn write_sparse_run(entries: &rapidhash::RapidHashSet<Vec<u8>>, id: DocId) -> Result<TempPath> {
    let mut ordered = entries.iter().collect::<Vec<_>>();
    ordered.sort_unstable();
    let mut file = tempfile::NamedTempFile::new()?;
    let mut writer = BufWriter::new(file.as_file_mut());
    for gram in ordered {
        writer.write_all(&u32::try_from(gram.len())?.to_be_bytes())?;
        writer.write_all(gram)?;
        writer.write_all(&id.to_be_bytes())?;
    }
    writer.flush()?;
    drop(writer);
    Ok(file.into_temp_path())
}

fn insert_posting_file<W: Write>(
    builder: &mut TermBuilder<W>,
    postings: &mut impl Write,
    offset: &mut u64,
    gram: &[u8],
    mut ids: Vec<DocId>,
    doc_count: u32,
) -> Result<()> {
    ids.sort_unstable();
    ids.dedup();
    if ids.is_empty() {
        return Ok(());
    }
    let mut block = Vec::new();
    encode_posting_block(&mut block, &ids, doc_count);
    builder.insert(gram, eval::pack_posting(*offset, ids.len())?)?;
    postings.write_all(&block)?;
    *offset += u64::try_from(block.len())?;
    Ok(())
}

pub(crate) struct PostingRun {
    pub(crate) reader: BufReader<File>,
    pub(crate) strategy: Strategy,
}

pub(crate) const MAX_OPEN_POSTING_RUNS: usize = 64;

impl PostingRun {
    pub(crate) fn read_record(&mut self) -> Result<Option<(Vec<u8>, DocId)>> {
        match self.strategy {
            Strategy::Trigram => {
                let mut record = [0u8; 7];
                if !read_exact_or_eof(&mut self.reader, &mut record)? {
                    return Ok(None);
                }
                Ok(Some((
                    record[..3].to_vec(),
                    DocId::from_be_bytes(record[3..].try_into()?),
                )))
            }
            Strategy::Sparse => {
                let mut length = [0u8; 4];
                if !read_exact_or_eof(&mut self.reader, &mut length)? {
                    return Ok(None);
                }
                let mut gram = vec![0; usize::try_from(u32::from_be_bytes(length))?];
                self.reader
                    .read_exact(&mut gram)
                    .context("truncated temporary posting run")?;
                let mut id = [0u8; 4];
                self.reader
                    .read_exact(&mut id)
                    .context("truncated temporary posting run")?;
                Ok(Some((gram, DocId::from_be_bytes(id))))
            }
        }
    }
}

fn read_exact_or_eof(reader: &mut impl Read, bytes: &mut [u8]) -> Result<bool> {
    match reader.read(&mut bytes[..1])? {
        0 => Ok(false),
        1 => {
            reader
                .read_exact(&mut bytes[1..])
                .context("truncated temporary posting run")?;
            Ok(true)
        }
        _ => unreachable!(),
    }
}

pub(crate) fn write_posting_record(
    writer: &mut impl Write,
    strategy: Strategy,
    gram: &[u8],
    id: DocId,
) -> Result<()> {
    match strategy {
        Strategy::Trigram => {
            anyhow::ensure!(gram.len() == 3, "temporary trigram has invalid length");
            writer.write_all(gram)?;
        }
        Strategy::Sparse => {
            writer.write_all(&u32::try_from(gram.len())?.to_be_bytes())?;
            writer.write_all(gram)?;
        }
    }
    writer.write_all(&id.to_be_bytes())?;
    Ok(())
}

fn merge_run_group(paths: Vec<TempPath>, strategy: Strategy) -> Result<TempPath> {
    let mut runs = paths
        .iter()
        .map(|path| {
            Ok(PostingRun {
                reader: BufReader::new(File::open(path)?),
                strategy,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let mut heap = BinaryHeap::new();
    for (run_idx, run) in runs.iter_mut().enumerate() {
        if let Some((gram, id)) = run.read_record()? {
            heap.push(Reverse((gram, id, run_idx)));
        }
    }
    let mut file = tempfile::NamedTempFile::new()?;
    let mut writer = BufWriter::new(file.as_file_mut());
    let mut previous = None;
    while let Some(Reverse((gram, id, run_idx))) = heap.pop() {
        if previous
            .as_ref()
            .is_none_or(|(previous_gram, previous_id)| previous_gram != &gram || *previous_id != id)
        {
            write_posting_record(&mut writer, strategy, &gram, id)?;
            previous = Some((gram, id));
        }
        if let Some((gram, id)) = runs[run_idx].read_record()? {
            heap.push(Reverse((gram, id, run_idx)));
        }
    }
    writer.flush()?;
    drop(writer);
    Ok(file.into_temp_path())
}

pub(crate) fn collapse_posting_runs(
    mut runs: Vec<TempPath>,
    strategy: Strategy,
) -> Result<Vec<TempPath>> {
    while runs.len() > MAX_OPEN_POSTING_RUNS {
        let mut next = Vec::with_capacity(runs.len().div_ceil(MAX_OPEN_POSTING_RUNS));
        let mut iter = runs.into_iter();
        loop {
            let mut group = iter
                .by_ref()
                .take(MAX_OPEN_POSTING_RUNS)
                .collect::<Vec<_>>();
            if group.is_empty() {
                break;
            }
            if group.len() == 1 {
                next.push(group.pop().expect("one posting run"));
            } else {
                next.push(merge_run_group(group, strategy)?);
            }
        }
        runs = next;
    }
    Ok(runs)
}

pub(crate) fn merge_posting_runs(
    runs: Vec<TempPath>,
    strategy: Strategy,
    doc_count: u32,
) -> Result<(TempBlob, TempBlob)> {
    let paths = collapse_posting_runs(runs, strategy)?;
    let mut runs = paths
        .iter()
        .map(|path| {
            Ok(PostingRun {
                reader: BufReader::new(File::open(path)?),
                strategy,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let mut heap = BinaryHeap::new();
    for (run_idx, run) in runs.iter_mut().enumerate() {
        if let Some((gram, id)) = run.read_record()? {
            heap.push(Reverse((gram, id, run_idx)));
        }
    }
    let fst = tempfile::NamedTempFile::new()?;
    let postings = tempfile::NamedTempFile::new()?;
    let mut postings_writer = BufWriter::new(HashWriter::new(postings.reopen()?));
    let run_bytes = paths.iter().try_fold(0u64, |total, path| -> Result<u64> {
        total
            .checked_add(std::fs::metadata(path)?.len())
            .context("posting run bytes overflow u64")
    })?;
    let is_sharded = uses_sharded_terms(strategy, run_bytes);
    let mut builder = TermBuilder::new(
        strategy,
        is_sharded,
        BufWriter::new(HashWriter::new(fst.reopen()?)),
    )?;
    let mut postings_len = 0u64;
    let mut current_gram: Option<Vec<u8>> = None;
    let mut ids = Vec::new();
    while let Some(Reverse((gram, id, run_idx))) = heap.pop() {
        if current_gram.as_deref() != Some(gram.as_slice()) {
            if let Some(current) = current_gram.replace(gram) {
                insert_posting_file(
                    &mut builder,
                    &mut postings_writer,
                    &mut postings_len,
                    &current,
                    std::mem::take(&mut ids),
                    doc_count,
                )?;
            }
        }
        if ids.last() != Some(&id) {
            ids.push(id);
        }
        if let Some((next_gram, next_id)) = runs[run_idx].read_record()? {
            heap.push(Reverse((next_gram, next_id, run_idx)));
        }
    }
    if let Some(current) = current_gram {
        insert_posting_file(
            &mut builder,
            &mut postings_writer,
            &mut postings_len,
            &current,
            ids,
            doc_count,
        )?;
    }
    let mut fst_writer = builder.finish()?;
    fst_writer.flush()?;
    postings_writer.flush()?;
    let (_, fst_hash) = fst_writer
        .into_inner()
        .map_err(std::io::IntoInnerError::into_error)?
        .finish();
    let (_, postings_hash) = postings_writer
        .into_inner()
        .map_err(std::io::IntoInnerError::into_error)?
        .finish();
    let fst_len = fst.as_file().metadata()?.len();
    let written_postings_len = postings.as_file().metadata()?.len();
    anyhow::ensure!(
        postings_len == written_postings_len,
        "postings writer tracked {postings_len} bytes but wrote {written_postings_len}"
    );
    Ok((
        TempBlob {
            file: fst,
            len: fst_len,
            hash: fst_hash,
        },
        TempBlob {
            file: postings,
            len: written_postings_len,
            hash: postings_hash,
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shards_only_large_trigram_runs() {
        assert!(!uses_sharded_terms(
            Strategy::Trigram,
            TRIGRAM_SHARD_RUN_BYTES - 1
        ));
        assert!(uses_sharded_terms(
            Strategy::Trigram,
            TRIGRAM_SHARD_RUN_BYTES
        ));
        assert!(!uses_sharded_terms(
            Strategy::Sparse,
            TRIGRAM_SHARD_RUN_BYTES
        ));
    }
}
