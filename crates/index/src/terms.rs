use anyhow::{Context, Result};
use bytes::Bytes;
use fst::Streamer;
use holys3_core::Strategy;
use std::io::Write;

const TRIGRAM_SHARDS: usize = 256;
const TRIGRAM_OFFSETS: usize = TRIGRAM_SHARDS + 1;
const TRIGRAM_MAGIC: &[u8; 8] = b"HS3TERM1";
const TRIGRAM_FOOTER_LEN: usize = TRIGRAM_OFFSETS * size_of::<u64>();

struct CountingWriter<W> {
    inner: W,
    len: u64,
}

impl<W> CountingWriter<W> {
    fn new(inner: W) -> Self {
        Self { inner, len: 0 }
    }
}

impl<W: Write> Write for CountingWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let written = self.inner.write(bytes)?;
        let written_u64 = u64::try_from(written).map_err(std::io::Error::other)?;
        self.len = self
            .len
            .checked_add(written_u64)
            .ok_or_else(|| std::io::Error::other("term map length overflows u64"))?;
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

struct TrigramBuilder<W: Write> {
    builder: Option<fst::MapBuilder<CountingWriter<W>>>,
    writer: Option<CountingWriter<W>>,
    shard: usize,
    offsets: Vec<u64>,
    empty: Vec<u8>,
}

impl<W: Write> TrigramBuilder<W> {
    fn new(writer: W) -> Result<Self> {
        let mut writer = CountingWriter::new(writer);
        writer.write_all(TRIGRAM_MAGIC)?;
        Ok(Self {
            builder: None,
            writer: Some(writer),
            shard: 0,
            offsets: vec![u64::try_from(TRIGRAM_MAGIC.len())?],
            empty: fst::MapBuilder::memory().into_inner()?,
        })
    }

    fn close_shard(&mut self) -> Result<()> {
        let writer = match self.builder.take() {
            Some(builder) => builder.into_inner()?,
            None => {
                let mut writer = self.writer.take().context("trigram writer is closed")?;
                writer.write_all(&self.empty)?;
                writer
            }
        };
        self.offsets.push(writer.len);
        self.shard += 1;
        self.writer = Some(writer);
        Ok(())
    }

    fn insert(&mut self, gram: &[u8], value: u64) -> Result<()> {
        anyhow::ensure!(gram.len() == 3, "trigram term has invalid length");
        let shard = usize::from(gram[0]);
        anyhow::ensure!(shard >= self.shard, "trigram terms are not sorted");
        while self.shard < shard {
            self.close_shard()?;
        }
        if self.builder.is_none() {
            let writer = self.writer.take().context("trigram writer is closed")?;
            self.builder = Some(fst::MapBuilder::new(writer)?);
        }
        self.builder
            .as_mut()
            .context("trigram shard is closed")?
            .insert(&gram[1..], value)?;
        Ok(())
    }

    fn finish(mut self) -> Result<W> {
        while self.shard < TRIGRAM_SHARDS {
            self.close_shard()?;
        }
        anyhow::ensure!(
            self.offsets.len() == TRIGRAM_OFFSETS,
            "trigram term offsets are incomplete"
        );
        let mut writer = self.writer.take().context("trigram writer is closed")?;
        for offset in self.offsets {
            writer.write_all(&offset.to_le_bytes())?;
        }
        writer.flush()?;
        Ok(writer.inner)
    }
}

enum TermBuilderInner<W: Write> {
    Single(fst::MapBuilder<W>),
    Sparse(crate::sparse_table::SparseTableWriter<W>),
    Trigram(TrigramBuilder<W>),
}

pub(crate) struct TermBuilder<W: Write> {
    inner: TermBuilderInner<W>,
}

impl<W: Write> TermBuilder<W> {
    pub(crate) fn new(strategy: Strategy, is_sharded: bool, writer: W) -> Result<Self> {
        anyhow::ensure!(
            !is_sharded || strategy == Strategy::Trigram,
            "only trigram term maps can be sharded"
        );
        match (strategy, is_sharded) {
            (Strategy::Trigram, true) => Ok(Self {
                inner: TermBuilderInner::Trigram(TrigramBuilder::new(writer)?),
            }),
            (Strategy::Trigram, false) => Ok(Self {
                inner: TermBuilderInner::Single(fst::MapBuilder::new(writer)?),
            }),
            (Strategy::Sparse, false) => Ok(Self {
                inner: TermBuilderInner::Sparse(crate::sparse_table::SparseTableWriter::new(
                    writer,
                )?),
            }),
            (Strategy::Sparse, true) => unreachable!(),
        }
    }

    pub(crate) fn insert(&mut self, gram: &[u8], value: u64) -> Result<()> {
        match &mut self.inner {
            TermBuilderInner::Single(builder) => builder.insert(gram, value)?,
            TermBuilderInner::Sparse(builder) => {
                let hash = u64::from_be_bytes(
                    gram.try_into()
                        .context("sparse term key must be an 8-byte hash")?,
                );
                builder.insert(hash, value)?;
            }
            TermBuilderInner::Trigram(builder) => builder.insert(gram, value)?,
        }
        Ok(())
    }

    /// Returns the writer and, for sparse dictionaries, the SHA-256 of the
    /// block-index tail recorded in segment metadata.
    pub(crate) fn finish(self) -> Result<(W, Option<String>)> {
        match self.inner {
            TermBuilderInner::Single(builder) => Ok((builder.into_inner()?, None)),
            TermBuilderInner::Sparse(builder) => {
                let (writer, tail_hash) = builder.finish()?;
                Ok((writer, Some(tail_hash)))
            }
            TermBuilderInner::Trigram(builder) => Ok((builder.finish()?, None)),
        }
    }
}

/// Lazily decoded dictionary blocks: block id -> its (hash, value) entries.
type DecodedBlocks =
    std::sync::Mutex<rapidhash::RapidHashMap<usize, std::sync::Arc<Vec<(u64, u64)>>>>;

pub(crate) enum TermMap {
    Single(fst::Map<memmap2::Mmap>),
    /// Sparse dictionaries key by `hash_ngram` of the gram, not gram bytes:
    /// a sorted block table, never an FST. Varint blocks decode sequentially,
    /// so each touched block is decoded once into `decoded` and bisected on
    /// every later lookup — a per-gram scan of a 128 KiB block would repeat
    /// that decode for every gram landing in it.
    Sparse {
        index: crate::sparse_table::SparseTableIndex,
        bytes: memmap2::Mmap,
        decoded: DecodedBlocks,
    },
    /// Large sparse dictionary accessed by ranged reads: only the block
    /// index is resident; lookups are resolved per query by the reader.
    SparseRemote {
        index: crate::sparse_table::SparseTableIndex,
    },
    Trigram(Vec<fst::Map<Bytes>>),
}

impl TermMap {
    pub(crate) fn open(bytes: memmap2::Mmap, strategy: Strategy) -> Result<Self> {
        let is_sharded = strategy == Strategy::Trigram
            && bytes.len() >= TRIGRAM_MAGIC.len()
            && &bytes[..TRIGRAM_MAGIC.len()] == TRIGRAM_MAGIC;
        if !is_sharded {
            return Ok(match strategy {
                Strategy::Sparse => Self::Sparse {
                    decoded: DecodedBlocks::default(),
                    index: crate::sparse_table::SparseTableIndex::parse(
                        bytes.len() as u64,
                        &bytes,
                    )?,
                    bytes,
                },
                Strategy::Trigram => Self::Single(fst::Map::new(bytes)?),
            });
        }
        anyhow::ensure!(
            bytes.len() >= TRIGRAM_MAGIC.len() + TRIGRAM_FOOTER_LEN,
            "trigram term map footer is truncated"
        );
        let footer = bytes.len() - TRIGRAM_FOOTER_LEN;
        let offsets = bytes[footer..]
            .chunks_exact(size_of::<u64>())
            .map(|chunk| u64::from_le_bytes(chunk.try_into().expect("eight-byte offset")))
            .map(usize::try_from)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        anyhow::ensure!(
            offsets.len() == TRIGRAM_OFFSETS
                && offsets.first() == Some(&TRIGRAM_MAGIC.len())
                && offsets.last() == Some(&footer)
                && offsets.windows(2).all(|pair| pair[0] < pair[1]),
            "trigram term map offsets are invalid"
        );
        let bytes = Bytes::from_owner(bytes);
        let maps = offsets
            .windows(2)
            .map(|range| Ok(fst::Map::new(bytes.slice(range[0]..range[1]))?))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self::Trigram(maps))
    }

    /// A lookup failure is a corrupt dictionary and must surface as an error:
    /// mapping it to "gram absent" would silently drop matching documents.
    pub(crate) fn get(&self, gram: &[u8]) -> Result<Option<u64>> {
        match self {
            Self::Single(map) => Ok(map.get(gram)),
            Self::Sparse {
                index,
                bytes,
                decoded,
            } => {
                let hash = holys3_core::hash_ngram(gram);
                let Some(block_id) = index.block_for(hash) else {
                    return Ok(None);
                };
                let entries = {
                    let mut cache = decoded
                        .lock()
                        .map_err(|_| anyhow::anyhow!("sparse block cache lock is poisoned"))?;
                    match cache.get(&block_id) {
                        Some(entries) => entries.clone(),
                        None => {
                            let block = &index.blocks[block_id];
                            let start = usize::try_from(block.offset)?;
                            let end = start
                                .checked_add(usize::try_from(block.len)?)
                                .context("sparse block extent overflows")?;
                            let mut entries = Vec::new();
                            crate::sparse_table::for_each_entry(
                                bytes
                                    .get(start..end)
                                    .context("sparse block extends beyond the dictionary")?,
                                |hash, value| {
                                    entries.push((hash, value));
                                    Ok(())
                                },
                            )?;
                            let entries = std::sync::Arc::new(entries);
                            cache.insert(block_id, entries.clone());
                            entries
                        }
                    }
                };
                Ok(entries
                    .binary_search_by_key(&hash, |(entry, _)| *entry)
                    .ok()
                    .map(|at| entries[at].1))
            }
            Self::SparseRemote { .. } => {
                unreachable!("remote sparse lookups are resolved by the reader per query")
            }
            Self::Trigram(maps) if gram.len() == 3 => {
                Ok(maps[usize::from(gram[0])].get(&gram[1..]))
            }
            Self::Trigram(_) => Ok(None),
        }
    }

    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Single(map) => map.len(),
            Self::Sparse { index, .. } | Self::SparseRemote { index } => {
                usize::try_from(index.entry_count).expect("fits usize")
            }
            Self::Trigram(maps) => maps.iter().map(fst::Map::len).sum(),
        }
    }

    pub(crate) fn visit(&self, mut visit: impl FnMut(&[u8], u64) -> Result<()>) -> Result<()> {
        match self {
            Self::Single(map) => {
                let mut stream = map.stream();
                while let Some((gram, value)) = stream.next() {
                    visit(gram, value)?;
                }
            }
            Self::SparseRemote { .. } => {
                anyhow::bail!("remote sparse dictionaries do not support iteration")
            }
            Self::Sparse { index, bytes, .. } => {
                for block in &index.blocks {
                    let start = usize::try_from(block.offset)?;
                    let end = start
                        .checked_add(usize::try_from(block.len)?)
                        .context("sparse table block range overflows")?;
                    let raw = bytes
                        .get(start..end)
                        .context("sparse table block is out of bounds")?;
                    crate::sparse_table::for_each_entry(raw, |hash, value| {
                        visit(&hash.to_be_bytes(), value)
                    })?;
                }
            }
            Self::Trigram(maps) => {
                let mut gram = [0u8; 3];
                for (shard, map) in maps.iter().enumerate() {
                    gram[0] = u8::try_from(shard)?;
                    let mut stream = map.stream();
                    while let Some((suffix, value)) = stream.next() {
                        anyhow::ensure!(suffix.len() == 2, "trigram suffix has invalid length");
                        gram[1..].copy_from_slice(suffix);
                        visit(&gram, value)?;
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Compaction round-trips dictionaries through visit() -> insert():
    // whatever key bytes visit emits must be accepted, in order, by a fresh
    // builder and resolve to the same values afterwards.
    #[test]
    fn sparse_visit_round_trips_through_a_new_builder() {
        let grams: Vec<&[u8]> = vec![b"whale", b"ahab", b"pequod", b"ishmael", b"harpoon"];
        let mut entries: Vec<(u64, u64)> = grams
            .iter()
            .enumerate()
            .map(|(value, gram)| (holys3_core::hash_ngram(gram), value as u64))
            .collect();
        entries.sort_unstable();
        let mut builder = TermBuilder::new(Strategy::Sparse, false, Vec::new()).unwrap();
        for (hash, value) in &entries {
            builder.insert(&hash.to_be_bytes(), *value).unwrap();
        }
        let (bytes, _) = builder.finish().unwrap();

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("terms.fst");
        std::fs::write(&path, &bytes).unwrap();
        let mmap = unsafe { memmap2::MmapOptions::new().map(&std::fs::File::open(&path).unwrap()) }
            .unwrap();
        let map = TermMap::open(mmap, Strategy::Sparse).unwrap();

        let mut rebuilt = TermBuilder::new(Strategy::Sparse, false, Vec::new()).unwrap();
        map.visit(|key, value| rebuilt.insert(key, value))
            .expect("visit output must feed a new builder in order");
        let (rebuilt_bytes, _) = rebuilt.finish().unwrap();
        assert_eq!(bytes, rebuilt_bytes, "round trip must be byte-identical");

        for gram in grams {
            assert!(
                map.get(gram).expect("lookup").is_some(),
                "gram {gram:?} must resolve after the round trip"
            );
        }
    }

    // A block that fails to decode is a corrupt dictionary: get() must
    // surface the error, not report the gram as absent.
    #[test]
    fn corrupt_sparse_blocks_error_instead_of_resolving_absent() {
        let grams: Vec<&[u8]> = vec![b"whale", b"ahab", b"pequod", b"ishmael", b"harpoon"];
        let mut entries: Vec<(u64, u64)> = grams
            .iter()
            .enumerate()
            .map(|(value, gram)| (holys3_core::hash_ngram(gram), value as u64))
            .collect();
        entries.sort_unstable();
        let mut builder = TermBuilder::new(Strategy::Sparse, false, Vec::new()).unwrap();
        for (hash, value) in &entries {
            builder.insert(&hash.to_be_bytes(), *value).unwrap();
        }
        let (mut bytes, _) = builder.finish().unwrap();

        let index =
            crate::sparse_table::SparseTableIndex::parse(bytes.len() as u64, &bytes).unwrap();
        let block = &index.blocks[0];
        let start = usize::try_from(block.offset).unwrap() + 8;
        let end = usize::try_from(block.offset + block.len).unwrap();
        bytes[start..end].fill(0xFF);

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("terms.fst");
        std::fs::write(&path, &bytes).unwrap();
        let mmap = unsafe { memmap2::MmapOptions::new().map(&std::fs::File::open(&path).unwrap()) }
            .unwrap();
        let map = TermMap::open(mmap, Strategy::Sparse).unwrap();
        for gram in grams {
            assert!(
                map.get(gram).is_err(),
                "gram {gram:?} must error on a corrupt block"
            );
        }
    }
}
