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
            (Strategy::Trigram | Strategy::Sparse, false) => Ok(Self {
                inner: TermBuilderInner::Single(fst::MapBuilder::new(writer)?),
            }),
            (Strategy::Sparse, true) => unreachable!(),
        }
    }

    pub(crate) fn insert(&mut self, gram: &[u8], value: u64) -> Result<()> {
        match &mut self.inner {
            TermBuilderInner::Single(builder) => builder.insert(gram, value)?,
            TermBuilderInner::Trigram(builder) => builder.insert(gram, value)?,
        }
        Ok(())
    }

    pub(crate) fn finish(self) -> Result<W> {
        match self.inner {
            TermBuilderInner::Single(builder) => Ok(builder.into_inner()?),
            TermBuilderInner::Trigram(builder) => builder.finish(),
        }
    }
}

pub(crate) enum TermMap {
    Single(fst::Map<memmap2::Mmap>),
    Trigram(Vec<fst::Map<Bytes>>),
}

impl TermMap {
    pub(crate) fn open(bytes: memmap2::Mmap, strategy: Strategy) -> Result<Self> {
        let is_sharded = strategy == Strategy::Trigram
            && bytes.len() >= TRIGRAM_MAGIC.len()
            && &bytes[..TRIGRAM_MAGIC.len()] == TRIGRAM_MAGIC;
        if !is_sharded {
            return Ok(Self::Single(fst::Map::new(bytes)?));
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

    pub(crate) fn get(&self, gram: &[u8]) -> Option<u64> {
        match self {
            Self::Single(map) => map.get(gram),
            Self::Trigram(maps) if gram.len() == 3 => maps[usize::from(gram[0])].get(&gram[1..]),
            Self::Trigram(_) => None,
        }
    }

    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Single(map) => map.len(),
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
