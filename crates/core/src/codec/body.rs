use anyhow::{Context, Result as AnyhowResult};
use std::io::{Read, Seek, SeekFrom, Write};

#[derive(Debug)]
pub(super) enum DocumentStorage {
    Bytes(bytes::Bytes),
    File { file: std::fs::File, len: u64 },
}

#[derive(Debug)]
pub struct DocumentBody {
    pub(super) storage: DocumentStorage,
}

pub struct DocumentReader {
    storage: DocumentReaderStorage,
}

pub struct DocumentSpool {
    file: std::fs::File,
    len: u64,
}

enum DocumentReaderStorage {
    Bytes(std::io::Cursor<bytes::Bytes>),
    File { file: std::fs::File, offset: u64 },
}

impl Read for DocumentReader {
    fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
        match &mut self.storage {
            DocumentReaderStorage::Bytes(reader) => reader.read(bytes),
            DocumentReaderStorage::File { file, offset } => {
                #[cfg(unix)]
                let read = std::os::unix::fs::FileExt::read_at(file, bytes, *offset)?;
                #[cfg(windows)]
                let read = std::os::windows::fs::FileExt::seek_read(file, bytes, *offset)?;
                *offset = offset
                    .checked_add(u64::try_from(read).map_err(std::io::Error::other)?)
                    .ok_or_else(|| std::io::Error::other("document reader offset overflows"))?;
                Ok(read)
            }
        }
    }
}

impl DocumentBody {
    pub fn from_bytes(bytes: bytes::Bytes) -> Self {
        Self {
            storage: DocumentStorage::Bytes(bytes),
        }
    }

    pub(super) fn from_file(file: std::fs::File, len: u64) -> Self {
        Self {
            storage: DocumentStorage::File { file, len },
        }
    }

    pub fn len(&self) -> u64 {
        match &self.storage {
            DocumentStorage::Bytes(bytes) => {
                u64::try_from(bytes.len()).expect("document length fits u64")
            }
            DocumentStorage::File { len, .. } => *len,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn is_file(&self) -> bool {
        matches!(&self.storage, DocumentStorage::File { .. })
    }

    pub fn try_clone(&self) -> AnyhowResult<Self> {
        let storage = match &self.storage {
            DocumentStorage::Bytes(bytes) => DocumentStorage::Bytes(bytes.clone()),
            DocumentStorage::File { file, len } => DocumentStorage::File {
                file: file.try_clone()?,
                len: *len,
            },
        };
        Ok(Self { storage })
    }

    pub(super) fn inspect<T>(&self, inspect: impl FnOnce(&[u8]) -> T) -> AnyhowResult<T> {
        match &self.storage {
            DocumentStorage::Bytes(bytes) => Ok(inspect(bytes)),
            DocumentStorage::File { file, len: 0 } => {
                let _ = file;
                Ok(inspect(&[]))
            }
            DocumentStorage::File { file, .. } => {
                // SAFETY: DocumentBody owns the only mutable access to its private file.
                let map = unsafe { memmap2::MmapOptions::new().map(file)? };
                Ok(inspect(&map))
            }
        }
    }

    pub fn into_reader(self) -> DocumentReader {
        let storage = match self.storage {
            DocumentStorage::Bytes(bytes) => {
                DocumentReaderStorage::Bytes(std::io::Cursor::new(bytes))
            }
            DocumentStorage::File { file, .. } => DocumentReaderStorage::File { file, offset: 0 },
        };
        DocumentReader { storage }
    }

    pub fn into_bytes(self) -> AnyhowResult<bytes::Bytes> {
        match self.storage {
            DocumentStorage::Bytes(bytes) => Ok(bytes),
            DocumentStorage::File { file, .. } => {
                // SAFETY: the private temporary file cannot be mutated after this point.
                let map = unsafe { memmap2::MmapOptions::new().map(&file)? };
                #[cfg(unix)]
                map.advise(memmap2::Advice::Sequential)?;
                Ok(bytes::Bytes::from_owner(map))
            }
        }
    }
}

impl DocumentSpool {
    pub fn new(len: u64) -> AnyhowResult<Self> {
        let file = tempfile::tempfile()?;
        file.set_len(len)?;
        Ok(Self { file, len })
    }

    pub fn write_at(&mut self, start: u64, bytes: &[u8]) -> AnyhowResult<()> {
        let end = start
            .checked_add(u64::try_from(bytes.len())?)
            .context("document spool range overflows")?;
        anyhow::ensure!(end <= self.len, "document spool range is out of bounds");
        self.file.seek(SeekFrom::Start(start))?;
        self.file.write_all(bytes)?;
        Ok(())
    }

    pub fn finish(mut self) -> AnyhowResult<DocumentBody> {
        self.file.flush()?;
        self.file.seek(SeekFrom::Start(0))?;
        Ok(DocumentBody::from_file(self.file, self.len))
    }
}

pub(super) enum DecodeStorage {
    Memory(Vec<u8>),
    File(std::fs::File),
}

pub(super) struct DecodeWriter<'a> {
    key: &'a str,
    limit: Option<u64>,
    memory_limit: usize,
    len: u64,
    pub(super) storage: DecodeStorage,
}

impl<'a> DecodeWriter<'a> {
    pub(super) fn new(
        key: &'a str,
        limit: Option<u64>,
        capacity: usize,
        memory_limit: usize,
    ) -> Self {
        Self {
            key,
            limit,
            memory_limit,
            len: 0,
            storage: DecodeStorage::Memory(Vec::with_capacity(capacity.min(memory_limit))),
        }
    }

    pub(super) fn append(&mut self, bytes: &[u8]) -> AnyhowResult<()> {
        let len = self
            .len
            .checked_add(u64::try_from(bytes.len())?)
            .context("decoded length overflows")?;
        if let Some(limit) = self.limit {
            anyhow::ensure!(
                len <= limit,
                "decoded source {} exceeds {limit} bytes",
                self.key
            );
        }
        if matches!(self.storage, DecodeStorage::Memory(_))
            && usize::try_from(len).unwrap_or(usize::MAX) > self.memory_limit
        {
            let DecodeStorage::Memory(memory) =
                std::mem::replace(&mut self.storage, DecodeStorage::Memory(Vec::new()))
            else {
                unreachable!();
            };
            let mut file = tempfile::tempfile()?;
            file.write_all(&memory)?;
            self.storage = DecodeStorage::File(file);
        }
        match &mut self.storage {
            DecodeStorage::Memory(memory) => memory.extend_from_slice(bytes),
            DecodeStorage::File(file) => file.write_all(bytes)?,
        }
        self.len = len;
        Ok(())
    }

    pub(super) fn len(&self) -> u64 {
        self.len
    }

    pub(super) fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub(super) fn finish(self) -> AnyhowResult<DocumentBody> {
        match self.storage {
            DecodeStorage::Memory(memory) => Ok(DocumentBody::from_bytes(memory.into())),
            DecodeStorage::File(mut file) => {
                file.flush()?;
                file.seek(SeekFrom::Start(0))?;
                Ok(DocumentBody::from_file(file, self.len))
            }
        }
    }
}

impl Write for DecodeWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.append(bytes)
            .map(|()| bytes.len())
            .map_err(std::io::Error::other)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match &mut self.storage {
            DecodeStorage::Memory(_) => Ok(()),
            DecodeStorage::File(file) => file.flush(),
        }
    }
}
