use anyhow::{Context, Result};
use bytes::Bytes;
use fs4::FileExt;
use holys3_core::{DocumentBody, DocumentSpool};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

const MAGIC: &[u8; 8] = b"HS3CACH2";
const STATE_MAGIC: &[u8; 8] = b"HS3STAT2";
const HEADER_LEN: usize = 56;
#[cfg(not(test))]
const CACHE_MEMORY_LIMIT: u64 = 8 * 1024 * 1024;
#[cfg(test)]
const CACHE_MEMORY_LIMIT: u64 = 1024;

enum CacheBody {
    Missing,
    Valid(DocumentBody),
    Invalid(u64),
}

#[derive(Clone, Copy)]
struct CacheState {
    total: u64,
    stamp: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectCacheConfig {
    pub root: PathBuf,
    pub cap_bytes: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct CacheKey<'a> {
    pub endpoint: &'a str,
    pub bucket: &'a str,
    pub key: &'a str,
    pub version: &'a str,
}

#[derive(Clone)]
pub struct ObjectCache {
    root: PathBuf,
    objects: PathBuf,
    cap_bytes: u64,
    lock_file: Arc<File>,
    gate: Arc<Mutex<()>>,
    #[cfg(test)]
    scans: Arc<AtomicUsize>,
}

impl ObjectCache {
    pub fn open(root: &Path, cap_bytes: u64) -> Result<ObjectCache> {
        anyhow::ensure!(cap_bytes > 0, "object cache cap must be greater than 0");
        let objects = root.join("objects");
        std::fs::create_dir_all(&objects)
            .with_context(|| format!("create object cache {}", root.display()))?;
        set_dir_mode(root)?;
        set_dir_mode(&objects)?;
        let lock_file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(root.join("cache.lock"))?;
        set_file_mode(&lock_file)?;
        let cache = ObjectCache {
            root: root.to_path_buf(),
            objects,
            cap_bytes,
            lock_file: Arc::new(lock_file),
            gate: Arc::new(Mutex::new(())),
            #[cfg(test)]
            scans: Arc::new(AtomicUsize::new(0)),
        };
        let lock = cache.lock()?;
        cache.remove_temps()?;
        let state = cache.evict()?;
        cache.write_state(Some(state))?;
        drop(lock);
        Ok(cache)
    }

    pub fn get(&self, key: &CacheKey<'_>) -> Result<Option<Bytes>> {
        self.get_body(key)?
            .map(DocumentBody::into_bytes)
            .transpose()
    }

    pub(crate) fn get_body(&self, key: &CacheKey<'_>) -> Result<Option<DocumentBody>> {
        let path = self.path(key);
        match read_cache_body(&path)? {
            CacheBody::Missing => return Ok(None),
            CacheBody::Valid(body) => return Ok(Some(body)),
            CacheBody::Invalid(_) => {}
        }
        let lock = self.lock()?;
        let len = match read_cache_body(&path)? {
            CacheBody::Missing => return Ok(None),
            CacheBody::Valid(body) => return Ok(Some(body)),
            CacheBody::Invalid(len) => len,
        };
        let mut state = self.read_state()?;
        self.write_state(None)?;
        std::fs::remove_file(&path)?;
        state.total = state.total.saturating_sub(len);
        self.write_state(Some(state))?;
        drop(lock);
        Ok(None)
    }

    pub fn put(&self, key: &CacheKey<'_>, body: &Bytes) -> Result<()> {
        self.put_body(key, DocumentBody::from_bytes(body.clone()))
    }

    pub(crate) fn put_body(&self, key: &CacheKey<'_>, body: DocumentBody) -> Result<()> {
        let body_len = body.len();
        if u64::try_from(HEADER_LEN)? + body_len > self.cap_bytes {
            return Ok(());
        }
        let lock = self.lock()?;
        let path = self.path(key);
        let old_len = match std::fs::metadata(&path) {
            Ok(metadata) => metadata.len(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
            Err(error) => return Err(error.into()),
        };
        let new_len = u64::try_from(HEADER_LEN)? + body_len;
        let mut state = self.read_state()?;
        self.write_state(None)?;
        state.stamp = state
            .stamp
            .checked_add(1)
            .context("object cache insertion sequence exhausted")?;
        let mut temp = tempfile::NamedTempFile::new_in(&self.objects)?;
        temp.write_all(MAGIC)?;
        temp.write_all(&body_len.to_le_bytes())?;
        temp.write_all(&[0; 32])?;
        temp.write_all(&state.stamp.to_le_bytes())?;
        let mut reader = body.into_reader();
        let mut hasher = blake3::Hasher::new();
        let mut written = 0u64;
        let mut chunk = [0u8; 64 * 1024];
        loop {
            let read = reader.read(&mut chunk)?;
            if read == 0 {
                break;
            }
            let bytes = &chunk[..read];
            hasher.update(bytes);
            temp.write_all(bytes)?;
            written = written
                .checked_add(u64::try_from(read)?)
                .context("object cache body length overflows")?;
        }
        anyhow::ensure!(written == body_len, "object cache body length changed");
        temp.seek(SeekFrom::Start(16))?;
        temp.write_all(hasher.finalize().as_bytes())?;
        temp.flush()?;
        set_file_mode(temp.as_file())?;
        match std::fs::remove_file(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        temp.persist(&path).map_err(|error| error.error)?;
        state.total = state
            .total
            .checked_sub(old_len)
            .and_then(|value| value.checked_add(new_len))
            .context("object cache size overflows u64")?;
        if state.total > self.cap_bytes {
            state = self.evict()?;
        }
        self.write_state(Some(state))?;
        drop(lock);
        Ok(())
    }

    pub(crate) fn get_each(
        &self,
        keys: &[CacheKey<'_>],
        concurrency: usize,
        consume: &mut dyn FnMut(usize, Option<DocumentBody>) -> Result<()>,
    ) -> Result<()> {
        anyhow::ensure!(
            concurrency > 0,
            "cache read concurrency must be greater than 0"
        );
        if keys.is_empty() {
            return Ok(());
        }
        let workers = concurrency.min(keys.len()).min(
            std::thread::available_parallelism()?
                .get()
                .saturating_mul(2),
        );
        let next = AtomicUsize::new(0);
        let cancelled = AtomicBool::new(false);
        let (sender, receiver) =
            std::sync::mpsc::sync_channel::<Result<(usize, Option<DocumentBody>)>>(workers * 2);
        let failure = std::thread::scope(|scope| {
            let next = &next;
            let cancelled = &cancelled;
            for _ in 0..workers {
                let sender = sender.clone();
                scope.spawn(move || {
                    while !cancelled.load(Ordering::Relaxed) {
                        let index = next.fetch_add(1, Ordering::Relaxed);
                        let Some(key) = keys.get(index) else {
                            break;
                        };
                        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            self.get_body(key)
                        }))
                        .unwrap_or_else(|_| {
                            Err(anyhow::anyhow!("an object cache worker panicked"))
                        });
                        match result {
                            Ok(body) => {
                                if sender.send(Ok((index, body))).is_err() {
                                    break;
                                }
                            }
                            Err(error) => {
                                cancelled.store(true, Ordering::Relaxed);
                                let _ = sender.send(Err(error));
                                break;
                            }
                        }
                    }
                });
            }
            drop(sender);
            let mut failure = None;
            while let Ok(result) = receiver.recv() {
                let result = result.and_then(|(index, body)| consume(index, body));
                if let Err(error) = result {
                    cancelled.store(true, Ordering::Relaxed);
                    failure = Some(error);
                    break;
                }
            }
            drop(receiver);
            failure
        });
        match failure {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn path(&self, key: &CacheKey<'_>) -> PathBuf {
        self.objects.join(hash_key(key))
    }

    fn lock(&self) -> Result<CacheLock<'_>> {
        let gate = self
            .gate
            .lock()
            .map_err(|_| anyhow::anyhow!("object cache lock panicked"))?;
        FileExt::lock(self.lock_file.as_ref())?;
        Ok(CacheLock {
            file: self.lock_file.as_ref(),
            gate,
        })
    }

    fn remove_temps(&self) -> Result<()> {
        for entry in std::fs::read_dir(&self.objects)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.len() != 64 || !name.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                std::fs::remove_file(entry.path())?;
            }
        }
        Ok(())
    }

    fn read_state(&self) -> Result<CacheState> {
        let path = self.root.join("cache.state");
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(error) => return Err(error.into()),
        };
        if bytes.len() == 24 && &bytes[..8] == STATE_MAGIC {
            return Ok(CacheState {
                total: u64::from_le_bytes(bytes[8..16].try_into()?),
                stamp: u64::from_le_bytes(bytes[16..24].try_into()?),
            });
        }
        self.remove_temps()?;
        let state = self.evict()?;
        self.write_state(Some(state))?;
        Ok(state)
    }

    fn write_state(&self, state: Option<CacheState>) -> Result<()> {
        let path = self.root.join("cache.state");
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(path)?;
        set_file_mode(&file)?;
        match state {
            Some(state) => {
                file.write_all(STATE_MAGIC)?;
                file.write_all(&state.total.to_le_bytes())?;
                file.write_all(&state.stamp.to_le_bytes())?;
            }
            None => file.write_all(b"D")?,
        }
        file.flush()?;
        Ok(())
    }

    fn evict(&self) -> Result<CacheState> {
        #[cfg(test)]
        self.scans.fetch_add(1, Ordering::Relaxed);
        let mut entries = Vec::new();
        for entry in std::fs::read_dir(&self.objects)? {
            let entry = entry?;
            let metadata = entry.metadata()?;
            if metadata.len() < u64::try_from(HEADER_LEN)? {
                std::fs::remove_file(entry.path())?;
                continue;
            }
            // FIFO order comes from the insertion stamp recorded in the
            // entry header; filesystem mtimes are too coarse to break ties.
            entries.push((
                read_stamp(&entry.path())?,
                entry.file_name(),
                entry.path(),
                metadata.len(),
            ));
        }
        let mut total = entries.iter().try_fold(0u64, |total, entry| {
            total
                .checked_add(entry.3)
                .context("object cache size overflows u64")
        })?;
        let stamp = entries.iter().fold(0, |stamp, entry| stamp.max(entry.0));
        if total <= self.cap_bytes {
            return Ok(CacheState { total, stamp });
        }
        entries.sort_unstable_by(|left, right| (left.0, &left.1).cmp(&(right.0, &right.1)));
        for (_, _, path, bytes) in entries {
            if total <= self.cap_bytes {
                break;
            }
            std::fs::remove_file(path)?;
            total -= bytes;
        }
        Ok(CacheState { total, stamp })
    }
}

struct CacheLock<'a> {
    file: &'a File,
    gate: MutexGuard<'a, ()>,
}

impl Drop for CacheLock<'_> {
    fn drop(&mut self) {
        let _ = FileExt::unlock(self.file);
        let _ = &self.gate;
    }
}

fn hash_key(key: &CacheKey<'_>) -> String {
    let mut hasher = blake3::Hasher::new();
    for field in [key.endpoint, key.bucket, key.key, key.version] {
        hasher.update(&(field.len() as u64).to_le_bytes());
        hasher.update(field.as_bytes());
    }
    hasher.finalize().to_hex().to_string()
}

fn read_cache_body(path: &Path) -> Result<CacheBody> {
    let metadata = match std::fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(CacheBody::Missing);
        }
        Err(error) => return Err(error.into()),
    };
    let file_len = metadata.len();
    if file_len <= u64::try_from(HEADER_LEN)? + CACHE_MEMORY_LIMIT {
        let bytes = match std::fs::read(path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(CacheBody::Missing);
            }
            Err(error) => return Err(error.into()),
        };
        return if read_entry(&bytes).is_ok() {
            Ok(CacheBody::Valid(DocumentBody::from_bytes(
                Bytes::from(bytes).slice(HEADER_LEN..),
            )))
        } else {
            Ok(CacheBody::Invalid(file_len))
        };
    }
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(CacheBody::Missing);
        }
        Err(error) => return Err(error.into()),
    };
    let mut header = [0u8; HEADER_LEN];
    if let Err(error) = file.read_exact(&mut header) {
        return if error.kind() == std::io::ErrorKind::UnexpectedEof {
            Ok(CacheBody::Invalid(file_len))
        } else {
            Err(error.into())
        };
    }
    if &header[..8] != MAGIC {
        return Ok(CacheBody::Invalid(file_len));
    }
    let body_len = u64::from_le_bytes(header[8..16].try_into()?);
    if u64::try_from(HEADER_LEN)?
        .checked_add(body_len)
        .is_none_or(|expected| expected != file_len)
    {
        return Ok(CacheBody::Invalid(file_len));
    }
    let mut body = DocumentSpool::new(body_len)?;
    let mut hasher = blake3::Hasher::new();
    let mut at = 0u64;
    let mut chunk = [0u8; 64 * 1024];
    while at < body_len {
        let read = usize::try_from((body_len - at).min(chunk.len() as u64))?;
        if let Err(error) = file.read_exact(&mut chunk[..read]) {
            return if error.kind() == std::io::ErrorKind::UnexpectedEof {
                Ok(CacheBody::Invalid(file_len))
            } else {
                Err(error.into())
            };
        }
        hasher.update(&chunk[..read]);
        body.write_at(at, &chunk[..read])?;
        at += u64::try_from(read)?;
    }
    if hasher.finalize().as_bytes() != &header[16..48] {
        return Ok(CacheBody::Invalid(file_len));
    }
    Ok(CacheBody::Valid(body.finish()?))
}

fn read_entry(bytes: &[u8]) -> Result<()> {
    anyhow::ensure!(bytes.len() >= HEADER_LEN, "cache entry header is truncated");
    anyhow::ensure!(&bytes[..8] == MAGIC, "cache entry magic is invalid");
    let len = u64::from_le_bytes(bytes[8..16].try_into()?);
    let body = &bytes[HEADER_LEN..];
    anyhow::ensure!(body.len() as u64 == len, "cache entry length is invalid");
    anyhow::ensure!(
        blake3::hash(body).as_bytes() == &bytes[16..48],
        "cache entry checksum is invalid"
    );
    Ok(())
}

fn read_stamp(path: &Path) -> Result<u64> {
    use std::io::Read;
    let mut header = [0u8; HEADER_LEN];
    File::open(path)?.read_exact(&mut header)?;
    Ok(u64::from_le_bytes(header[48..56].try_into()?))
}

#[cfg(unix)]
fn set_dir_mode(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    Ok(())
}

#[cfg(not(unix))]
fn set_dir_mode(path: &Path) -> Result<()> {
    let _ = path;
    Ok(())
}

#[cfg(unix)]
fn set_file_mode(file: &File) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    Ok(())
}

#[cfg(not(unix))]
fn set_file_mode(file: &File) -> Result<()> {
    let _ = file;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key<'a>(version: &'a str) -> CacheKey<'a> {
        CacheKey {
            endpoint: "http://127.0.0.1:9000",
            bucket: "bucket",
            key: "logs/a.zip",
            version,
        }
    }

    #[test]
    fn cache_round_trips_separates_versions_and_evicts() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let cache = ObjectCache::open(dir.path(), u64::try_from(HEADER_LEN + 5)?)?;
        cache.put(&key("v1"), &Bytes::from_static(b"1234"))?;
        assert_eq!(cache.get(&key("v1"))?, Some(Bytes::from_static(b"1234")));
        assert_eq!(cache.get(&key("v2"))?, None);
        cache.put(&key("v2"), &Bytes::from_static(b"56789"))?;
        assert_eq!(cache.get(&key("v1"))?, None);
        assert_eq!(cache.get(&key("v2"))?, Some(Bytes::from_static(b"56789")));
        Ok(())
    }

    #[test]
    fn cache_persists_monotonic_insertion_stamps() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let cap = u64::try_from(2 * (HEADER_LEN + 4))?;
        let first_cache = ObjectCache::open(dir.path(), cap)?;
        let second_cache = ObjectCache::open(dir.path(), cap)?;
        first_cache.put(&key("v1"), &Bytes::from_static(b"1234"))?;
        let first = read_stamp(&first_cache.path(&key("v1")))?;
        second_cache.put(&key("v2"), &Bytes::from_static(b"5678"))?;
        assert_eq!(
            read_stamp(&second_cache.path(&key("v2")))?,
            first
                .checked_add(1)
                .context("object cache insertion sequence exhausted")?
        );
        Ok(())
    }

    #[test]
    fn cache_skips_oversize_and_repairs_corruption() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let cache = ObjectCache::open(dir.path(), u64::try_from(HEADER_LEN + 4)?)?;
        cache.put(&key("large"), &Bytes::from_static(b"12345"))?;
        assert_eq!(cache.get(&key("large"))?, None);
        cache.put(&key("bad"), &Bytes::from_static(b"1234"))?;
        std::fs::write(cache.path(&key("bad")), b"bad")?;
        assert_eq!(cache.get(&key("bad"))?, None);
        Ok(())
    }

    #[test]
    fn cache_streams_large_bodies() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let bytes = Bytes::from(vec![b'x'; usize::try_from(CACHE_MEMORY_LIMIT)? + 1]);
        let cache = ObjectCache::open(
            dir.path(),
            u64::try_from(HEADER_LEN)? + u64::try_from(bytes.len())?,
        )?;
        cache.put(&key("large"), &bytes)?;
        let body = cache.get_body(&key("large"))?.context("cached body")?;
        assert!(body.is_file());
        assert_eq!(body.into_bytes()?, bytes);
        Ok(())
    }

    #[test]
    fn cache_counts_headers_and_repairs_truncated_entries_on_open() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let cap = u64::try_from(HEADER_LEN)?;
        let cache = ObjectCache::open(dir.path(), cap)?;
        cache.put(&key("v1"), &Bytes::new())?;
        cache.put(&key("v2"), &Bytes::new())?;
        assert_eq!(std::fs::read_dir(&cache.objects)?.count(), 1);
        let remaining = std::fs::read_dir(&cache.objects)?
            .next()
            .context("missing cache entry")??
            .path();
        std::fs::write(remaining, b"bad")?;
        let cache = ObjectCache::open(dir.path(), cap)?;
        assert_eq!(std::fs::read_dir(&cache.objects)?.count(), 0);
        Ok(())
    }

    #[test]
    fn cache_does_not_scan_directory_for_under_cap_inserts() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let cap = u64::try_from(100 * (HEADER_LEN + 4))?;
        let cache = ObjectCache::open(dir.path(), cap)?;
        for version in 0..100 {
            cache.put(&key(&version.to_string()), &Bytes::from_static(b"body"))?;
        }
        assert_eq!(cache.scans.load(Ordering::Relaxed), 1);
        Ok(())
    }

    #[test]
    fn cache_reads_entries_concurrently_and_reports_misses() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let cache = ObjectCache::open(dir.path(), u64::try_from(100 * (HEADER_LEN + 4))?)?;
        let versions = (0..100)
            .map(|index| format!("v{index}"))
            .collect::<Vec<_>>();
        for version in &versions {
            cache.put(&key(version), &Bytes::from_static(b"body"))?;
        }
        let mut keys = versions
            .iter()
            .map(|version| key(version))
            .collect::<Vec<_>>();
        keys.push(key("missing"));
        let mut seen = Vec::new();
        cache.get_each(&keys, 8, &mut |index, body| {
            seen.push((index, body.map(DocumentBody::into_bytes).transpose()?));
            Ok(())
        })?;
        seen.sort_unstable_by_key(|entry| entry.0);
        assert!(seen[..100]
            .iter()
            .all(|(_, body)| body.as_deref() == Some(b"body".as_slice())));
        assert_eq!(seen[100], (100, None));
        assert!(cache.get_each(&keys, 0, &mut |_, _| Ok(())).is_err());
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn cache_uses_owner_only_modes() -> Result<()> {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir()?;
        let cache = ObjectCache::open(dir.path(), u64::try_from(HEADER_LEN + 4)?)?;
        cache.put(&key("v1"), &Bytes::from_static(b"body"))?;
        assert_eq!(
            std::fs::metadata(dir.path())?.permissions().mode() & 0o777,
            0o700
        );
        assert_eq!(
            std::fs::metadata(cache.path(&key("v1")))?
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        Ok(())
    }
}
