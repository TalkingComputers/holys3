use anyhow::{Context, Result};
use bytes::Bytes;
use fs4::FileExt;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

const MAGIC: &[u8; 8] = b"HS3CACHE";
const STATE_MAGIC: &[u8; 8] = b"HS3SIZE1";
const HEADER_LEN: usize = 48;

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
        let total = cache.evict()?;
        cache.write_total(Some(total))?;
        drop(lock);
        Ok(cache)
    }

    pub fn get(&self, key: &CacheKey<'_>) -> Result<Option<Bytes>> {
        let path = self.path(key);
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        if read_entry(&bytes).is_ok() {
            return Ok(Some(Bytes::from(bytes).slice(HEADER_LEN..)));
        }
        let lock = self.lock()?;
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        if read_entry(&bytes).is_ok() {
            return Ok(Some(Bytes::from(bytes).slice(HEADER_LEN..)));
        }
        let total = self.read_total()?;
        self.write_total(None)?;
        std::fs::remove_file(&path)?;
        self.write_total(Some(total.saturating_sub(u64::try_from(bytes.len())?)))?;
        drop(lock);
        Ok(None)
    }

    pub fn put(&self, key: &CacheKey<'_>, body: &Bytes) -> Result<()> {
        if u64::try_from(HEADER_LEN)? + u64::try_from(body.len())? > self.cap_bytes {
            return Ok(());
        }
        let lock = self.lock()?;
        let path = self.path(key);
        let old_len = match std::fs::metadata(&path) {
            Ok(metadata) => metadata.len(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
            Err(error) => return Err(error.into()),
        };
        let new_len = u64::try_from(HEADER_LEN)? + u64::try_from(body.len())?;
        let total = self.read_total()?;
        self.write_total(None)?;
        let mut temp = tempfile::NamedTempFile::new_in(&self.objects)?;
        temp.write_all(MAGIC)?;
        temp.write_all(&u64::try_from(body.len())?.to_le_bytes())?;
        temp.write_all(blake3::hash(body).as_bytes())?;
        temp.write_all(body)?;
        set_file_mode(temp.as_file())?;
        match std::fs::remove_file(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        temp.persist(&path).map_err(|error| error.error)?;
        let total = total
            .checked_sub(old_len)
            .and_then(|value| value.checked_add(new_len))
            .context("object cache size overflows u64")?;
        let total = if total > self.cap_bytes {
            self.evict()?
        } else {
            total
        };
        self.write_total(Some(total))?;
        drop(lock);
        Ok(())
    }

    pub(crate) fn get_each(
        &self,
        keys: &[CacheKey<'_>],
        concurrency: usize,
        consume: &mut dyn FnMut(usize, Option<Bytes>) -> Result<()>,
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
            std::sync::mpsc::sync_channel::<Result<(usize, Option<Bytes>)>>(workers * 2);
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
                            self.get(key)
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

    fn read_total(&self) -> Result<u64> {
        let path = self.root.join("cache.state");
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(error) => return Err(error.into()),
        };
        if bytes.len() == 16 && &bytes[..8] == STATE_MAGIC {
            return Ok(u64::from_le_bytes(bytes[8..].try_into()?));
        }
        self.remove_temps()?;
        let total = self.evict()?;
        self.write_total(Some(total))?;
        Ok(total)
    }

    fn write_total(&self, total: Option<u64>) -> Result<()> {
        let path = self.root.join("cache.state");
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(path)?;
        set_file_mode(&file)?;
        match total {
            Some(total) => {
                file.write_all(STATE_MAGIC)?;
                file.write_all(&total.to_le_bytes())?;
            }
            None => file.write_all(b"D")?,
        }
        file.flush()?;
        Ok(())
    }

    fn evict(&self) -> Result<u64> {
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
            entries.push((
                metadata.modified()?,
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
        if total <= self.cap_bytes {
            return Ok(total);
        }
        entries.sort_unstable_by(|left, right| (left.0, &left.1).cmp(&(right.0, &right.1)));
        for (_, _, path, bytes) in entries {
            if total <= self.cap_bytes {
                break;
            }
            std::fs::remove_file(path)?;
            total -= bytes;
        }
        Ok(total)
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
        std::thread::sleep(std::time::Duration::from_millis(2));
        cache.put(&key("v2"), &Bytes::from_static(b"56789"))?;
        assert_eq!(cache.get(&key("v1"))?, None);
        assert_eq!(cache.get(&key("v2"))?, Some(Bytes::from_static(b"56789")));
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
            seen.push((index, body));
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
