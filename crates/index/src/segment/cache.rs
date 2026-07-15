use super::{hex_encode, segment_blob, sha256_hex};
use anyhow::{Context, Result};
use holys3_core::BlobStore;
use sha2::{Digest, Sha256};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

pub(super) fn cached_blob(
    store: &dyn BlobStore,
    cache_dir: &Path,
    seg_id: &str,
    name: &str,
    expected_len: u64,
    expected_hash: &str,
) -> Result<Vec<u8>> {
    let bytes = cached_bytes(cache_dir, seg_id, name, expected_hash, &|| {
        store
            .get(&segment_blob(seg_id, name))?
            .with_context(|| format!("segment blob {name} of {seg_id} missing from the store"))
    })?;
    anyhow::ensure!(
        bytes.len() as u64 == expected_len,
        "segment blob {name} of {seg_id} is {} bytes, expected {expected_len}",
        bytes.len()
    );
    Ok(bytes)
}

/// Read-through cache for small immutable segment artifacts: a disk hit is
/// trusted only if its SHA-256 matches, anything else is refetched through
/// `fetch`, verified, and written back atomically.
pub(crate) fn cached_bytes(
    cache_dir: &Path,
    seg_id: &str,
    name: &str,
    expected_hash: &str,
    fetch: &dyn Fn() -> Result<Vec<u8>>,
) -> Result<Vec<u8>> {
    let cache_path = cache_dir.join(seg_id).join(name);
    if let Some(bytes) = read_verified(&cache_path, expected_hash) {
        return Ok(bytes);
    }
    let bytes = fetch()?;
    anyhow::ensure!(
        sha256_hex(&[&bytes]) == expected_hash,
        "segment blob {name} of {seg_id} failed its SHA-256 check"
    );
    write_back(cache_dir, &cache_path, &bytes).ok();
    Ok(bytes)
}

/// Read a cache file and trust it only if its SHA-256 matches; a mismatch
/// deletes the file so the caller refetches.
pub(crate) fn read_verified(cache_path: &Path, expected_hash: &str) -> Option<Vec<u8>> {
    let bytes = std::fs::read(cache_path).ok()?;
    set_cache_path_mode(cache_path).ok();
    if sha256_hex(&[&bytes]) == expected_hash {
        return Some(bytes);
    }
    std::fs::remove_file(cache_path).ok();
    None
}

/// Atomically publish verified bytes into the cache.
pub(crate) fn write_back(cache_dir: &Path, cache_path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = cache_path.parent().context("cache path has no parent")?;
    std::fs::create_dir_all(parent)?;
    set_cache_dir_mode(cache_dir)?;
    set_cache_dir_mode(parent)?;
    let mut temp = tempfile::NamedTempFile::new_in(parent)?;
    set_cache_file_mode(temp.as_file())?;
    temp.write_all(bytes)?;
    temp.persist(cache_path)?;
    Ok(())
}

fn sha256_file(path: &Path) -> Result<String> {
    let mut file = std::fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut bytes = [0u8; 64 * 1024];
    loop {
        let read = file.read(&mut bytes)?;
        if read == 0 {
            return Ok(hex_encode(&hasher.finalize()));
        }
        hasher.update(&bytes[..read]);
    }
}

fn is_cached_file(path: &Path, expected_len: u64, expected_hash: &str) -> Result<bool> {
    match std::fs::metadata(path) {
        Ok(metadata) if metadata.len() == expected_len => Ok(sha256_file(path)? == expected_hash),
        Ok(_) => Ok(false),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn verified_path(path: &Path) -> Result<PathBuf> {
    let mut name = path
        .file_name()
        .context("cached file path has no name")?
        .to_os_string();
    name.push(".verified");
    Ok(path.with_file_name(name))
}

fn verification_text(metadata: &std::fs::Metadata, expected_hash: &str) -> Result<String> {
    let modified = metadata
        .modified()?
        .duration_since(std::time::UNIX_EPOCH)
        .context("cached file modification time predates the Unix epoch")?;
    Ok(format!(
        "{expected_hash}\n{}:{}",
        modified.as_secs(),
        modified.subsec_nanos()
    ))
}

fn is_verified_file(path: &Path, expected_len: u64, expected_hash: &str) -> Result<bool> {
    let metadata = match std::fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error.into()),
    };
    if metadata.len() != expected_len {
        return Ok(false);
    }
    let expected = verification_text(&metadata, expected_hash)?;
    match std::fs::read_to_string(verified_path(path)?) {
        Ok(marker) => Ok(marker == expected),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn mark_verified(path: &Path, expected_hash: &str) -> Result<()> {
    let marker = verified_path(path)?;
    let mut file = std::fs::File::create(marker)?;
    set_cache_file_mode(&file)?;
    file.write_all(verification_text(&std::fs::metadata(path)?, expected_hash)?.as_bytes())?;
    file.sync_all()?;
    Ok(())
}

pub(super) fn cached_file(
    store: &dyn BlobStore,
    cache_dir: &Path,
    seg_id: &str,
    name: &str,
    expected_len: u64,
    expected_hash: &str,
) -> Result<PathBuf> {
    let cache_path = cache_dir.join(seg_id).join(name);
    if is_verified_file(&cache_path, expected_len, expected_hash)? {
        set_cache_path_mode(&cache_path).ok();
        set_cache_path_mode(&verified_path(&cache_path)?).ok();
        return Ok(cache_path);
    }
    if is_cached_file(&cache_path, expected_len, expected_hash)? {
        set_cache_path_mode(&cache_path).ok();
        mark_verified(&cache_path, expected_hash)?;
        return Ok(cache_path);
    }
    std::fs::remove_file(&cache_path).ok();
    std::fs::remove_file(verified_path(&cache_path)?).ok();
    let parent = cache_path
        .parent()
        .with_context(|| format!("cache path has no parent: {}", cache_path.display()))?;
    std::fs::create_dir_all(parent)?;
    set_cache_dir_mode(cache_dir)?;
    set_cache_dir_mode(parent)?;
    let mut temp = tempfile::NamedTempFile::new_in(parent)?;
    set_cache_file_mode(temp.as_file())?;
    store.get_file(
        &segment_blob(seg_id, name),
        temp.as_file_mut(),
        expected_len,
    )?;
    anyhow::ensure!(
        temp.as_file().metadata()?.len() == expected_len,
        "segment blob {name} of {seg_id} has the wrong length"
    );
    anyhow::ensure!(
        sha256_file(temp.path())? == expected_hash,
        "segment blob {name} of {seg_id} failed its SHA-256 check"
    );
    temp.as_file().sync_all()?;
    if let Err(error) = temp.persist(&cache_path) {
        if !is_cached_file(&cache_path, expected_len, expected_hash)? {
            return Err(error.error.into());
        }
    }
    mark_verified(&cache_path, expected_hash)?;
    Ok(cache_path)
}

pub(super) fn map_file(path: &Path) -> Result<memmap2::Mmap> {
    let file = std::fs::File::open(path)?;
    Ok(unsafe { memmap2::MmapOptions::new().map(&file)? })
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
