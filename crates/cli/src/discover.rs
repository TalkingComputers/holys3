//! Index discovery for searches whose default index location is empty. An
//! index built at `s3://bucket/logs` serves a search of
//! `s3://bucket/logs/2026/07`: the reader validates that the index covers
//! the requested source and the search scopes itself to the narrower
//! prefix. So when `<prefix>/.seagrep` has no index, the parent prefixes
//! are probed up to the bucket root. Explicit `--index` locations are
//! remembered per source in a local cache file and used as the last
//! fallback, so repeated searches of a read-only source need no flags.
//!
//! Discovery runs only after the default location reports `IndexMissing`;
//! the happy path pays nothing.

use crate::{
    build_cache_dir, build_source_identity, open_index_storage, IndexArgs, IndexStorage, S3Source,
};
use anyhow::Result;
use seagrep_index::SegmentedReader;
use seagrep_s3::{build_index_namespace, list_prefix};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq)]
struct RememberedIndex {
    location: String,
    index_region: Option<String>,
    index_endpoint: Option<String>,
}

fn map_path() -> Result<PathBuf> {
    let mut path = seagrep_core::cache_home()?;
    path.push("seagrep");
    path.push("index-locations.json");
    Ok(path)
}

fn read_map() -> BTreeMap<String, RememberedIndex> {
    map_path()
        .ok()
        .and_then(|path| std::fs::read(path).ok())
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
        .unwrap_or_default()
}

/// Map key for one source prefix. Normalized with `list_prefix` like the
/// source identity, so `logs` and `logs/` name the same source.
fn source_key(source: &S3Source, prefix: &str) -> String {
    format!(
        "{}\u{0}{}\u{0}{}",
        source.endpoint,
        source.bucket,
        list_prefix(prefix.trim_matches('/'))
    )
}

/// Record an explicit `--index` location for this source. Best-effort: the
/// map is a convenience cache, never load-bearing, so failures are ignored.
pub(crate) fn remember_index(source: &S3Source, index: &IndexArgs) {
    let Some(location) = index.location.clone() else {
        return;
    };
    let entry = RememberedIndex {
        location,
        index_region: index.index_region.clone(),
        index_endpoint: index.index_endpoint.clone(),
    };
    let mut map = read_map();
    let key = source_key(source, &source.prefix);
    if map.get(&key) == Some(&entry) {
        return;
    }
    map.insert(key, entry);
    let Ok(path) = map_path() else {
        return;
    };
    let Some(dir) = path.parent() else {
        return;
    };
    if std::fs::create_dir_all(dir).is_err() {
        return;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700));
    }
    let Ok(bytes) = serde_json::to_vec_pretty(&map) else {
        return;
    };
    // A unique, freshly-created temp file (never a pre-existing path, so a
    // planted symlink cannot redirect the write), persisted over the map.
    let Ok(mut staged) = tempfile::Builder::new()
        .prefix(".index-locations-")
        .tempfile_in(dir)
    else {
        return;
    };
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = staged
            .as_file()
            .set_permissions(std::fs::Permissions::from_mode(0o600));
    }
    if std::io::Write::write_all(staged.as_file_mut(), &bytes).is_err() {
        return;
    }
    let _ = staged.persist(&path);
}

/// Parent prefixes of the searched prefix, nearest first, ending at the
/// bucket root. The searched prefix itself is excluded — its default
/// namespace already failed before discovery runs.
fn parent_chain(prefix: &str) -> Vec<String> {
    let mut chain = Vec::new();
    let mut current = prefix.trim_matches('/');
    while let Some((parent, _)) = current.rsplit_once('/') {
        chain.push(parent.to_owned());
        current = parent;
    }
    if !prefix.trim_matches('/').is_empty() {
        chain.push(String::new());
    }
    chain
}

fn storage_at(source: &S3Source, prefix: &str) -> Result<IndexStorage> {
    let root = build_index_namespace(prefix).trim_matches('/').to_owned();
    let endpoint = source.client.endpoint_identity();
    let cache = build_cache_dir(Some(&endpoint), &source.bucket, &root)?;
    Ok(IndexStorage {
        client: source.client.clone(),
        endpoint,
        bucket: source.bucket.clone(),
        root,
        cache,
    })
}

/// Find an index for a source whose default location is empty: parent
/// prefixes first, then the remembered location from an earlier `--index`
/// run. `None` means nothing usable was found anywhere.
pub(crate) fn discover_fallback(
    source: &S3Source,
    concurrency: usize,
) -> Result<Option<IndexStorage>> {
    let identity = build_source_identity(source);
    for candidate in parent_chain(&source.prefix) {
        let storage = storage_at(source, &candidate)?;
        let present = match storage.store().get_versioned("segments.bin") {
            Ok(present) => present.is_some(),
            // A parent the caller cannot read must not fail the search.
            Err(error) => {
                eprintln!(
                    "note: cannot probe index at {}: {error:#}",
                    storage.location()
                );
                false
            }
        };
        if !present {
            continue;
        }
        // The full open validates the format and that this index covers the
        // requested source; a parent index built for a sibling is skipped.
        match SegmentedReader::open(storage.store(), storage.cache(), &identity) {
            Ok(_) => {
                eprintln!(
                    "note: using index at {} (discovered at a parent prefix; pass --index to override)",
                    storage.location()
                );
                return Ok(Some(storage));
            }
            Err(error) => {
                eprintln!("note: skipping index at {}: {error:#}", storage.location());
            }
        }
    }
    // An --index recorded for a parent prefix covers this narrower search
    // too, so the lookup walks the same chain as the `.seagrep` probes.
    let map = read_map();
    let remembered = std::iter::once(source.prefix.clone())
        .chain(parent_chain(&source.prefix))
        .find_map(|prefix| map.get(&source_key(source, &prefix)).cloned());
    if let Some(entry) = remembered {
        // A remembered entry is a hint, never load-bearing: if it is stale,
        // unreachable, or no longer covers this source, fall through so the
        // caller reports the original missing-index error.
        let args = IndexArgs {
            location: Some(entry.location),
            index_region: entry.index_region,
            index_endpoint: entry.index_endpoint,
        };
        match open_index_storage(source, &args, concurrency) {
            Ok(storage) => {
                match SegmentedReader::open(storage.store(), storage.cache(), &identity) {
                    Ok(_) => {
                        eprintln!(
                            "note: using remembered index {} (recorded from an earlier --index run)",
                            storage.location()
                        );
                        return Ok(Some(storage));
                    }
                    Err(error) => eprintln!(
                        "note: ignoring remembered index {}: {error:#}",
                        storage.location()
                    ),
                }
            }
            Err(error) => eprintln!("note: ignoring remembered index: {error:#}"),
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_map_tolerates_missing_and_corrupt_files() {
        // Serializes on XDG_CACHE_HOME; combine both cases in one test to
        // avoid parallel-test races on the env var.
        let dir = tempfile::tempdir().unwrap();
        // SAFETY: test-only env mutation, single-threaded within this test.
        unsafe { std::env::set_var("XDG_CACHE_HOME", dir.path()) };
        assert!(read_map().is_empty(), "missing file reads as empty");
        let path = map_path().unwrap();
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, b"{not json").unwrap();
        assert!(read_map().is_empty(), "corrupt file reads as empty");
        std::fs::write(&path, b"[1, 2, 3]").unwrap();
        assert!(read_map().is_empty(), "wrong shape reads as empty");
        unsafe { std::env::remove_var("XDG_CACHE_HOME") };
    }

    #[test]
    fn parent_chain_walks_to_root() {
        assert_eq!(parent_chain("raw/rcaeval"), vec!["raw", ""]);
        assert_eq!(parent_chain("a/b/c"), vec!["a/b", "a", ""]);
        assert_eq!(parent_chain("logs"), vec![""]);
        assert!(parent_chain("").is_empty());
        assert_eq!(parent_chain("/a/b/"), vec!["a", ""]);
    }
}
