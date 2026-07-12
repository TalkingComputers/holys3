use crate::scenarios::read_scenarios;
use anyhow::{Context, Result};
use regex::bytes::Regex;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

const ALPHABET: &[u8] = b"bcdfgjkmqrstvwxyz ";
const MIN_OBJECT_SIZE: usize = 256;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SeedDoc {
    pub key: String,
    pub path: PathBuf,
    pub bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SeedManifest {
    pub seed: u64,
    pub objects: usize,
    pub size: usize,
    pub total_bytes: u64,
    pub docs: Vec<SeedDoc>,
    pub expected_hits: BTreeMap<String, usize>,
}

struct DeterministicRng {
    state: u64,
}

impl DeterministicRng {
    fn new(seed: u64) -> DeterministicRng {
        DeterministicRng { state: seed }
    }

    fn byte(&mut self) -> u8 {
        self.state = self
            .state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        ALPHABET[((self.state >> 32) as usize) % ALPHABET.len()]
    }
}

pub(crate) fn corpus_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("runs/corpus")
}

pub(crate) fn objects_dir() -> PathBuf {
    corpus_dir().join("objects")
}

pub(crate) fn manifest_path() -> PathBuf {
    corpus_dir().join("manifest.json")
}

pub(crate) fn local_index_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("runs/local-index")
}

/// The canonical scenario list. Seed-time expected hits and run-time queries
/// both read this file, so a scenario is added in exactly one place.
pub(crate) fn scenarios_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("scenarios/queries.toml")
}

pub(crate) fn reports_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("runs")
}

pub(crate) fn latest_run_path() -> PathBuf {
    reports_dir().join("latest.json")
}

pub(crate) fn churn_run_path() -> PathBuf {
    reports_dir().join("churn.json")
}

pub(crate) fn read_manifest() -> Result<SeedManifest> {
    let file = std::fs::File::open(manifest_path())?;
    Ok(serde_json::from_reader(file)?)
}

pub(crate) fn write_seed(seed: u64, objects: usize, size: usize) -> Result<SeedManifest> {
    anyhow::ensure!(objects > 0, "objects must be greater than 0");
    anyhow::ensure!(
        size >= MIN_OBJECT_SIZE,
        "size must be at least {MIN_OBJECT_SIZE}"
    );
    let root = corpus_dir();
    if root.exists() {
        std::fs::remove_dir_all(&root)?;
    }
    let objects_root = objects_dir();
    std::fs::create_dir_all(&objects_root)?;
    let mut rng = DeterministicRng::new(seed);
    let mut docs = Vec::with_capacity(objects);
    let mut bytes_by_doc = Vec::with_capacity(objects);
    for index in 0..objects {
        let bytes = object_bytes(&mut rng, index, size);
        let key = format!("object-{index:06}.txt");
        let path = objects_root.join(&key);
        std::fs::write(&path, &bytes)?;
        docs.push(SeedDoc {
            key,
            path: path
                .strip_prefix(&root)
                .context("seed path outside corpus root")?
                .to_path_buf(),
            bytes: u64::try_from(bytes.len())?,
        });
        bytes_by_doc.push(bytes);
    }
    let expected_hits = expected_hits(&bytes_by_doc)?;
    let total_bytes = docs.iter().map(|doc| doc.bytes).sum();
    let manifest = SeedManifest {
        seed,
        objects,
        size,
        total_bytes,
        docs,
        expected_hits,
    };
    let file = std::fs::File::create(manifest_path())?;
    serde_json::to_writer_pretty(file, &manifest)?;
    Ok(manifest)
}

pub(crate) fn doc_path(doc: &SeedDoc) -> PathBuf {
    corpus_dir().join(&doc.path)
}

fn object_bytes(rng: &mut DeterministicRng, index: usize, size: usize) -> Vec<u8> {
    let mut bytes = (0..size)
        .map(|offset| if offset % 80 == 79 { b'\n' } else { rng.byte() })
        .collect::<Vec<_>>();
    if index.is_multiple_of(2) {
        write_token(&mut bytes, 32, b"needle");
    }
    if index.is_multiple_of(3) {
        write_token(&mut bytes, 96, b"longliteralbenchmarktoken");
    }
    if index.is_multiple_of(5) {
        write_token(&mut bytes, 64, b"alpha");
    }
    if index.is_multiple_of(7) {
        write_token(&mut bytes, 192, b"beta");
    }
    if index.is_multiple_of(11) {
        write_token(&mut bytes, 0, b"ANCHOR_START");
    }
    bytes
}

fn write_token(bytes: &mut [u8], offset: usize, token: &[u8]) {
    bytes[offset..offset + token.len()].copy_from_slice(token);
}

fn expected_hits(docs: &[Vec<u8>]) -> Result<BTreeMap<String, usize>> {
    let mut hits = BTreeMap::new();
    for scenario in read_scenarios(&scenarios_path())? {
        let re = Regex::new(&scenario.pattern)?;
        let count = docs.iter().filter(|bytes| re.is_match(bytes)).count();
        hits.insert(scenario.name, count);
    }
    Ok(hits)
}

pub(crate) fn remove_dir(path: &Path) -> Result<()> {
    if path.exists() {
        std::fs::remove_dir_all(path)?;
    }
    Ok(())
}
