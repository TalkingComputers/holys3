use crate::scenarios::read_scenarios;
use anyhow::{Context, Result};
use clap::ValueEnum;
use regex::bytes::Regex;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

const ALPHABET: &[u8] = b"bcdfgjkmqrstvwxyz ";
const MIN_OBJECT_SIZE: usize = 256;
const MIN_PROSE_OBJECT_SIZE: usize = 2048;
const PROSE_LINE_WIDTH: usize = 72;

/// Planted needles for the prose corpus. A unit test pins the scenario file
/// to these constants so the query patterns cannot drift from the corpus.
pub(crate) const PROSE_PHRASE: &str = "the people of the north came down to the great water";
pub(crate) const PROSE_PHRASE_EVERY: usize = 50;
pub(crate) const PROSE_RARE_WORD: &str = "xylophone";
pub(crate) const PROSE_RARE_EVERY: usize = 250;

/// Common English words in rough frequency order; Zipf sampling over this
/// list gives every document the shared-trigram profile of real prose.
#[rustfmt::skip]
const PROSE_WORDS: &[&str] = &[
    "the", "of", "and", "a", "to", "in", "is", "was", "he", "for", "it", "with", "as", "his", "on",
    "be", "at", "by", "had", "not", "are", "but", "from", "or", "have", "an", "they", "which",
    "one", "you", "were", "her", "all", "she", "there", "would", "their", "we", "him", "been",
    "has", "when", "who", "will", "more", "no", "if", "out", "so", "said", "what", "up", "its",
    "about", "into", "than", "them", "can", "only", "other", "new", "some", "could", "time",
    "these", "two", "may", "then", "do", "first", "any", "my", "now", "such", "like", "our",
    "over", "man", "me", "even", "most", "made", "after", "also", "did", "many", "before", "must",
    "through", "back", "years", "where", "much", "your", "way", "well", "down", "should",
    "because", "each", "just", "those", "people", "how", "too", "little", "state", "good", "very",
    "make", "world", "still", "own", "see", "men", "work", "long", "get", "here", "between",
    "both", "life", "being", "under", "never", "day", "same", "another", "know", "while", "last",
    "might", "us", "great", "old", "year", "off", "come", "since", "against", "go", "came",
    "right", "used", "take", "three", "himself", "few", "house", "use", "during", "without",
    "again", "place", "around", "however", "home", "small", "found", "thought", "went", "say",
    "part", "once", "high", "general", "upon", "school", "every", "does", "got", "united", "left",
    "number", "course", "war", "until", "always", "away", "something", "fact", "though", "water",
    "less", "public", "put", "think", "almost", "hand", "enough", "far", "took", "head", "yet",
    "government", "system", "better", "set", "told", "nothing", "night", "end", "why", "called",
    "real", "eyes", "find", "going", "look", "asked", "later", "knew", "point", "next", "city",
    "business", "give", "group", "toward", "young", "days", "let", "room", "within", "children",
    "side", "social", "given", "order", "often", "several", "national", "important", "rather",
    "large", "case", "big", "need", "four", "felt", "along", "among", "best", "turned", "power",
    "possible", "although", "done", "north", "open", "god", "kind", "began", "different", "door",
    "keep", "means", "others", "true", "white", "form", "face", "second", "certain", "seemed",
    "story", "am", "country", "help", "show", "light",
];

struct ZipfSampler {
    cumulative: Vec<f64>,
    total: f64,
}

impl ZipfSampler {
    fn new() -> ZipfSampler {
        let mut cumulative = Vec::with_capacity(PROSE_WORDS.len());
        let mut total = 0.0;
        for rank in 0..PROSE_WORDS.len() {
            total += 1.0 / (rank + 1) as f64;
            cumulative.push(total);
        }
        ZipfSampler { cumulative, total }
    }

    fn word(&self, rng: &mut DeterministicRng) -> &'static str {
        let unit = (rng.next_u64() >> 11) as f64 / (1u64 << 53) as f64;
        let target = unit * self.total;
        let rank = self.cumulative.partition_point(|bound| *bound <= target);
        PROSE_WORDS[rank.min(PROSE_WORDS.len() - 1)]
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CorpusKind {
    Random,
    Prose,
}

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
    pub corpus: CorpusKind,
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

    fn next_u64(&mut self) -> u64 {
        self.state = self
            .state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.state
    }

    fn byte(&mut self) -> u8 {
        ALPHABET[((self.next_u64() >> 32) as usize) % ALPHABET.len()]
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

/// The canonical scenario list for a corpus kind. Seed-time expected hits and
/// run-time queries both read this file, so a scenario is added in exactly one
/// place.
pub(crate) fn scenarios_path(corpus: CorpusKind) -> PathBuf {
    let file = match corpus {
        CorpusKind::Random => "scenarios/queries.toml",
        CorpusKind::Prose => "scenarios/prose.toml",
    };
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(file)
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

pub(crate) fn write_seed(
    seed: u64,
    objects: usize,
    size: usize,
    corpus: CorpusKind,
) -> Result<SeedManifest> {
    anyhow::ensure!(objects > 0, "objects must be greater than 0");
    let min_size = match corpus {
        CorpusKind::Random => MIN_OBJECT_SIZE,
        CorpusKind::Prose => MIN_PROSE_OBJECT_SIZE,
    };
    anyhow::ensure!(size >= min_size, "size must be at least {min_size}");
    let root = corpus_dir();
    if root.exists() {
        std::fs::remove_dir_all(&root)?;
    }
    let objects_root = objects_dir();
    std::fs::create_dir_all(&objects_root)?;
    let mut rng = DeterministicRng::new(seed);
    let zipf = ZipfSampler::new();
    let mut docs = Vec::with_capacity(objects);
    let mut bytes_by_doc = Vec::with_capacity(objects);
    for index in 0..objects {
        let bytes = match corpus {
            CorpusKind::Random => object_bytes(&mut rng, index, size),
            CorpusKind::Prose => prose_object_bytes(&mut rng, &zipf, index, size),
        };
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
    let expected_hits = expected_hits(corpus, &bytes_by_doc)?;
    let total_bytes = docs.iter().map(|doc| doc.bytes).sum();
    let manifest = SeedManifest {
        seed,
        objects,
        size,
        corpus,
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

/// Zipf-sampled words hard-wrapped at `PROSE_LINE_WIDTH`, mimicking Gutenberg
/// prose: common trigrams appear in every document while any specific word
/// sequence stays rare. Planted needles land on their own line (the phrase)
/// or in the word stream (the rare word) at deterministic offsets. Documents
/// end on a word boundary, so they run up to one word past `size`; the
/// manifest records actual byte counts.
fn prose_object_bytes(
    rng: &mut DeterministicRng,
    zipf: &ZipfSampler,
    index: usize,
    size: usize,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(size + PROSE_LINE_WIDTH);
    let mut line_len = 0usize;
    let mut plant_phrase = index.is_multiple_of(PROSE_PHRASE_EVERY);
    let mut plant_rare = index.is_multiple_of(PROSE_RARE_EVERY);
    while out.len() < size {
        if plant_phrase && out.len() >= 64 {
            push_prose_line(&mut out, &mut line_len, PROSE_PHRASE);
            plant_phrase = false;
            continue;
        }
        if plant_rare && out.len() >= 1024 {
            push_prose_word(&mut out, &mut line_len, PROSE_RARE_WORD);
            plant_rare = false;
            continue;
        }
        push_prose_word(&mut out, &mut line_len, zipf.word(rng));
    }
    out.push(b'\n');
    out
}

fn push_prose_word(out: &mut Vec<u8>, line_len: &mut usize, word: &str) {
    if *line_len > 0 {
        if *line_len + 1 + word.len() > PROSE_LINE_WIDTH {
            out.push(b'\n');
            *line_len = 0;
        } else {
            out.push(b' ');
            *line_len += 1;
        }
    }
    out.extend_from_slice(word.as_bytes());
    *line_len += word.len();
}

fn push_prose_line(out: &mut Vec<u8>, line_len: &mut usize, line: &str) {
    if *line_len > 0 {
        out.push(b'\n');
    }
    out.extend_from_slice(line.as_bytes());
    out.push(b'\n');
    *line_len = 0;
}

fn expected_hits(corpus: CorpusKind, docs: &[Vec<u8>]) -> Result<BTreeMap<String, usize>> {
    let mut hits = BTreeMap::new();
    for scenario in read_scenarios(&scenarios_path(corpus))? {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    fn prose_doc(seed: u64, index: usize, size: usize) -> Vec<u8> {
        let mut rng = DeterministicRng::new(seed);
        let zipf = ZipfSampler::new();
        prose_object_bytes(&mut rng, &zipf, index, size)
    }

    #[test]
    fn prose_words_are_unique() {
        let unique = PROSE_WORDS.iter().collect::<BTreeSet<_>>();
        assert_eq!(unique.len(), PROSE_WORDS.len());
    }

    #[test]
    fn prose_generation_is_deterministic() {
        assert_eq!(prose_doc(7, 3, 4096), prose_doc(7, 3, 4096));
        assert_ne!(prose_doc(7, 3, 4096), prose_doc(8, 3, 4096));
    }

    #[test]
    fn prose_lines_stay_within_width() {
        let doc = prose_doc(1, 1, 8192);
        for line in doc.split(|byte| *byte == b'\n') {
            assert!(line.len() <= PROSE_LINE_WIDTH, "line of {}", line.len());
        }
        assert_eq!(doc.last(), Some(&b'\n'));
    }

    #[test]
    fn prose_plants_needles_on_schedule() {
        let planted = prose_doc(1, 0, 8192);
        let unplanted = prose_doc(1, 1, 8192);
        let phrase_line = format!("\n{PROSE_PHRASE}\n");
        let contains =
            |doc: &[u8], token: &str| doc.windows(token.len()).any(|w| w == token.as_bytes());
        assert!(contains(&planted, &phrase_line));
        assert!(contains(&planted, PROSE_RARE_WORD));
        assert!(!contains(&unplanted, PROSE_PHRASE));
        assert!(!contains(&unplanted, PROSE_RARE_WORD));
    }

    #[test]
    fn prose_scenarios_match_planted_constants() {
        let scenarios = read_scenarios(&scenarios_path(CorpusKind::Prose)).unwrap();
        let by_name = scenarios
            .iter()
            .map(|scenario| (scenario.name.as_str(), scenario.pattern.as_str()))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(by_name["planted_phrase"], PROSE_PHRASE);
        assert_eq!(by_name["rare_word"], PROSE_RARE_WORD);
        assert!(PROSE_WORDS.contains(&by_name["common_word"]));
        let unplanted = by_name["unplanted_phrase"];
        assert_ne!(unplanted, PROSE_PHRASE);
        for word in unplanted.split(' ') {
            assert!(PROSE_WORDS.contains(&word), "{word} is not a vocab word");
        }
    }
}
