#![cfg_attr(docsrs, feature(doc_auto_cfg))]
//! Shared types, gram extraction, storage traits, and scan verification.

use anyhow::{Context, Result as AnyhowResult};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::path::PathBuf;

pub type DocId = u32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Codec {
    Raw,
    Gzip,
    Zstd,
    Bzip2,
    SnappyFrame,
    Lz4Frame,
    /// `lz4 -l` output (magic 02 21 4c 18): detected so it fails loudly
    /// instead of being grepped as compressed bytes; not decodable here.
    Lz4Legacy,
    Xz,
    /// Columnar file; decoded as a JSON Lines projection (one row per line).
    Parquet,
    /// Avro Object Container File; decoded as JSON Lines (one record per line).
    Avro,
}

/// Snappy framing format stream identifier (`framing_format.txt` section 4.1).
const SNAPPY_FRAME_MAGIC: [u8; 10] = [0xff, 0x06, 0x00, 0x00, b's', b'N', b'a', b'P', b'p', b'Y'];

/// Skippable frame (5? 2a 4d 18): byte-identical between zstd and lz4 BY
/// DESIGN, so frames at the start identify nothing. Returns the offset of
/// the first non-skippable byte, or `None` when a frame header lies or runs
/// past EOF (not a valid frame flow).
fn skip_skippable_frames(bytes: &[u8]) -> Option<usize> {
    let mut at = 0usize;
    while let Some(rest) = bytes.get(at..) {
        if rest.len() >= 8 && rest[0] & 0xf0 == 0x50 && rest[1..4] == [0x2a, 0x4d, 0x18] {
            let size = u32::from_le_bytes(rest[4..8].try_into().expect("4 bytes")) as usize;
            match at.checked_add(8 + size) {
                Some(next) if next <= bytes.len() => at = next,
                _ => return None,
            }
        } else {
            break;
        }
    }
    Some(at)
}

/// Detect format by magic bytes; key extensions are not trusted. Every check
/// is an exact byte comparison from the format's official spec — an object
/// either matches a magic or it is raw bytes.
///
/// - gzip: 1f 8b 08 (deflate is the only method the format defines)
/// - zstd: 28 b5 2f fd
/// - bzip2: `BZh` + level `1`..`9` + block magic 0x314159265359 OR
///   end-of-stream magic 0x177245385090 (empty input) — the 6-byte tail is
///   what makes a text file starting with literal `BZh1` undetectable as bzip2
/// - snappy framing format: ff 06 00 00 "sNaPpY" (raw snappy has no magic at
///   all and is deliberately unsupported as an object format)
/// - lz4 frame: 04 22 4d 18; legacy frame 02 21 4c 18 detected as unsupported
/// - xz: fd 37 7a 58 5a 00
/// - parquet: `PAR1` leading AND trailing (the header alone is printable
///   ASCII and could open a text file)
/// - avro object container: `Obj` 01
///
/// Skippable frames are shared between zstd and lz4: identity comes from the
/// first non-skippable frame after them.
pub fn detect_codec(bytes: &[u8]) -> Codec {
    let Some(at) = skip_skippable_frames(bytes) else {
        return Codec::Raw;
    };
    if at > 0 {
        // After skippable frames only zstd/lz4 frames are legal; a flow of
        // ONLY skippable frames decodes to empty under both — zstd wins.
        let rest = &bytes[at..];
        return if rest.is_empty() || rest.starts_with(&[0x28, 0xb5, 0x2f, 0xfd]) {
            Codec::Zstd
        } else if rest.starts_with(&[0x04, 0x22, 0x4d, 0x18]) {
            Codec::Lz4Frame
        } else {
            Codec::Raw
        };
    }
    if bytes.starts_with(&[0x1f, 0x8b, 0x08]) {
        Codec::Gzip
    } else if bytes.starts_with(&[0x28, 0xb5, 0x2f, 0xfd]) {
        Codec::Zstd
    } else if is_bzip2(bytes) {
        Codec::Bzip2
    } else if bytes.starts_with(&SNAPPY_FRAME_MAGIC) {
        Codec::SnappyFrame
    } else if bytes.starts_with(&[0x04, 0x22, 0x4d, 0x18]) {
        Codec::Lz4Frame
    } else if bytes.starts_with(&[0x02, 0x21, 0x4c, 0x18]) {
        Codec::Lz4Legacy
    } else if bytes.starts_with(&[0xfd, 0x37, 0x7a, 0x58, 0x5a, 0x00]) {
        Codec::Xz
    } else if bytes.len() >= 12 && bytes.starts_with(b"PAR1") && bytes.ends_with(b"PAR1") {
        Codec::Parquet
    } else if bytes.starts_with(&[b'O', b'b', b'j', 0x01]) {
        Codec::Avro
    } else {
        Codec::Raw
    }
}

fn is_bzip2(bytes: &[u8]) -> bool {
    bytes.len() >= 10
        && bytes.starts_with(b"BZh")
        && (0x31..=0x39).contains(&bytes[3])
        && (bytes[4..10] == [0x31, 0x41, 0x59, 0x26, 0x53, 0x59]
            || bytes[4..10] == [0x17, 0x72, 0x45, 0x38, 0x50, 0x90])
}

/// Drain a streaming decoder with trailing-garbage salvage: AWS log
/// deliveries concatenate members and sometimes pad, so a decode error after
/// some bytes already decoded keeps the decoded text with a warning — for
/// grep, partial coverage beats dropping the object.
fn read_salvaging(key: &str, label: &str, reader: &mut dyn std::io::Read) -> AnyhowResult<Vec<u8>> {
    let mut out = Vec::new();
    match reader.read_to_end(&mut out) {
        Ok(_) => Ok(out),
        Err(err) if !out.is_empty() => {
            eprintln!(
                "warning: {key}: {label} stream ends in garbage ({err}); \
                 searching the {} bytes that decoded",
                out.len()
            );
            Ok(out)
        }
        Err(err) => {
            Err(anyhow::Error::new(err).context(format!("{label} decode failed for {key}")))
        }
    }
}

/// One row per line, schema column order, explicit nulls, RFC3339 timestamps,
/// hex binary — arrow-json's documented deterministic rendering.
fn parquet_to_json_lines(key: &str, bytes: Vec<u8>) -> AnyhowResult<Vec<u8>> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let context = || format!("parquet decode failed for {key}");
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
        .with_context(context)?
        .build()
        .with_context(context)?;
    let mut writer = arrow_json::writer::WriterBuilder::new()
        .with_explicit_nulls(true)
        .build::<_, arrow_json::writer::LineDelimited>(Vec::new());
    for batch in reader {
        writer
            .write(&batch.with_context(context)?)
            .with_context(context)?;
    }
    writer.finish().with_context(context)?;
    Ok(writer.into_inner())
}

/// One record per line via the crate's own Value -> JSON conversion.
fn avro_to_json_lines(key: &str, bytes: &[u8]) -> AnyhowResult<Vec<u8>> {
    let context = || format!("avro decode failed for {key}");
    let reader = apache_avro::Reader::new(bytes).with_context(context)?;
    let mut out = Vec::new();
    for value in reader {
        let json =
            serde_json::Value::try_from(value.with_context(context)?).with_context(context)?;
        out.extend_from_slice(json.to_string().as_bytes());
        out.push(b'\n');
    }
    Ok(out)
}

/// Transparently decode an object body into searchable text. Compressed
/// objects decompress (multi-member/multi-stream concatenations included);
/// columnar/container formats project to JSON Lines so the same bytes are
/// indexed and verified. Detection is by magic bytes only.
pub fn decode_body(key: &str, bytes: Vec<u8>) -> AnyhowResult<Vec<u8>> {
    match detect_codec(&bytes) {
        Codec::Raw => Ok(bytes),
        Codec::Gzip => read_salvaging(
            key,
            "gzip",
            &mut flate2::read::MultiGzDecoder::new(bytes.as_slice()),
        ),
        Codec::Zstd => zstd::stream::decode_all(bytes.as_slice())
            .with_context(|| format!("zstd decode failed for {key}")),
        Codec::Bzip2 => read_salvaging(
            key,
            "bzip2",
            &mut bzip2::read::MultiBzDecoder::new(bytes.as_slice()),
        ),
        Codec::SnappyFrame => read_salvaging(
            key,
            "snappy",
            &mut snap::read::FrameDecoder::new(bytes.as_slice()),
        ),
        Codec::Lz4Frame => {
            // identity may sit after skippable frames the decoder won't skip
            let at = skip_skippable_frames(&bytes).expect("validated by detect_codec");
            let mut decoder = lz4_flex::frame::FrameDecoder::new(&bytes[at..]);
            let mut out = Vec::new();
            loop {
                // each call decodes one frame; Ok(0) = input exhausted
                match std::io::Read::read_to_end(&mut decoder, &mut out) {
                    Ok(0) => return Ok(out),
                    Ok(_) => {}
                    Err(err) if !out.is_empty() => {
                        eprintln!(
                            "warning: {key}: lz4 stream ends in garbage ({err}); \
                             searching the {} bytes that decoded",
                            out.len()
                        );
                        return Ok(out);
                    }
                    Err(err) => {
                        return Err(
                            anyhow::Error::new(err).context(format!("lz4 decode failed for {key}"))
                        )
                    }
                }
            }
        }
        Codec::Lz4Legacy => anyhow::bail!(
            "{key} is an lz4 LEGACY frame (`lz4 -l` output), which holys3 does \
             not decode; re-compress with the default lz4 frame format"
        ),
        Codec::Xz => read_salvaging(
            key,
            "xz",
            &mut liblzma::read::XzDecoder::new_multi_decoder(bytes.as_slice()),
        ),
        Codec::Parquet => parquet_to_json_lines(key, bytes),
        Codec::Avro => avro_to_json_lines(key, &bytes),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Strategy {
    Trigram,
    Sparse,
}

/// Every overlapping 3-byte window as raw bytes (sorted, deduped). <3 bytes => empty.
/// Windows pack big-endian into u32 so sort+dedup run over integers (u32
/// order == lexicographic byte order) and only distinct grams allocate.
pub fn trigram_grams_bytes(data: &[u8]) -> Vec<Vec<u8>> {
    let mut packed: Vec<u32> = data
        .windows(3)
        .map(|w| u32::from(w[0]) << 16 | u32::from(w[1]) << 8 | u32::from(w[2]))
        .collect();
    packed.sort_unstable();
    packed.dedup();
    packed
        .into_iter()
        .map(|g| vec![(g >> 16) as u8, (g >> 8) as u8, g as u8])
        .collect()
}

/// Stable u64 hash of an n-gram's bytes. Deterministic across runs/platforms
/// (used as the on-disk + in-memory gram key).
pub fn hash_ngram(gram: &[u8]) -> u64 {
    rapidhash::v3::rapidhash_v3(gram)
}

/// Deterministic weight of an adjacent byte pair. Drives sparse-ngram
/// boundary selection. Only affects selectivity, never correctness.
pub(crate) fn pair_weight(a: u8, b: u8) -> u32 {
    rapidhash::v3::rapidhash_v3(&[a, b]) as u32
}

/// `build_all` as raw gram byte strings (sorted, deduped). Index-time.
pub fn sparse_grams_all_bytes(data: &[u8]) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    if data.len() < 2 {
        return out;
    }
    let weights: Vec<u32> = data.windows(2).map(|w| pair_weight(w[0], w[1])).collect();
    let n = weights.len();
    for i in 0..n {
        out.push(data[i..i + 2].to_vec());
        let mut interior_max: u32 = 0;
        for j in (i + 1)..n {
            if j > i + 1 {
                interior_max = interior_max.max(weights[j - 1]);
            }
            if interior_max >= weights[i] {
                break;
            }
            if weights[j] > interior_max {
                out.push(data[i..j + 2].to_vec());
            }
        }
    }
    out.sort_unstable();
    out.dedup();
    out
}

/// `build_covering` as raw gram byte strings (sorted, deduped). Query-time.
pub fn sparse_grams_covering_bytes(data: &[u8]) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    if data.len() < 2 {
        return out;
    }
    let weights: Vec<u32> = data.windows(2).map(|w| pair_weight(w[0], w[1])).collect();
    let mut stack: Vec<usize> = Vec::new();
    for i in 0..weights.len() {
        while let Some(&top) = stack.last() {
            if weights[top] <= weights[i] {
                out.push(data[top..i + 2].to_vec());
                if weights[top] == weights[i] {
                    stack.pop();
                    break;
                }
                stack.pop();
            } else {
                break;
            }
        }
        stack.push(i);
    }
    while stack.len() > 1 {
        let top = stack.pop().unwrap();
        if let Some(&prev) = stack.last() {
            out.push(data[prev..top + 2].to_vec());
        }
    }
    if let Some(&pos) = stack.last() {
        out.push(data[pos..pos + 2].to_vec());
    }
    out.sort_unstable();
    out.dedup();
    out
}

/// Index-time grams for a strategy.
pub fn grams_index(data: &[u8], s: Strategy) -> Vec<Vec<u8>> {
    match s {
        Strategy::Trigram => trigram_grams_bytes(data),
        Strategy::Sparse => sparse_grams_all_bytes(data),
    }
}

/// Query-time grams for a strategy (trigram has no separate covering form).
pub fn grams_query(data: &[u8], s: Strategy) -> Vec<Vec<u8>> {
    match s {
        Strategy::Trigram => trigram_grams_bytes(data),
        Strategy::Sparse => sparse_grams_covering_bytes(data),
    }
}

#[cfg(test)]
mod invariant_grams {
    use super::{hash_ngram, sparse_grams_all_bytes, sparse_grams_covering_bytes};

    pub(super) fn extract_sparse_ngrams_all(data: &[u8]) -> Vec<(u64, usize)> {
        sparse_grams_all_bytes(data)
            .iter()
            .map(|g| (hash_ngram(g), g.len()))
            .collect()
    }

    pub(super) fn extract_sparse_ngrams_covering(data: &[u8]) -> Vec<(u64, usize)> {
        sparse_grams_covering_bytes(data)
            .iter()
            .map(|g| (hash_ngram(g), g.len()))
            .collect()
    }
}

/// A source of documents for INDEX BUILDS, which need full enumeration.
/// Implemented by a local dir (tests) and S3 (prod).
pub trait Corpus {
    /// All document ids with their keys (object key / file path).
    fn docs(&self) -> &[(DocId, String)];
    /// Fetch the full bytes of one document.
    fn fetch(&self, id: DocId) -> AnyhowResult<Vec<u8>>;
    /// Fetch many docs concurrently. Result order is NOT guaranteed; each item
    /// carries its `DocId`. Implementations may return fewer docs than
    /// requested when a doc vanished between indexing and fetching.
    /// Default = sequential, fail-fast.
    fn fetch_many(&self, ids: &[DocId]) -> anyhow::Result<Vec<(DocId, Vec<u8>)>> {
        ids.iter().map(|&id| Ok((id, self.fetch(id)?))).collect()
    }
}

/// Fetches documents by key for SEARCH verification — no enumeration, no
/// doc table. `consume` receives the index into `keys` plus the body, as
/// fetches complete (order NOT guaranteed). Implementations may fetch
/// concurrently and may skip vanished docs; the first `consume` error
/// aborts the remaining fetches.
pub trait DocFetcher {
    fn fetch_each(
        &self,
        keys: &[String],
        consume: &mut dyn FnMut(usize, Vec<u8>) -> AnyhowResult<()>,
    ) -> AnyhowResult<()>;
}

pub trait BlobStore {
    fn put(&self, name: &str, bytes: &[u8]) -> AnyhowResult<()>;
    /// `Ok(None)` = blob does not exist. Transient store failures are `Err`
    /// so callers never mistake an outage for an empty store.
    fn get(&self, name: &str) -> AnyhowResult<Option<Vec<u8>>>;
    fn get_range(&self, name: &str, start: u64, len: u64) -> AnyhowResult<Vec<u8>>;
    /// Fetch many byte ranges of one blob, preserving order. Implementations
    /// may fetch concurrently. Default = sequential.
    fn get_ranges(&self, name: &str, ranges: &[(u64, u64)]) -> AnyhowResult<Vec<Vec<u8>>> {
        ranges
            .iter()
            .map(|&(start, len)| self.get_range(name, start, len))
            .collect()
    }
}

#[cfg(any(test, feature = "testutil"))]
pub mod testutil {
    use super::{Corpus, DocId};
    use anyhow::Result;

    pub struct MemCorpus {
        docs: Vec<(DocId, String)>,
        bodies: Vec<Vec<u8>>,
    }

    impl MemCorpus {
        pub fn new(docs: Vec<(DocId, String)>, bodies: Vec<Vec<u8>>) -> MemCorpus {
            assert_eq!(docs.len(), bodies.len());
            MemCorpus { docs, bodies }
        }
    }

    impl Corpus for MemCorpus {
        fn docs(&self) -> &[(DocId, String)] {
            &self.docs
        }

        fn fetch(&self, id: DocId) -> Result<Vec<u8>> {
            Ok(self.bodies[id as usize].clone())
        }
    }

    impl crate::DocFetcher for MemCorpus {
        fn fetch_each(
            &self,
            keys: &[String],
            consume: &mut dyn FnMut(usize, Vec<u8>) -> Result<()>,
        ) -> Result<()> {
            for (idx, key) in keys.iter().enumerate() {
                let (id, _) = self
                    .docs
                    .iter()
                    .find(|(_, k)| k == key)
                    .ok_or_else(|| anyhow::anyhow!("unknown key {key}"))?;
                consume(idx, self.bodies[*id as usize].clone())?;
            }
            Ok(())
        }
    }

    /// Format fixtures for differential tests: each encoder produces real
    /// bytes of its format so engine-vs-oracle equality covers every codec.
    pub mod encode {
        use std::io::Write;

        pub fn gzip(data: &[u8]) -> Vec<u8> {
            let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            enc.write_all(data).unwrap();
            enc.finish().unwrap()
        }

        pub fn zstd(data: &[u8]) -> Vec<u8> {
            zstd::stream::encode_all(data, 0).unwrap()
        }

        pub fn bzip2(data: &[u8]) -> Vec<u8> {
            let mut enc = bzip2::write::BzEncoder::new(Vec::new(), bzip2::Compression::default());
            enc.write_all(data).unwrap();
            enc.finish().unwrap()
        }

        pub fn snappy_frame(data: &[u8]) -> Vec<u8> {
            let mut enc = snap::write::FrameEncoder::new(Vec::new());
            enc.write_all(data).unwrap();
            enc.into_inner().unwrap()
        }

        pub fn lz4_frame(data: &[u8]) -> Vec<u8> {
            let mut enc = lz4_flex::frame::FrameEncoder::new(Vec::new());
            enc.write_all(data).unwrap();
            enc.finish().unwrap()
        }

        pub fn xz(data: &[u8]) -> Vec<u8> {
            let mut enc = liblzma::write::XzEncoder::new(Vec::new(), 6);
            enc.write_all(data).unwrap();
            enc.finish().unwrap()
        }

        /// One string column `line`, one row per input line: the JSON Lines
        /// projection contains each input line verbatim inside its row.
        pub fn parquet_of_lines(lines: &[&str]) -> Vec<u8> {
            use arrow_array::{ArrayRef, RecordBatch, StringArray};
            use std::sync::Arc;
            let batch = RecordBatch::try_from_iter(vec![(
                "line",
                Arc::new(StringArray::from(lines.to_vec())) as ArrayRef,
            )])
            .unwrap();
            let mut buf = Vec::new();
            let mut writer =
                parquet::arrow::ArrowWriter::try_new(&mut buf, batch.schema(), None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
            buf
        }

        /// One record per input line, schema {line: string}.
        pub fn avro_of_lines(lines: &[&str]) -> Vec<u8> {
            let schema = apache_avro::Schema::parse_str(
                r#"{"type":"record","name":"row","fields":[{"name":"line","type":"string"}]}"#,
            )
            .unwrap();
            let mut writer = apache_avro::Writer::new(&schema, Vec::new());
            for line in lines {
                let mut record = apache_avro::types::Record::new(&schema).unwrap();
                record.put("line", *line);
                writer.append(record).unwrap();
            }
            writer.into_inner().unwrap()
        }
    }
}

pub struct LocalBlobStore {
    root: PathBuf,
}

impl LocalBlobStore {
    pub fn new(root: impl Into<PathBuf>) -> LocalBlobStore {
        LocalBlobStore { root: root.into() }
    }
}

impl BlobStore for LocalBlobStore {
    fn put(&self, name: &str, bytes: &[u8]) -> AnyhowResult<()> {
        let path = self.root.join(name);
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        std::fs::write(path, bytes)?;
        Ok(())
    }

    fn get(&self, name: &str) -> AnyhowResult<Option<Vec<u8>>> {
        match std::fs::read(self.root.join(name)) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    fn get_range(&self, name: &str, start: u64, len: u64) -> AnyhowResult<Vec<u8>> {
        use std::io::{Read, Seek, SeekFrom};

        let mut file = std::fs::File::open(self.root.join(name))?;
        file.seek(SeekFrom::Start(start))?;
        let mut bytes = vec![0; usize::try_from(len)?];
        file.read_exact(&mut bytes)?;
        Ok(bytes)
    }
}

/// One occurrence within a line: byte offsets into `LineEvent.text`,
/// half-open, clamped to the line's content (pre-`\n`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubMatch {
    pub start: usize,
    pub end: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LineKind {
    Match,
    Context,
}

/// One output line of a search: a matching line or a context line around
/// one. The owning object's key travels alongside, not inside.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LineEvent {
    /// 1-based line number.
    pub line: u64,
    pub kind: LineKind,
    /// Byte offset of the line start in the decoded doc.
    pub offset: u64,
    /// Exact line bytes INCLUDING the trailing `\n` when present.
    pub text: Vec<u8>,
    /// Ordered by start; non-empty for Match. A Context line past a
    /// `max_count` cap can also carry submatches (rg behavior).
    pub submatches: Vec<SubMatch>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct MatchOptions {
    pub before_context: usize,
    pub after_context: usize,
    /// Cap on MATCHING lines per doc (`rg -m`). After-context still drains.
    pub max_count: Option<u64>,
}

/// Run `re` over one decoded doc, producing the rg-ordered, overlap-merged
/// line event stream: events sorted by line, each line present at most once,
/// matches preferred over context. Empty result == zero matching lines.
pub fn grep_doc(bytes: &[u8], re: &regex::bytes::Regex, options: MatchOptions) -> Vec<LineEvent> {
    if options.max_count == Some(0) {
        return Vec::new();
    }
    let mut out = Vec::new();
    let mut finder = re.find_iter(bytes).peekable();
    let mut ring: VecDeque<(u64, usize, usize)> = VecDeque::new();
    let mut line_no: u64 = 0;
    let mut pos = 0usize;
    let mut last_emitted: u64 = 0;
    let mut after_remaining = 0usize;
    let mut matched_lines: u64 = 0;
    let mut done = false;
    while pos < bytes.len() {
        line_no += 1;
        let (content_end, span_end) = match memchr::memchr(b'\n', &bytes[pos..]) {
            Some(off) => (pos + off, pos + off + 1),
            None => (bytes.len(), bytes.len()),
        };
        let mut subs = Vec::new();
        while finder.peek().is_some_and(|m| m.start() < span_end) {
            let m = finder.next().expect("peeked");
            subs.push(SubMatch {
                start: m.start() - pos,
                end: m.end().min(content_end).max(m.start()) - pos,
            });
        }
        if !subs.is_empty() && !done {
            while let Some((l, s, e)) = ring.pop_front() {
                if l <= last_emitted {
                    continue;
                }
                out.push(LineEvent {
                    line: l,
                    kind: LineKind::Context,
                    offset: s as u64,
                    text: bytes[s..e].to_vec(),
                    submatches: Vec::new(),
                });
            }
            out.push(LineEvent {
                line: line_no,
                kind: LineKind::Match,
                offset: pos as u64,
                text: bytes[pos..span_end].to_vec(),
                submatches: subs,
            });
            last_emitted = line_no;
            matched_lines += 1;
            after_remaining = options.after_context;
            if options.max_count == Some(matched_lines) {
                done = true;
            }
        } else if after_remaining > 0 {
            out.push(LineEvent {
                line: line_no,
                kind: LineKind::Context,
                offset: pos as u64,
                text: bytes[pos..span_end].to_vec(),
                submatches: subs,
            });
            last_emitted = line_no;
            after_remaining -= 1;
        } else if options.before_context > 0 {
            if ring.len() == options.before_context {
                ring.pop_front();
            }
            ring.push_back((line_no, pos, span_end));
        }
        if (done || finder.peek().is_none()) && after_remaining == 0 {
            break;
        }
        pos = span_end;
    }
    out
}

/// Line-semantics match test (rg behavior): a doc matches iff some match
/// STARTS before EOF — an empty doc has no lines and never matches, and an
/// empty match at EOF belongs to no line.
pub fn has_line_match(bytes: &[u8], re: &regex::bytes::Regex) -> bool {
    re.find(bytes).is_some_and(|m| m.start() < bytes.len())
}

/// Oracle: keys of docs containing at least one matching line, sorted. The
/// differential ground truth.
pub fn scan_matching_docs(
    corpus: &dyn Corpus,
    re: &regex::bytes::Regex,
) -> AnyhowResult<Vec<String>> {
    let mut hits = Vec::new();
    for (id, key) in corpus.docs() {
        let bytes = corpus.fetch(*id)?;
        if has_line_match(&bytes, re) {
            hits.push(key.clone());
        }
    }
    hits.sort_unstable();
    Ok(hits)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn trigrams_basic() {
        // "abcab" -> abc, bca, cab sorted; "abc" appears once after dedup
        assert_eq!(
            trigram_grams_bytes(b"abcab"),
            vec![b"abc".to_vec(), b"bca".to_vec(), b"cab".to_vec()]
        );
    }

    #[test]
    fn trigrams_short_is_empty() {
        assert!(trigram_grams_bytes(b"ab").is_empty());
        assert!(trigram_grams_bytes(b"").is_empty());
    }

    #[test]
    fn trigram_query_subset_of_index() {
        use std::collections::HashSet;
        let pattern = b"CONSTANT";
        let content = b"let CONSTANT = 1;";
        let all: HashSet<Vec<u8>> = grams_index(content, Strategy::Trigram)
            .into_iter()
            .collect();
        let q: HashSet<Vec<u8>> = grams_query(pattern, Strategy::Trigram)
            .into_iter()
            .collect();
        assert!(q.is_subset(&all));
    }

    #[test]
    fn local_blob_store_round_trips_ranges() -> AnyhowResult<()> {
        let root = std::env::temp_dir().join(format!(
            "holys3-core-{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        ));
        let store = LocalBlobStore::new(&root);
        store.put("builds/a/postings.bin", b"abcdef")?;
        assert_eq!(
            store.get("builds/a/postings.bin")?.as_deref(),
            Some(b"abcdef".as_slice())
        );
        assert_eq!(store.get("missing")?, None);
        assert_eq!(store.get_range("builds/a/postings.bin", 2, 3)?, b"cde");
        assert_eq!(
            store.get_ranges("builds/a/postings.bin", &[(0, 2), (4, 2)])?,
            vec![b"ab".to_vec(), b"ef".to_vec()]
        );
        std::fs::remove_dir_all(root)?;
        Ok(())
    }
}

#[cfg(test)]
mod sparse_tests {
    use super::invariant_grams::{extract_sparse_ngrams_all, extract_sparse_ngrams_covering};
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn sparse_short_input() {
        assert!(extract_sparse_ngrams_all(b"a").is_empty());
        assert!(!extract_sparse_ngrams_all(b"ab").is_empty());
        assert!(extract_sparse_ngrams_covering(b"a").is_empty());
        assert!(!extract_sparse_ngrams_covering(b"ab").is_empty());
    }

    #[test]
    fn covering_subset_of_all_same_input() {
        let input = b"MAX_FILE_SIZE";
        let all: HashSet<u64> = extract_sparse_ngrams_all(input)
            .iter()
            .map(|(h, _)| *h)
            .collect();
        let cov: HashSet<u64> = extract_sparse_ngrams_covering(input)
            .iter()
            .map(|(h, _)| *h)
            .collect();
        assert!(cov.is_subset(&all));
        assert!(all.len() >= cov.len());
    }

    #[test]
    fn subset_invariant_modified_constant() {
        // covering(pattern) must be a subset of all(content) when pattern occurs in content.
        let pattern = b"MODIFIED_CONSTANT";
        let content = b"fn main() {\n let x = MODIFIED_CONSTANT;\n}\n";
        let all: HashSet<u64> = extract_sparse_ngrams_all(content)
            .iter()
            .map(|(h, _)| *h)
            .collect();
        let cov: HashSet<u64> = extract_sparse_ngrams_covering(pattern)
            .iter()
            .map(|(h, _)| *h)
            .collect();
        let missing: Vec<u64> = cov.difference(&all).copied().collect();
        assert!(
            missing.is_empty(),
            "covering(pattern) must be subset of all(content); missing: {missing:?}"
        );
    }

    #[test]
    fn covering_bytes_subset_of_all_bytes() {
        let pattern = b"MODIFIED_CONSTANT";
        let content = b"fn main() {\n let x = MODIFIED_CONSTANT;\n}\n";
        let all: HashSet<Vec<u8>> = sparse_grams_all_bytes(content).into_iter().collect();
        let cov: HashSet<Vec<u8>> = sparse_grams_covering_bytes(pattern).into_iter().collect();
        assert!(
            cov.is_subset(&all),
            "covering bytes must be subset of all bytes"
        );
    }

    #[test]
    fn subset_invariant_randomized() {
        // Deterministic pseudo-random fuzz of the invariant across many embeddings.
        let mut state: u64 = 0x9E3779B97F4A7C15;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for _ in 0..200 {
            let plen = 2 + (next() % 12) as usize;
            let pattern: Vec<u8> = (0..plen).map(|_| (next() % 96 + 32) as u8).collect();
            let pre: Vec<u8> = (0..(next() % 8) as usize)
                .map(|_| (next() % 96 + 32) as u8)
                .collect();
            let post: Vec<u8> = (0..(next() % 8) as usize)
                .map(|_| (next() % 96 + 32) as u8)
                .collect();
            let mut content = pre.clone();
            content.extend_from_slice(&pattern);
            content.extend_from_slice(&post);
            let all: HashSet<u64> = extract_sparse_ngrams_all(&content)
                .iter()
                .map(|(h, _)| *h)
                .collect();
            let cov: HashSet<u64> = extract_sparse_ngrams_covering(&pattern)
                .iter()
                .map(|(h, _)| *h)
                .collect();
            assert!(
                cov.is_subset(&all),
                "invariant broke for pattern {pattern:?} in content {content:?}"
            );
        }
    }
}

#[cfg(test)]
mod corpus_tests {
    use super::*;
    use crate::testutil::MemCorpus;

    #[test]
    fn scan_finds_matching_docs() {
        let c = MemCorpus::new(
            vec![(0, "a".into()), (1, "b".into())],
            vec![b"hello world".to_vec(), b"nothing here".to_vec()],
        );
        let re = regex::bytes::Regex::new("world").unwrap();
        assert_eq!(scan_matching_docs(&c, &re).unwrap(), vec!["a".to_owned()]);
    }

    fn re(p: &str) -> regex::bytes::Regex {
        regex::bytes::Regex::new(p).unwrap()
    }

    type EventShape = (u64, LineKind, Vec<(usize, usize)>);

    fn shape(events: &[LineEvent]) -> Vec<EventShape> {
        events
            .iter()
            .map(|e| {
                (
                    e.line,
                    e.kind,
                    e.submatches.iter().map(|s| (s.start, s.end)).collect(),
                )
            })
            .collect()
    }

    #[test]
    fn match_line_col() {
        let events = grep_doc(b"foo\nbar baz", &re("baz"), MatchOptions::default());
        assert_eq!(
            events,
            vec![LineEvent {
                line: 2,
                kind: LineKind::Match,
                offset: 4,
                text: b"bar baz".to_vec(),
                submatches: vec![SubMatch { start: 4, end: 7 }],
            }]
        );
    }

    #[test]
    fn grep_doc_merges_per_line_and_tracks_lines() {
        // x appears twice on line 3: ONE event with two submatches
        let bytes = b"alpha x\nbeta\nx gamma x\nx";
        let events = grep_doc(bytes, &re("x"), MatchOptions::default());
        assert_eq!(
            shape(&events),
            vec![
                (1, LineKind::Match, vec![(6, 7)]),
                (3, LineKind::Match, vec![(0, 1), (8, 9)]),
                (4, LineKind::Match, vec![(0, 1)]),
            ]
        );
        assert_eq!(events[1].text, b"x gamma x\n".to_vec());
        assert_eq!(events[2].text, b"x".to_vec());
    }

    #[test]
    fn grep_doc_context_merges_overlaps() {
        // matches on lines 3 and 5 with C=2: lines 1-7 once each, 3+5 Match
        let bytes = b"l1\nl2\nhit\nl4\nhit\nl6\nl7\nl8\n";
        let opts = MatchOptions {
            before_context: 2,
            after_context: 2,
            max_count: None,
        };
        let events = grep_doc(bytes, &re("hit"), opts);
        let lines: Vec<(u64, LineKind)> = events.iter().map(|e| (e.line, e.kind)).collect();
        assert_eq!(
            lines,
            vec![
                (1, LineKind::Context),
                (2, LineKind::Context),
                (3, LineKind::Match),
                (4, LineKind::Context),
                (5, LineKind::Match),
                (6, LineKind::Context),
                (7, LineKind::Context),
            ]
        );
    }

    #[test]
    fn grep_doc_independent_before_after() {
        let bytes = b"a\nb\nhit\nc\nd\n";
        let only_after = MatchOptions {
            after_context: 1,
            ..Default::default()
        };
        let events = grep_doc(bytes, &re("hit"), only_after);
        assert_eq!(
            events.iter().map(|e| e.line).collect::<Vec<_>>(),
            vec![3, 4]
        );
        let only_before = MatchOptions {
            before_context: 1,
            ..Default::default()
        };
        let events = grep_doc(bytes, &re("hit"), only_before);
        assert_eq!(
            events.iter().map(|e| e.line).collect::<Vec<_>>(),
            vec![2, 3]
        );
    }

    #[test]
    fn grep_doc_max_count_caps_but_drains_after_context() {
        let bytes = b"hit\nmid\nhit\ntail\n";
        let opts = MatchOptions {
            after_context: 1,
            max_count: Some(1),
            ..Default::default()
        };
        let events = grep_doc(bytes, &re("hit"), opts);
        // one Match, then line 2 as after-context; the capped line-3 match
        // never surfaces because after-context ran out before it
        assert_eq!(
            shape(&events),
            vec![
                (1, LineKind::Match, vec![(0, 3)]),
                (2, LineKind::Context, vec![]),
            ]
        );
        assert!(grep_doc(
            bytes,
            &re("hit"),
            MatchOptions {
                max_count: Some(0),
                ..Default::default()
            }
        )
        .is_empty());
    }

    #[test]
    fn grep_doc_post_cap_match_in_context_carries_submatches() {
        let bytes = b"hit\nhit\nrest\n";
        let opts = MatchOptions {
            after_context: 1,
            max_count: Some(1),
            ..Default::default()
        };
        let events = grep_doc(bytes, &re("hit"), opts);
        assert_eq!(
            shape(&events),
            vec![
                (1, LineKind::Match, vec![(0, 3)]),
                (2, LineKind::Context, vec![(0, 3)]),
            ]
        );
    }

    #[test]
    fn grep_doc_eof_line_without_newline() {
        let events = grep_doc(b"no newline tail", &re("tail"), MatchOptions::default());
        assert_eq!(events[0].text, b"no newline tail".to_vec());
        assert_eq!(events[0].submatches, vec![SubMatch { start: 11, end: 15 }]);
    }

    #[test]
    fn decode_body_handles_raw_gzip_multimember_and_zstd() {
        use std::io::Write;

        assert_eq!(
            decode_body("k", b"plain text".to_vec()).unwrap(),
            b"plain text"
        );

        let gz = |data: &[u8]| {
            let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            enc.write_all(data).unwrap();
            enc.finish().unwrap()
        };
        let mut multi = gz(b"first member\n");
        multi.extend(gz(b"second member\n"));
        assert_eq!(
            decode_body("k.gz", multi).unwrap(),
            b"first member\nsecond member\n"
        );

        let zst = zstd::stream::encode_all(&b"zstd body"[..], 0).unwrap();
        assert_eq!(decode_body("k.zst", zst).unwrap(), b"zstd body");

        let truncated = gz(b"data")[..6].to_vec();
        let err = decode_body("bad.gz", truncated).unwrap_err();
        assert!(err.to_string().contains("bad.gz"));
    }

    #[test]
    fn decode_body_bzip2_including_multistream_and_empty() {
        use std::io::Write;
        let bz = |data: &[u8]| {
            let mut enc = bzip2::write::BzEncoder::new(Vec::new(), bzip2::Compression::default());
            enc.write_all(data).unwrap();
            enc.finish().unwrap()
        };
        assert_eq!(detect_codec(&bz(b"hello bzip2")), Codec::Bzip2);
        assert_eq!(
            decode_body("k.bz2", bz(b"hello bzip2")).unwrap(),
            b"hello bzip2"
        );
        // bunzip2 semantics: concatenated streams decode to concatenated text
        let mut multi = bz(b"stream one\n");
        multi.extend(bz(b"stream two\n"));
        assert_eq!(
            decode_body("k.bz2", multi).unwrap(),
            b"stream one\nstream two\n"
        );
        // empty input uses the END-OF-STREAM magic, not the block magic
        let empty = bz(b"");
        assert_eq!(detect_codec(&empty), Codec::Bzip2);
        assert_eq!(decode_body("k.bz2", empty).unwrap(), b"");
    }

    #[test]
    fn decode_body_snappy_frame_including_concat() {
        use std::io::Write;
        let sz = |data: &[u8]| {
            let mut enc = snap::write::FrameEncoder::new(Vec::new());
            enc.write_all(data).unwrap();
            enc.into_inner().unwrap()
        };
        assert_eq!(detect_codec(&sz(b"snappy framed body")), Codec::SnappyFrame);
        assert_eq!(
            decode_body("k.sz", sz(b"snappy framed body")).unwrap(),
            b"snappy framed body"
        );
        // the framing spec's concat mechanism: repeated stream identifiers
        let mut multi = sz(b"part one\n");
        multi.extend(sz(b"part two\n"));
        assert_eq!(decode_body("k.sz", multi).unwrap(), b"part one\npart two\n");
    }

    #[test]
    fn decode_body_lz4_frame_including_concat_and_skippable() {
        use std::io::Write;
        let lz = |data: &[u8]| {
            let mut enc = lz4_flex::frame::FrameEncoder::new(Vec::new());
            enc.write_all(data).unwrap();
            enc.finish().unwrap()
        };
        assert_eq!(detect_codec(&lz(b"lz4 frame body")), Codec::Lz4Frame);
        assert_eq!(
            decode_body("k.lz4", lz(b"lz4 frame body")).unwrap(),
            b"lz4 frame body"
        );
        // lz4cat semantics: concatenated frames decode in order
        let mut multi = lz(b"frame one\n");
        multi.extend(lz(b"frame two\n"));
        assert_eq!(
            decode_body("k.lz4", multi).unwrap(),
            b"frame one\nframe two\n"
        );
        // a leading skippable frame (magic shared with zstd) must dispatch to
        // lz4 because the first REAL frame is lz4
        let mut skippable = vec![0x50, 0x2a, 0x4d, 0x18, 3, 0, 0, 0, 0xaa, 0xbb, 0xcc];
        skippable.extend(lz(b"after skippable"));
        assert_eq!(detect_codec(&skippable), Codec::Lz4Frame);
        assert_eq!(decode_body("k.lz4", skippable).unwrap(), b"after skippable");
    }

    #[test]
    fn decode_body_xz_including_multistream() {
        use std::io::Write;
        let xz = |data: &[u8]| {
            let mut enc = liblzma::write::XzEncoder::new(Vec::new(), 6);
            enc.write_all(data).unwrap();
            enc.finish().unwrap()
        };
        assert_eq!(detect_codec(&xz(b"xz body")), Codec::Xz);
        assert_eq!(decode_body("k.xz", xz(b"xz body")).unwrap(), b"xz body");
        let mut multi = xz(b"stream a\n");
        multi.extend(xz(b"stream b\n"));
        assert_eq!(decode_body("k.xz", multi).unwrap(), b"stream a\nstream b\n");
    }

    #[test]
    fn skippable_frames_dispatch_between_zstd_and_lz4() {
        let skippable = |payload: &[u8]| {
            let mut frame = vec![0x5a, 0x2a, 0x4d, 0x18];
            frame.extend(u32::try_from(payload.len()).unwrap().to_le_bytes());
            frame.extend(payload);
            frame
        };
        // skippable then zstd frame -> zstd, and decodes
        let mut to_zstd = skippable(b"meta");
        to_zstd.extend(zstd::stream::encode_all(&b"zstd after skip"[..], 0).unwrap());
        assert_eq!(detect_codec(&to_zstd), Codec::Zstd);
        assert_eq!(decode_body("k", to_zstd).unwrap(), b"zstd after skip");
        // only skippable frames -> empty content under either format
        assert_eq!(detect_codec(&skippable(b"junkmeta")), Codec::Zstd);
        assert_eq!(decode_body("k", skippable(b"junkmeta")).unwrap(), b"");
        // skippable then garbage -> raw bytes, untouched
        let mut to_raw = skippable(b"x");
        to_raw.extend(b"not a frame");
        assert_eq!(detect_codec(&to_raw), Codec::Raw);
        // truncated skippable header -> raw
        assert_eq!(
            detect_codec(&[0x50, 0x2a, 0x4d, 0x18, 0xff, 0xff, 0xff, 0xff]),
            Codec::Raw
        );
    }

    #[test]
    fn printable_magics_do_not_shadow_text() {
        // bzip2's "BZh1" prefix is printable; the 6-byte block magic decides
        assert_eq!(
            detect_codec(b"BZh1 is a chess move, not a codec"),
            Codec::Raw
        );
        assert_eq!(detect_codec(b"BZh0123456789"), Codec::Raw); // '0' invalid level
                                                                // parquet needs PAR1 at BOTH ends
        assert_eq!(detect_codec(b"PAR1 some text file"), Codec::Raw);
        assert_eq!(detect_codec(b"PAR1tinyPAR1"), Codec::Parquet); // 12 bytes, both ends
        assert!(decode_body("fake.parquet", b"PAR1tinyPAR1".to_vec()).is_err());
        // loud, not garbage
    }

    #[test]
    fn lz4_legacy_fails_loudly() {
        let err = decode_body("old.lz4", vec![0x02, 0x21, 0x4c, 0x18, 0, 0, 0, 0]).unwrap_err();
        assert!(err.to_string().contains("LEGACY"));
    }

    #[test]
    fn decode_body_parquet_projects_rows_as_json_lines() {
        use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
        use std::sync::Arc;
        let batch = RecordBatch::try_from_iter(vec![
            ("id", Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef),
            (
                "msg",
                Arc::new(StringArray::from(vec![Some("needle in parquet"), None])) as ArrayRef,
            ),
        ])
        .unwrap();
        let mut buf = Vec::new();
        let mut writer =
            parquet::arrow::ArrowWriter::try_new(&mut buf, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        assert_eq!(detect_codec(&buf), Codec::Parquet);
        let text = decode_body("k.parquet", buf).unwrap();
        // explicit nulls keep every row the same shape
        assert_eq!(
            text,
            b"{\"id\":1,\"msg\":\"needle in parquet\"}\n{\"id\":2,\"msg\":null}\n"
        );
    }

    #[test]
    fn decode_body_avro_projects_records_as_json_lines() {
        use apache_avro::types::Record;
        let schema = apache_avro::Schema::parse_str(
            r#"{"type":"record","name":"log","fields":[
                {"name":"id","type":"long"},{"name":"msg","type":"string"}]}"#,
        )
        .unwrap();
        let mut writer = apache_avro::Writer::new(&schema, Vec::new());
        for (id, msg) in [(1i64, "needle in avro"), (2, "hay")] {
            let mut record = Record::new(&schema).unwrap();
            record.put("id", id);
            record.put("msg", msg);
            writer.append(record).unwrap();
        }
        let buf = writer.into_inner().unwrap();
        assert_eq!(detect_codec(&buf), Codec::Avro);
        let text = decode_body("k.avro", buf).unwrap();
        assert_eq!(
            text,
            b"{\"id\":1,\"msg\":\"needle in avro\"}\n{\"id\":2,\"msg\":\"hay\"}\n"
        );
    }

    #[test]
    fn doc_fetcher_resolves_keys() {
        use crate::testutil::MemCorpus;
        use crate::DocFetcher;
        let c = MemCorpus::new(
            vec![(0, "a".into()), (1, "b".into())],
            vec![b"one".to_vec(), b"two".to_vec()],
        );
        let keys = vec!["b".to_owned(), "a".to_owned()];
        let mut seen = Vec::new();
        c.fetch_each(&keys, &mut |idx, bytes| {
            seen.push((idx, bytes));
            Ok(())
        })
        .unwrap();
        assert_eq!(seen, vec![(0, b"two".to_vec()), (1, b"one".to_vec())]);
    }

    #[test]
    fn fetch_many_aborts_on_first_error() {
        struct BrokenCorpus {
            docs: Vec<(DocId, String)>,
        }

        impl Corpus for BrokenCorpus {
            fn docs(&self) -> &[(DocId, String)] {
                &self.docs
            }

            fn fetch(&self, id: DocId) -> AnyhowResult<Vec<u8>> {
                if id == 1 {
                    anyhow::bail!("broken");
                }
                Ok(b"ok".to_vec())
            }
        }

        let corpus = BrokenCorpus {
            docs: vec![(0, "a".into()), (1, "b".into())],
        };
        assert!(corpus.fetch_many(&[0, 1]).is_err());
    }
}
