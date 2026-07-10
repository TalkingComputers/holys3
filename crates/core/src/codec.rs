use anyhow::{Context, Result as AnyhowResult};
use serde::{Deserialize, Serialize};
use std::io::Read;

mod detect;

use detect::{detect_codec, detect_codec_for_key, skip_skippable_frames, Codec};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceEncoding {
    Raw,
    Gzip,
    Zstd,
    Bzip2,
    Xz,
    SnappyFrame,
    Lz4Frame,
    Parquet,
    Avro,
    Zip,
    Tar,
    ArrowIpc,
    Orc,
    Brotli,
    Zlib,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecodeLimits {
    pub max_depth: u8,
    pub max_members: u32,
    pub max_expanded_bytes: u64,
}

pub const DECODE_LIMITS: DecodeLimits = DecodeLimits {
    max_depth: 4,
    max_members: 100_000,
    max_expanded_bytes: 64 * 1024 * 1024 * 1024,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalDocumentMeta {
    pub display_key: String,
    pub member_path: Option<String>,
}

pub trait DecodeSink {
    fn begin(&mut self, document: &LogicalDocumentMeta) -> AnyhowResult<()>;
    fn write(&mut self, bytes: &[u8]) -> AnyhowResult<()>;
    fn write_bytes(&mut self, bytes: bytes::Bytes) -> AnyhowResult<()> {
        self.write(&bytes)
    }
    fn finish(&mut self) -> AnyhowResult<()>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecodeSummary {
    pub encoding: SourceEncoding,
    pub documents: u32,
    pub expanded_bytes: u64,
}

/// Drain a streaming decoder with trailing-garbage salvage: AWS log
/// deliveries concatenate members and sometimes pad, so a decode error after
/// some bytes already decoded keeps the decoded text with a warning — for
/// grep, partial coverage beats dropping the object.
/// Chunked, not `read_to_end`: the `Read` contract discards bytes produced by
/// a FAILING read call, so a small stream decoded in one call would salvage
/// nothing. Salvage is best-effort by design — bytes decoded before the
/// error are kept even when the error proves them unreliable (checksum
/// mismatch); the warning tells the user the coverage is partial.
fn check_output(key: &str, len: usize, limit: Option<u64>) -> AnyhowResult<()> {
    if let Some(limit) = limit {
        anyhow::ensure!(
            len as u64 <= limit,
            "decoded source {key} exceeds {limit} bytes"
        );
    }
    Ok(())
}

fn read_salvaging(
    key: &str,
    label: &str,
    reader: &mut dyn Read,
    limit: Option<u64>,
    capacity: usize,
) -> AnyhowResult<Vec<u8>> {
    let mut out = Vec::with_capacity(capacity);
    let mut chunk = [0u8; 64 * 1024];
    loop {
        match reader.read(&mut chunk) {
            Ok(0) => return Ok(out),
            Ok(n) => {
                check_output(
                    key,
                    out.len()
                        .checked_add(n)
                        .context("decoded length overflows")?,
                    limit,
                )?;
                out.extend_from_slice(&chunk[..n]);
            }
            Err(err) if err.kind() == std::io::ErrorKind::Interrupted => {}
            Err(err) if !out.is_empty() => {
                eprintln!(
                    "warning: {key}: {label} stream ends in garbage ({err}); \
                     searching the {} bytes that decoded",
                    out.len()
                );
                return Ok(out);
            }
            Err(err) => {
                return Err(
                    anyhow::Error::new(err).context(format!("{label} decode failed for {key}"))
                )
            }
        }
    }
}

fn gzip_capacity(bytes: &[u8], limit: Option<u64>) -> usize {
    let Some(size) = bytes.get(bytes.len().saturating_sub(4)..) else {
        return 0;
    };
    let Ok(size) = <[u8; 4]>::try_from(size) else {
        return 0;
    };
    let size = u64::from(u32::from_le_bytes(size));
    let capacity = size.min(16 * 1024 * 1024).min(limit.unwrap_or(u64::MAX));
    usize::try_from(capacity).unwrap_or(0)
}

/// One row per line, schema column order, explicit nulls, RFC3339 timestamps,
/// hex binary — arrow-json's documented deterministic rendering.
fn parquet_to_json_lines(
    key: &str,
    bytes: bytes::Bytes,
    limit: Option<u64>,
) -> AnyhowResult<Vec<u8>> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let context = || format!("parquet decode failed for {key}");
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
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
        check_output(key, writer.get_ref().len(), limit)?;
    }
    writer.finish().with_context(context)?;
    Ok(writer.into_inner())
}

/// JSON has no NaN/Infinity; render them as null exactly like the parquet
/// projection (arrow-json) does, instead of failing the whole object over
/// one record.
fn finite_floats(value: apache_avro::types::Value) -> apache_avro::types::Value {
    use apache_avro::types::Value;
    match value {
        Value::Float(f) if !f.is_finite() => Value::Null,
        Value::Double(d) if !d.is_finite() => Value::Null,
        Value::Union(branch, inner) => Value::Union(branch, Box::new(finite_floats(*inner))),
        Value::Array(items) => Value::Array(items.into_iter().map(finite_floats).collect()),
        Value::Map(entries) => Value::Map(
            entries
                .into_iter()
                .map(|(k, v)| (k, finite_floats(v)))
                .collect(),
        ),
        Value::Record(fields) => Value::Record(
            fields
                .into_iter()
                .map(|(name, v)| (name, finite_floats(v)))
                .collect(),
        ),
        other => other,
    }
}

/// Render an unscaled decimal integer string at `scale` (digits after the
/// point), the same text fastavro/pyarrow produce.
fn decimal_text(unscaled: &num_bigint::BigInt, scale: usize) -> String {
    let raw = unscaled.to_string();
    let (sign, digits) = match raw.strip_prefix('-') {
        Some(rest) => ("-", rest),
        None => ("", raw.as_str()),
    };
    if scale == 0 {
        return format!("{sign}{digits}");
    }
    let padded = if digits.len() <= scale {
        format!("{}{digits}", "0".repeat(scale + 1 - digits.len()))
    } else {
        digits.to_owned()
    };
    let point = padded.len() - scale;
    format!("{sign}{}.{}", &padded[..point], &padded[point..])
}

/// The crate's own JSON conversion renders Decimal as a raw byte array —
/// unsearchable. The schema (which holds the scale) is in hand, so walk
/// value and schema together and render decimals as decimal strings.
fn scaled_decimals(
    value: apache_avro::types::Value,
    schema: &apache_avro::Schema,
    names: &std::collections::HashMap<apache_avro::schema::Name, &apache_avro::Schema>,
) -> AnyhowResult<apache_avro::types::Value> {
    use apache_avro::types::Value;
    use apache_avro::Schema;
    Ok(match (value, schema) {
        (value, Schema::Ref { name }) => {
            let resolved = names
                .get(name)
                .with_context(|| format!("avro schema reference {name} unresolved"))?;
            scaled_decimals(value, resolved, names)?
        }
        (Value::Decimal(decimal), Schema::Decimal(spec)) => {
            let unscaled: num_bigint::BigInt = decimal.into();
            Value::String(decimal_text(&unscaled, spec.scale))
        }
        (Value::Record(fields), Schema::Record(spec)) => Value::Record(
            fields
                .into_iter()
                .zip(&spec.fields)
                .map(|((name, value), field)| {
                    anyhow::ensure!(
                        name == field.name,
                        "avro record field order diverged from schema"
                    );
                    Ok((name, scaled_decimals(value, &field.schema, names)?))
                })
                .collect::<AnyhowResult<_>>()?,
        ),
        (Value::Array(items), Schema::Array(spec)) => Value::Array(
            items
                .into_iter()
                .map(|item| scaled_decimals(item, &spec.items, names))
                .collect::<AnyhowResult<_>>()?,
        ),
        (Value::Map(entries), Schema::Map(spec)) => Value::Map(
            entries
                .into_iter()
                .map(|(k, v)| Ok((k, scaled_decimals(v, &spec.types, names)?)))
                .collect::<AnyhowResult<_>>()?,
        ),
        (Value::Union(branch, inner), Schema::Union(spec)) => {
            let variant = spec
                .variants()
                .get(branch as usize)
                .with_context(|| format!("avro union branch {branch} out of range"))?;
            Value::Union(branch, Box::new(scaled_decimals(*inner, variant, names)?))
        }
        (value, _) => value,
    })
}

/// One record per line via the crate's own Value -> JSON conversion (with
/// schema-aware decimal rendering and NaN -> null). A mid-stream read error
/// after some records decoded salvages the decoded records with a warning,
/// like the compressed-codec paths.
fn avro_to_json_lines(key: &str, bytes: &[u8], limit: Option<u64>) -> AnyhowResult<Vec<u8>> {
    let context = || format!("avro decode failed for {key}");
    let reader = apache_avro::Reader::new(bytes).with_context(context)?;
    let schema = reader.writer_schema().clone();
    let resolved = apache_avro::schema::ResolvedSchema::try_from(&schema).with_context(context)?;
    let mut out = Vec::new();
    for value in reader {
        let value = match value {
            Ok(value) => value,
            Err(err) if !out.is_empty() => {
                eprintln!(
                    "warning: {key}: avro stream ends in garbage ({err}); \
                     searching the {} records that decoded",
                    out.iter().filter(|&&b| b == b'\n').count()
                );
                return Ok(out);
            }
            Err(err) => return Err(anyhow::Error::new(err)).with_context(context),
        };
        let value = scaled_decimals(value, &schema, resolved.get_names()).with_context(context)?;
        let json = serde_json::Value::try_from(finite_floats(value)).with_context(context)?;
        let json = json.to_string();
        let next_len = out
            .len()
            .checked_add(json.len())
            .and_then(|len| len.checked_add(1))
            .context("decoded length overflows")?;
        check_output(key, next_len, limit)?;
        out.extend_from_slice(json.as_bytes());
        out.push(b'\n');
    }
    Ok(out)
}

fn arrow_batches_to_json_lines(
    key: &str,
    limit: Option<u64>,
    batches: impl Iterator<Item = Result<arrow_array::RecordBatch, arrow_schema::ArrowError>>,
) -> AnyhowResult<Vec<u8>> {
    let context = || format!("Arrow IPC decode failed for {key}");
    let mut writer = arrow_json::writer::WriterBuilder::new()
        .with_explicit_nulls(true)
        .build::<_, arrow_json::writer::LineDelimited>(Vec::new());
    for batch in batches {
        writer
            .write(&batch.with_context(context)?)
            .with_context(context)?;
        check_output(key, writer.get_ref().len(), limit)?;
    }
    writer.finish().with_context(context)?;
    Ok(writer.into_inner())
}

fn arrow_ipc_to_json_lines(
    key: &str,
    bytes: bytes::Bytes,
    limit: Option<u64>,
) -> AnyhowResult<Vec<u8>> {
    let reader = arrow_ipc::reader::FileReader::try_new(std::io::Cursor::new(bytes), None)
        .with_context(|| format!("Arrow IPC decode failed for {key}"))?;
    arrow_batches_to_json_lines(key, limit, reader)
}

fn arrow_ipc_stream_to_json_lines(
    key: &str,
    bytes: bytes::Bytes,
    limit: Option<u64>,
) -> AnyhowResult<Vec<u8>> {
    let reader = arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)
        .with_context(|| format!("Arrow IPC stream decode failed for {key}"))?;
    arrow_batches_to_json_lines(key, limit, reader)
}

fn orc_to_json_lines(key: &str, bytes: bytes::Bytes, limit: Option<u64>) -> AnyhowResult<Vec<u8>> {
    let context = || format!("ORC decode failed for {key}");
    let reader = orc_rust::ArrowReaderBuilder::try_new(bytes)
        .with_context(context)?
        .build();
    let mut writer = arrow_json_58::writer::WriterBuilder::new()
        .with_explicit_nulls(true)
        .build::<_, arrow_json_58::writer::LineDelimited>(Vec::new());
    for batch in reader {
        writer
            .write(&batch.with_context(context)?)
            .with_context(context)?;
        check_output(key, writer.get_ref().len(), limit)?;
    }
    writer.finish().with_context(context)?;
    Ok(writer.into_inner())
}

/// Decode a flow of lz4 frames by cursor: one `read_to_end` call decodes one
/// frame, and the remaining input (via `into_inner`) decides whether to
/// continue — `Ok(0)` alone CANNOT signal exhaustion, because a legal empty
/// frame (`lz4 -c` on empty input) also decodes to zero bytes. Skippable
/// frames may legally sit between data frames; trailing garbage after at
/// least one decoded frame salvages with a warning.
fn decode_lz4_frames(key: &str, bytes: &[u8], limit: Option<u64>) -> AnyhowResult<Vec<u8>> {
    let mut out = Vec::new();
    let mut rest = bytes;
    let mut decoded_any = false;
    loop {
        let skipped = match skip_skippable_frames(rest) {
            Some(at) => &rest[at..],
            None => &[][..], // truncated skippable header: treat as garbage
        };
        if skipped.is_empty() {
            return Ok(out);
        }
        if !skipped.starts_with(&[0x04, 0x22, 0x4d, 0x18]) {
            if decoded_any {
                eprintln!(
                    "warning: {key}: lz4 stream ends in garbage; \
                     searching the {} bytes that decoded",
                    out.len()
                );
                return Ok(out);
            }
            anyhow::bail!("lz4 decode failed for {key}: input is not an lz4 frame");
        }
        let mut decoder = lz4_flex::frame::FrameDecoder::new(skipped);
        let mut chunk = [0u8; 64 * 1024];
        loop {
            match decoder.read(&mut chunk) {
                Ok(0) => break,
                Ok(read) => {
                    check_output(
                        key,
                        out.len()
                            .checked_add(read)
                            .context("decoded length overflows")?,
                        limit,
                    )?;
                    out.extend_from_slice(&chunk[..read]);
                }
                Err(err) if decoded_any || !out.is_empty() => {
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
                    );
                }
            }
        }
        decoded_any = true;
        let remaining = decoder.into_inner();
        anyhow::ensure!(
            remaining.len() < skipped.len(),
            "lz4 decoder made no progress on {key}"
        );
        rest = remaining;
    }
}

/// Decode concatenated xz streams via the low-level Stream API: the Read
/// adapters consume the whole (small) input inside one failing call, so
/// trailing garbage would salvage nothing. Explicit positions let each
/// stream end cleanly, inter-stream null padding skip, and garbage after at
/// least one good stream salvage with a warning.
fn decode_xz_streams(key: &str, bytes: &[u8], limit: Option<u64>) -> AnyhowResult<Vec<u8>> {
    let mut out = Vec::new();
    let mut pos = 0usize;
    let mut chunk = vec![0u8; 256 * 1024];
    loop {
        while bytes.get(pos) == Some(&0) {
            pos += 1; // stream padding
        }
        let rest = &bytes[pos..];
        if rest.is_empty() {
            return Ok(out);
        }
        if !rest.starts_with(&[0xfd, b'7', b'z', b'X', b'Z', 0x00]) {
            anyhow::ensure!(
                !out.is_empty(),
                "xz decode failed for {key}: input is not an xz stream"
            );
            eprintln!(
                "warning: {key}: xz stream ends in garbage; \
                 searching the {} bytes that decoded",
                out.len()
            );
            return Ok(out);
        }
        let mut stream = liblzma::stream::Stream::new_stream_decoder(u64::MAX, 0)
            .with_context(|| format!("xz decoder init failed for {key}"))?;
        let mut emitted = 0u64;
        loop {
            let input = &rest[usize::try_from(stream.total_in())?..];
            let result = stream.process(input, &mut chunk, liblzma::stream::Action::Run);
            let written = usize::try_from(stream.total_out() - emitted)?;
            check_output(
                key,
                out.len()
                    .checked_add(written)
                    .context("decoded length overflows")?,
                limit,
            )?;
            out.extend_from_slice(&chunk[..written]);
            emitted = stream.total_out();
            match result {
                Ok(liblzma::stream::Status::StreamEnd) => break,
                Ok(_) if input.is_empty() && written == 0 => {
                    // truncated final stream: keep what decoded
                    anyhow::ensure!(!out.is_empty(), "xz stream of {key} is truncated");
                    eprintln!(
                        "warning: {key}: xz stream is truncated; \
                         searching the {} bytes that decoded",
                        out.len()
                    );
                    return Ok(out);
                }
                Ok(_) => {}
                Err(err) if !out.is_empty() => {
                    eprintln!(
                        "warning: {key}: xz stream ends in garbage ({err}); \
                         searching the {} bytes that decoded",
                        out.len()
                    );
                    return Ok(out);
                }
                Err(err) => {
                    return Err(
                        anyhow::Error::new(err).context(format!("xz decode failed for {key}"))
                    )
                }
            }
        }
        pos += usize::try_from(stream.total_in())?;
    }
}

/// Transparently decode an object body into searchable text. Compressed
/// objects decompress (multi-member/multi-stream concatenations included);
/// columnar/container formats project to JSON Lines so the same bytes are
/// indexed and verified. Detection is by magic bytes only.
fn decode_body_inner(
    key: &str,
    bytes: bytes::Bytes,
    limit: Option<u64>,
) -> AnyhowResult<bytes::Bytes> {
    match detect_codec_for_key(key, &bytes) {
        Codec::Raw => {
            check_output(key, bytes.len(), limit)?;
            Ok(bytes)
        }
        Codec::Gzip => Ok(read_salvaging(
            key,
            "gzip",
            &mut flate2::read::MultiGzDecoder::new(bytes.as_ref()),
            limit,
            gzip_capacity(&bytes, limit),
        )?
        .into()),
        Codec::Zstd => {
            let mut decoder = zstd::stream::read::Decoder::new(bytes.as_ref())
                .with_context(|| format!("zstd decode failed for {key}"))?;
            Ok(read_salvaging(key, "zstd", &mut decoder, limit, 0)?.into())
        }
        Codec::Bzip2 => Ok(read_salvaging(
            key,
            "bzip2",
            &mut bzip2::read::MultiBzDecoder::new(bytes.as_ref()),
            limit,
            0,
        )?
        .into()),
        Codec::SnappyFrame => Ok(read_salvaging(
            key,
            "snappy",
            &mut snap::read::FrameDecoder::new(bytes.as_ref()),
            limit,
            0,
        )?
        .into()),
        Codec::Lz4Frame => Ok(decode_lz4_frames(key, &bytes, limit)?.into()),
        Codec::Lz4Legacy => anyhow::bail!(
            "{key} is an lz4 LEGACY frame (`lz4 -l` output), which holys3 does \
             not decode; re-compress with the default lz4 frame format"
        ),
        Codec::Xz => Ok(decode_xz_streams(key, &bytes, limit)?.into()),
        Codec::Parquet => Ok(parquet_to_json_lines(key, bytes, limit)?.into()),
        Codec::Avro => Ok(avro_to_json_lines(key, &bytes, limit)?.into()),
        Codec::Zip | Codec::Tar => {
            anyhow::bail!("{key} contains multiple archive documents; use decode_source")
        }
        Codec::ArrowIpc => Ok(arrow_ipc_to_json_lines(key, bytes, limit)?.into()),
        Codec::ArrowIpcStream => Ok(arrow_ipc_stream_to_json_lines(key, bytes, limit)?.into()),
        Codec::Brotli => {
            let mut out = Vec::new();
            let mut decoder = brotli::Decompressor::new(bytes.as_ref(), 64 * 1024);
            read_strict(key, "brotli", &mut decoder, &mut out, limit)?;
            Ok(out.into())
        }
        Codec::Zlib => {
            let mut out = Vec::new();
            let mut decoder = flate2::read::ZlibDecoder::new(bytes.as_ref());
            read_strict(key, "zlib", &mut decoder, &mut out, limit)?;
            Ok(out.into())
        }
        Codec::Orc => Ok(orc_to_json_lines(key, bytes, limit)?.into()),
    }
}

fn read_strict(
    key: &str,
    label: &str,
    reader: &mut dyn Read,
    out: &mut Vec<u8>,
    limit: Option<u64>,
) -> AnyhowResult<()> {
    let mut chunk = [0u8; 64 * 1024];
    loop {
        let read = reader
            .read(&mut chunk)
            .with_context(|| format!("{label} decode failed for {key}"))?;
        if read == 0 {
            return Ok(());
        }
        check_output(
            key,
            out.len()
                .checked_add(read)
                .context("decoded length overflows")?,
            limit,
        )?;
        out.extend_from_slice(&chunk[..read]);
    }
}

pub fn decode_body(key: &str, bytes: Vec<u8>) -> AnyhowResult<Vec<u8>> {
    Ok(decode_body_inner(key, bytes.into(), None)?.to_vec())
}

pub fn is_raw_source(key: &str, bytes: &[u8]) -> bool {
    detect_codec_for_key(key, bytes) == Codec::Raw
}

struct DecodeState<'a> {
    source_key: &'a str,
    limits: DecodeLimits,
    sink: &'a mut dyn DecodeSink,
    documents: u32,
    members: u32,
    expanded_bytes: u64,
    path_counts: std::collections::HashMap<String, u32>,
    used_paths: std::collections::HashSet<String>,
}

impl DecodeState<'_> {
    fn decode_frame(
        &mut self,
        bytes: bytes::Bytes,
        member_path: Option<String>,
        depth: u8,
        allow_hint: bool,
    ) -> AnyhowResult<SourceEncoding> {
        let key = member_path.as_deref().unwrap_or(self.source_key).to_owned();
        let codec = if allow_hint {
            detect_codec_for_key(&key, &bytes)
        } else {
            detect_codec(&bytes)
        };
        if codec != Codec::Raw {
            anyhow::ensure!(
                depth <= self.limits.max_depth,
                "decode depth exceeds {} for {}",
                self.limits.max_depth,
                self.source_key
            );
        }
        let encoding = codec_encoding(codec);
        match codec {
            Codec::Raw => self.emit(member_path, bytes)?,
            Codec::Zip => self.decode_zip(bytes, member_path, depth)?,
            Codec::Tar => self.decode_tar(&bytes, member_path, depth)?,
            Codec::Lz4Legacy => {
                decode_body_inner(self.source_key, bytes, Some(self.remaining()?))?;
            }
            _ => {
                let decoded = decode_body_inner(&key, bytes, Some(self.remaining()?))?;
                self.decode_frame(decoded, member_path, depth + 1, false)?;
            }
        }
        Ok(encoding)
    }

    fn decode_zip(
        &mut self,
        bytes: bytes::Bytes,
        parent: Option<String>,
        depth: u8,
    ) -> AnyhowResult<()> {
        let mut archive = zip::ZipArchive::new(std::io::Cursor::new(bytes))
            .with_context(|| format!("zip decode failed for {}", self.source_key))?;
        for index in 0..archive.len() {
            let mut entry = archive
                .by_index(index)
                .with_context(|| format!("zip member {index} of {}", self.source_key))?;
            self.count_member()?;
            if entry.encrypted() {
                anyhow::bail!("encrypted ZIP member {} is unsupported", entry.name());
            }
            if entry.is_dir() || entry.is_symlink() || !entry.is_file() {
                continue;
            }
            let name = clean_member_path(entry.name())?;
            let path = self.unique_path(join_member_path(parent.as_deref(), &name));
            let mut body = Vec::new();
            read_bounded(&mut entry, &mut body, self.remaining()?, self.source_key)?;
            self.decode_frame(body.into(), Some(path), depth + 1, true)?;
        }
        Ok(())
    }

    fn decode_tar(&mut self, bytes: &[u8], parent: Option<String>, depth: u8) -> AnyhowResult<()> {
        let mut archive = tar::Archive::new(bytes);
        for entry in archive
            .entries()
            .with_context(|| format!("tar decode failed for {}", self.source_key))?
        {
            let mut entry = entry.with_context(|| format!("tar member of {}", self.source_key))?;
            self.count_member()?;
            if !entry.header().entry_type().is_file() {
                continue;
            }
            let path = entry
                .path()
                .with_context(|| format!("tar path of {}", self.source_key))?;
            let name = path
                .to_str()
                .with_context(|| format!("tar path of {} is not valid UTF-8", self.source_key))?;
            let name = clean_member_path(name)?;
            let path = self.unique_path(join_member_path(parent.as_deref(), &name));
            let mut body = Vec::new();
            read_bounded(&mut entry, &mut body, self.remaining()?, self.source_key)?;
            self.decode_frame(body.into(), Some(path), depth + 1, true)?;
        }
        Ok(())
    }

    fn emit(&mut self, member_path: Option<String>, bytes: bytes::Bytes) -> AnyhowResult<()> {
        let len = u64::try_from(bytes.len())?;
        let expanded_bytes = self
            .expanded_bytes
            .checked_add(len)
            .context("decoded byte count overflows u64")?;
        anyhow::ensure!(
            expanded_bytes <= self.limits.max_expanded_bytes,
            "decoded source {} exceeds {} bytes",
            self.source_key,
            self.limits.max_expanded_bytes
        );
        let display_key = match &member_path {
            Some(path) => format!("{}!/{path}", self.source_key),
            None => self.source_key.to_owned(),
        };
        self.sink.begin(&LogicalDocumentMeta {
            display_key,
            member_path,
        })?;
        if !bytes.is_empty() {
            self.sink.write_bytes(bytes)?;
        }
        self.sink.finish()?;
        self.documents = self
            .documents
            .checked_add(1)
            .context("logical document count overflows u32")?;
        self.expanded_bytes = expanded_bytes;
        Ok(())
    }

    fn count_member(&mut self) -> AnyhowResult<()> {
        self.members = self
            .members
            .checked_add(1)
            .context("archive member count overflows u32")?;
        anyhow::ensure!(
            self.members <= self.limits.max_members,
            "archive member count exceeds {} for {}",
            self.limits.max_members,
            self.source_key
        );
        Ok(())
    }

    fn remaining(&self) -> AnyhowResult<u64> {
        self.limits
            .max_expanded_bytes
            .checked_sub(self.expanded_bytes)
            .context("decoded byte count exceeds its limit")
    }

    fn unique_path(&mut self, path: String) -> String {
        if self.used_paths.insert(path.clone()) {
            self.path_counts.insert(path.clone(), 2);
            return path;
        }
        let count = self.path_counts.entry(path.clone()).or_insert(2);
        loop {
            let candidate = format!("{path}#{count}");
            *count += 1;
            if self.used_paths.insert(candidate.clone()) {
                return candidate;
            }
        }
    }
}

fn codec_encoding(codec: Codec) -> SourceEncoding {
    match codec {
        Codec::Raw => SourceEncoding::Raw,
        Codec::Gzip => SourceEncoding::Gzip,
        Codec::Zstd => SourceEncoding::Zstd,
        Codec::Bzip2 => SourceEncoding::Bzip2,
        Codec::SnappyFrame => SourceEncoding::SnappyFrame,
        Codec::Lz4Frame | Codec::Lz4Legacy => SourceEncoding::Lz4Frame,
        Codec::Xz => SourceEncoding::Xz,
        Codec::Parquet => SourceEncoding::Parquet,
        Codec::Avro => SourceEncoding::Avro,
        Codec::Zip => SourceEncoding::Zip,
        Codec::Tar => SourceEncoding::Tar,
        Codec::ArrowIpc | Codec::ArrowIpcStream => SourceEncoding::ArrowIpc,
        Codec::Brotli => SourceEncoding::Brotli,
        Codec::Zlib => SourceEncoding::Zlib,
        Codec::Orc => SourceEncoding::Orc,
    }
}

fn clean_member_path(raw: &str) -> AnyhowResult<String> {
    anyhow::ensure!(!raw.contains('\0'), "unsafe archive path contains NUL");
    let replaced = raw.replace('\\', "/");
    let path = std::path::Path::new(&replaced);
    let mut parts = Vec::new();
    for component in path.components() {
        match component {
            std::path::Component::Normal(part) => {
                parts.push(part.to_str().context("archive path is not valid UTF-8")?);
            }
            std::path::Component::CurDir => {}
            _ => anyhow::bail!("unsafe archive path {raw}"),
        }
    }
    anyhow::ensure!(!parts.is_empty(), "unsafe empty archive path");
    Ok(parts.join("/"))
}

fn join_member_path(parent: Option<&str>, child: &str) -> String {
    match parent {
        Some(parent) => format!("{parent}!/{child}"),
        None => child.to_owned(),
    }
}

fn read_bounded(
    reader: &mut impl Read,
    out: &mut Vec<u8>,
    limit: u64,
    source_key: &str,
) -> AnyhowResult<()> {
    let read_limit = limit
        .checked_add(1)
        .context("archive byte limit overflows u64")?;
    Read::take(reader, read_limit).read_to_end(out)?;
    anyhow::ensure!(
        out.len() as u64 <= limit,
        "archive member of {source_key} exceeds {limit} bytes"
    );
    Ok(())
}

pub fn decode_source(
    source_key: &str,
    bytes: bytes::Bytes,
    limits: DecodeLimits,
    sink: &mut dyn DecodeSink,
) -> AnyhowResult<DecodeSummary> {
    anyhow::ensure!(limits.max_depth > 0, "decode max depth must be positive");
    anyhow::ensure!(
        limits.max_members > 0,
        "decode max members must be positive"
    );
    anyhow::ensure!(
        limits.max_expanded_bytes > 0,
        "decode expanded-byte limit must be positive"
    );
    let mut state = DecodeState {
        source_key,
        limits,
        sink,
        documents: 0,
        members: 0,
        expanded_bytes: 0,
        path_counts: std::collections::HashMap::new(),
        used_paths: std::collections::HashSet::new(),
    };
    let encoding = state.decode_frame(bytes, None, 1, true)?;
    Ok(DecodeSummary {
        encoding,
        documents: state.documents,
        expanded_bytes: state.expanded_bytes,
    })
}

struct RequestedSink<'a> {
    requests: std::collections::HashMap<Option<String>, Vec<usize>>,
    selected: Vec<usize>,
    bytes: Vec<bytes::Bytes>,
    consume: &'a mut dyn FnMut(usize, bytes::Bytes) -> AnyhowResult<()>,
}

impl DecodeSink for RequestedSink<'_> {
    fn begin(&mut self, document: &LogicalDocumentMeta) -> AnyhowResult<()> {
        self.selected = self
            .requests
            .remove(&document.member_path)
            .unwrap_or_default();
        self.bytes.clear();
        Ok(())
    }

    fn write(&mut self, bytes: &[u8]) -> AnyhowResult<()> {
        if !self.selected.is_empty() {
            self.bytes.push(bytes::Bytes::copy_from_slice(bytes));
        }
        Ok(())
    }

    fn write_bytes(&mut self, bytes: bytes::Bytes) -> AnyhowResult<()> {
        if !self.selected.is_empty() {
            self.bytes.push(bytes);
        }
        Ok(())
    }

    fn finish(&mut self) -> AnyhowResult<()> {
        if self.selected.is_empty() {
            return Ok(());
        }
        let bytes = join_bytes(std::mem::take(&mut self.bytes));
        for index in self.selected.drain(..) {
            (self.consume)(index, bytes.clone())?;
        }
        Ok(())
    }
}

fn join_bytes(mut chunks: Vec<bytes::Bytes>) -> bytes::Bytes {
    match chunks.len() {
        0 => bytes::Bytes::new(),
        1 => chunks.pop().expect("one chunk"),
        _ => {
            let len = chunks.iter().map(bytes::Bytes::len).sum();
            let mut joined = bytes::BytesMut::with_capacity(len);
            for chunk in chunks {
                joined.extend_from_slice(&chunk);
            }
            joined.freeze()
        }
    }
}

pub fn decode_requested(
    source_key: &str,
    requests: &[(usize, Option<String>)],
    bytes: bytes::Bytes,
    consume: &mut dyn FnMut(usize, bytes::Bytes) -> AnyhowResult<()>,
) -> AnyhowResult<()> {
    let mut grouped = std::collections::HashMap::new();
    for (index, member_path) in requests {
        grouped
            .entry(member_path.clone())
            .or_insert_with(Vec::new)
            .push(*index);
    }
    let mut sink = RequestedSink {
        requests: grouped,
        selected: Vec::new(),
        bytes: Vec::new(),
        consume,
    };
    decode_source(source_key, bytes, DECODE_LIMITS, &mut sink)?;
    anyhow::ensure!(
        sink.requests.is_empty(),
        "{} indexed logical documents are missing from {source_key}",
        sink.requests.len()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{grep_doc, MatchOptions};

    #[derive(Default)]
    struct RecordingSink {
        documents: Vec<(LogicalDocumentMeta, Vec<u8>)>,
    }

    impl DecodeSink for RecordingSink {
        fn begin(&mut self, document: &LogicalDocumentMeta) -> AnyhowResult<()> {
            self.documents.push((document.clone(), Vec::new()));
            Ok(())
        }

        fn write(&mut self, bytes: &[u8]) -> AnyhowResult<()> {
            self.documents
                .last_mut()
                .unwrap()
                .1
                .extend_from_slice(bytes);
            Ok(())
        }

        fn finish(&mut self) -> AnyhowResult<()> {
            Ok(())
        }
    }

    fn zip_bytes(entries: &[(&str, &[u8])]) -> Vec<u8> {
        use std::io::Write;
        let cursor = std::io::Cursor::new(Vec::new());
        let mut writer = zip::ZipWriter::new(cursor);
        for (name, body) in entries {
            writer
                .start_file(*name, zip::write::SimpleFileOptions::default())
                .unwrap();
            writer.write_all(body).unwrap();
        }
        writer.finish().unwrap().into_inner()
    }

    fn tar_bytes(entries: &[(&str, &[u8])]) -> Vec<u8> {
        let mut writer = tar::Builder::new(Vec::new());
        for (name, body) in entries {
            let mut header = tar::Header::new_gnu();
            header.set_mode(0o644);
            header.set_size(body.len() as u64);
            header.set_cksum();
            writer.append_data(&mut header, *name, *body).unwrap();
        }
        writer.into_inner().unwrap()
    }

    #[test]
    fn decode_source_emits_one_raw_document() {
        let mut sink = RecordingSink::default();
        let summary = decode_source(
            "logs/a.log",
            bytes::Bytes::from_static(b"needle\n"),
            DECODE_LIMITS,
            &mut sink,
        )
        .unwrap();
        assert_eq!(summary.encoding, SourceEncoding::Raw);
        assert_eq!(summary.documents, 1);
        assert_eq!(summary.expanded_bytes, 7);
        assert_eq!(
            sink.documents,
            vec![(
                LogicalDocumentMeta {
                    display_key: "logs/a.log".to_owned(),
                    member_path: None,
                },
                b"needle\n".to_vec()
            )]
        );
    }

    #[test]
    fn decode_source_emits_zip_members() {
        let bytes = zip_bytes(&[
            ("logs/b.log", b"beta needle\n"),
            ("logs/a.log", b"alpha needle\n"),
            ("empty.log", b""),
        ]);
        let mut sink = RecordingSink::default();
        let summary = decode_source("bundle.zip", bytes.into(), DECODE_LIMITS, &mut sink).unwrap();
        assert_eq!(summary.encoding, SourceEncoding::Zip);
        assert_eq!(summary.documents, 3);
        assert_eq!(
            sink.documents
                .iter()
                .map(|(meta, _)| meta.display_key.as_str())
                .collect::<Vec<_>>(),
            [
                "bundle.zip!/logs/b.log",
                "bundle.zip!/logs/a.log",
                "bundle.zip!/empty.log"
            ]
        );
    }

    #[test]
    fn decode_source_emits_tar_members_after_gzip() {
        use std::io::Write;
        let tar = tar_bytes(&[("logs/app.log", b"nested needle\n")]);
        let mut gzip = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        gzip.write_all(&tar).unwrap();
        let bytes = gzip.finish().unwrap();
        let mut sink = RecordingSink::default();
        let summary =
            decode_source("bundle.tar.gz", bytes.into(), DECODE_LIMITS, &mut sink).unwrap();
        assert_eq!(summary.encoding, SourceEncoding::Gzip);
        assert_eq!(summary.documents, 1);
        assert_eq!(
            sink.documents[0].0.display_key,
            "bundle.tar.gz!/logs/app.log"
        );
        assert_eq!(sink.documents[0].1, b"nested needle\n");
    }

    #[test]
    fn decode_source_rejects_archive_traversal() {
        let bytes = zip_bytes(&[("../secret.log", b"secret")]);
        let error = decode_source(
            "unsafe.zip",
            bytes.into(),
            DECODE_LIMITS,
            &mut RecordingSink::default(),
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("unsafe archive path"),
            "{error:#}"
        );
    }

    #[test]
    fn decode_source_disambiguates_duplicate_members() {
        let bytes = zip_bytes(&[
            ("dir\\same.log", b"first"),
            ("dir/same.log", b"second"),
            ("dir/same.log#2", b"literal suffix"),
        ]);
        let mut sink = RecordingSink::default();
        decode_source("duplicates.zip", bytes.into(), DECODE_LIMITS, &mut sink).unwrap();
        assert_eq!(
            sink.documents
                .iter()
                .map(|(meta, _)| meta.display_key.as_str())
                .collect::<Vec<_>>(),
            [
                "duplicates.zip!/dir/same.log",
                "duplicates.zip!/dir/same.log#2",
                "duplicates.zip!/dir/same.log#2#2"
            ]
        );
    }

    #[test]
    fn decode_requested_rejects_missing_logical_documents() {
        let error = decode_requested(
            "bundle.zip",
            &[(0, Some("missing.log".into()))],
            zip_bytes(&[("present.log", b"body")]).into(),
            &mut |_, _| Ok(()),
        )
        .unwrap_err();
        assert!(error.to_string().contains("1 indexed logical documents"));
    }

    #[test]
    fn decode_source_enforces_member_and_byte_limits() {
        let bytes = zip_bytes(&[("a.log", b"alpha"), ("b.log", b"beta")]);
        let member_error = decode_source(
            "members.zip",
            bytes.clone().into(),
            DecodeLimits {
                max_depth: 4,
                max_members: 1,
                max_expanded_bytes: 64,
            },
            &mut RecordingSink::default(),
        )
        .unwrap_err();
        assert!(member_error.to_string().contains("member count"));
        let byte_error = decode_source(
            "bytes.zip",
            bytes.into(),
            DecodeLimits {
                max_depth: 4,
                max_members: 10,
                max_expanded_bytes: 4,
            },
            &mut RecordingSink::default(),
        )
        .unwrap_err();
        assert!(byte_error.to_string().contains("exceeds 4 bytes"));
    }

    #[test]
    fn archive_limit_counts_non_file_entries() {
        let mut writer = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
        for index in 0..3 {
            writer
                .add_directory(
                    format!("directory-{index}/"),
                    zip::write::SimpleFileOptions::default(),
                )
                .unwrap();
        }
        let bytes = writer.finish().unwrap().into_inner();
        let error = decode_source(
            "directories.zip",
            bytes.into(),
            DecodeLimits {
                max_depth: 4,
                max_members: 2,
                max_expanded_bytes: 64,
            },
            &mut RecordingSink::default(),
        )
        .expect_err("archive entry limit should include directories");
        assert!(error.to_string().contains("archive member count"));
    }

    #[test]
    fn decode_source_enforces_nested_depth() {
        let mut four = b"leaf".to_vec();
        for depth in (1..=4).rev() {
            let name = format!("layer-{depth}.zip");
            four = zip_bytes(&[(name.as_str(), four.as_slice())]);
        }
        let mut sink = RecordingSink::default();
        decode_source("four.zip", four.clone().into(), DECODE_LIMITS, &mut sink).unwrap();
        assert_eq!(sink.documents.len(), 1);
        let five = zip_bytes(&[("layer-0.zip", four.as_slice())]);
        let error = decode_source(
            "five.zip",
            five.into(),
            DECODE_LIMITS,
            &mut RecordingSink::default(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("depth exceeds 4"), "{error:#}");
    }

    #[test]
    fn decode_source_handles_brotli_and_zlib_hints() {
        use std::io::Write;
        let mut brotli = brotli::CompressorWriter::new(Vec::new(), 64 * 1024, 5, 22);
        brotli.write_all(b"brotli needle\n").unwrap();
        let brotli = brotli.into_inner();
        let mut sink = RecordingSink::default();
        let summary = decode_source("app.log.br", brotli.into(), DECODE_LIMITS, &mut sink).unwrap();
        assert_eq!(summary.encoding, SourceEncoding::Brotli);
        assert_eq!(sink.documents[0].1, b"brotli needle\n");

        let mut zlib = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
        zlib.write_all(b"zlib needle\n").unwrap();
        let zlib = zlib.finish().unwrap();
        let mut sink = RecordingSink::default();
        let summary = decode_source("app.log.zz", zlib.into(), DECODE_LIMITS, &mut sink).unwrap();
        assert_eq!(summary.encoding, SourceEncoding::Zlib);
        assert_eq!(sink.documents[0].1, b"zlib needle\n");

        assert!(decode_source(
            "broken.br",
            bytes::Bytes::from_static(b"not brotli"),
            DECODE_LIMITS,
            &mut RecordingSink::default(),
        )
        .is_err());
    }

    #[test]
    fn decode_source_stops_compression_expansion_at_limit() {
        let expanded = vec![b'x'; 2 * 1024 * 1024];
        let cases = [
            ("bomb.gz", crate::testutil::encode::gzip(&expanded)),
            ("bomb.zst", crate::testutil::encode::zstd(&expanded)),
            ("bomb.bz2", crate::testutil::encode::bzip2(&expanded)),
            (
                "bomb.snappy",
                crate::testutil::encode::snappy_frame(&expanded),
            ),
            ("bomb.lz4", crate::testutil::encode::lz4_frame(&expanded)),
            ("bomb.xz", crate::testutil::encode::xz(&expanded)),
            ("bomb.br", crate::testutil::encode::brotli(&expanded)),
            ("bomb.zlib", crate::testutil::encode::zlib(&expanded)),
        ];
        for (key, bytes) in cases {
            let error = decode_source(
                key,
                bytes.into(),
                DecodeLimits {
                    max_depth: 4,
                    max_members: 10,
                    max_expanded_bytes: 64 * 1024,
                },
                &mut RecordingSink::default(),
            )
            .unwrap_err();
            assert!(
                error.to_string().contains("exceeds 65536 bytes"),
                "{key}: {error:#}"
            );
        }
    }

    #[test]
    fn decode_source_projects_arrow_ipc_file() {
        use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
        use std::sync::Arc;
        let batch = RecordBatch::try_from_iter(vec![
            ("id", Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef),
            (
                "msg",
                Arc::new(StringArray::from(vec![Some("arrow needle"), None])) as ArrayRef,
            ),
        ])
        .unwrap();
        let mut writer =
            arrow_ipc::writer::FileWriter::try_new(Vec::new(), batch.schema().as_ref()).unwrap();
        writer.write(&batch).unwrap();
        let bytes = writer.into_inner().unwrap();
        let mut sink = RecordingSink::default();
        let summary =
            decode_source("events.arrow", bytes.into(), DECODE_LIMITS, &mut sink).unwrap();
        assert_eq!(summary.encoding, SourceEncoding::ArrowIpc);
        assert_eq!(
            sink.documents[0].1,
            b"{\"id\":1,\"msg\":\"arrow needle\"}\n{\"id\":2,\"msg\":null}\n"
        );
    }

    #[test]
    fn decode_source_projects_arrow_ipc_stream() {
        use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
        use std::sync::Arc;
        let batch = RecordBatch::try_from_iter(vec![
            ("id", Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef),
            (
                "msg",
                Arc::new(StringArray::from(vec![Some("stream needle"), None])) as ArrayRef,
            ),
        ])
        .unwrap();
        let mut writer =
            arrow_ipc::writer::StreamWriter::try_new(Vec::new(), batch.schema().as_ref()).unwrap();
        writer.write(&batch).unwrap();
        let bytes = writer.into_inner().unwrap();
        let mut sink = RecordingSink::default();
        let summary =
            decode_source("events.arrows", bytes.into(), DECODE_LIMITS, &mut sink).unwrap();
        assert_eq!(summary.encoding, SourceEncoding::ArrowIpc);
        assert_eq!(
            sink.documents[0].1,
            b"{\"id\":1,\"msg\":\"stream needle\"}\n{\"id\":2,\"msg\":null}\n"
        );
    }

    #[test]
    fn decode_source_projects_legacy_arrow_ipc_stream() {
        let options =
            arrow_ipc::writer::IpcWriteOptions::try_new(8, true, arrow_ipc::MetadataVersion::V4)
                .unwrap();
        let writer = arrow_ipc::writer::StreamWriter::try_new_with_options(
            Vec::new(),
            &arrow_schema::Schema::empty(),
            options,
        )
        .unwrap();
        let bytes = writer.into_inner().unwrap();
        let mut sink = RecordingSink::default();
        let summary =
            decode_source("empty.arrows", bytes.into(), DECODE_LIMITS, &mut sink).unwrap();
        assert_eq!(summary.encoding, SourceEncoding::ArrowIpc);
        assert!(sink.documents[0].1.is_empty());
    }

    #[test]
    fn decode_source_rejects_invalid_arrow_stream_marker() {
        let bytes = vec![0xff, 0xff, 0xff, 0xff, 4, 0, 0, 0, 0, 0, 0, 0];
        let mut sink = RecordingSink::default();
        let summary = decode_source(
            "binary.arrows",
            bytes.clone().into(),
            DECODE_LIMITS,
            &mut sink,
        )
        .unwrap();
        assert_eq!(summary.encoding, SourceEncoding::Raw);
        assert_eq!(sink.documents[0].1, bytes);
    }

    #[test]
    fn decode_source_projects_orc_file() {
        use arrow_array_58::{ArrayRef, Int64Array, RecordBatch, StringArray};
        use std::sync::Arc;
        let batch = RecordBatch::try_from_iter(vec![
            ("id", Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef),
            (
                "msg",
                Arc::new(StringArray::from(vec![Some("orc needle"), None])) as ArrayRef,
            ),
        ])
        .unwrap();
        let mut bytes = Vec::new();
        let mut writer = orc_rust::ArrowWriterBuilder::new(&mut bytes, batch.schema())
            .try_build()
            .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let mut sink = RecordingSink::default();
        let summary = decode_source("events.orc", bytes.into(), DECODE_LIMITS, &mut sink).unwrap();
        assert_eq!(summary.encoding, SourceEncoding::Orc);
        assert_eq!(
            sink.documents[0].1,
            b"{\"id\":1,\"msg\":\"orc needle\"}\n{\"id\":2,\"msg\":null}\n"
        );
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
    fn zstd_multiframe_and_trailing_garbage_salvage() {
        let zst = |data: &[u8]| zstd::stream::encode_all(data, 0).unwrap();
        // concatenated frames decode to concatenated text
        let mut multi = zst(b"frame one\n");
        multi.extend(zst(b"frame two\n"));
        assert_eq!(
            decode_body("k.zst", multi).unwrap(),
            b"frame one\nframe two\n"
        );
        // trailing garbage salvages the decoded frames instead of dropping all
        let mut garbage = zst(b"good part\n");
        garbage.extend(b"not a frame at all");
        assert_eq!(decode_body("k.zst", garbage).unwrap(), b"good part\n");
    }

    #[test]
    fn xz_trailing_garbage_salvages() {
        use std::io::Write;
        let mut enc = liblzma::write::XzEncoder::new(Vec::new(), 6);
        enc.write_all(b"good part\n").unwrap();
        let mut bytes = enc.finish().unwrap();
        bytes.extend(b"@@@@ trailing junk that is not an xz stream");
        assert_eq!(decode_body("k.xz", bytes).unwrap(), b"good part\n");
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
        assert_eq!(detect_codec(b"ORC request completed normally"), Codec::Raw);

        // parquet needs PAR1 at BOTH ends AND a plausible footer length: a
        // text impostor's 4 bytes before the trailing magic decode as a
        // metadata length far larger than the file, so it stays Raw
        assert_eq!(detect_codec(b"PAR1 some text file"), Codec::Raw);
        assert_eq!(detect_codec(b"PAR1tinyPAR1"), Codec::Raw);
        assert_eq!(
            detect_codec(b"PAR1 this is a text file that ends with PAR1"),
            Codec::Raw
        );
        // a structurally plausible footer (metadata_len = 0) IS detected,
        // and the decoder then fails loudly rather than producing garbage
        let mut tiny = b"PAR1".to_vec();
        tiny.extend(0u32.to_le_bytes());
        tiny.extend(b"PAR1");
        assert_eq!(detect_codec(&tiny), Codec::Parquet);
        assert!(decode_body("fake.parquet", tiny).is_err());
    }

    #[test]
    fn lz4_legacy_fails_loudly() {
        let err = decode_body("old.lz4", vec![0x02, 0x21, 0x4c, 0x18, 0, 0, 0, 0]).unwrap_err();
        assert!(err.to_string().contains("LEGACY"));
    }

    #[test]
    fn parquet_named_timezone_utc_decodes() {
        // tz="UTC" is what pyarrow/pandas/Spark write; requires chrono-tz
        use arrow_array::{ArrayRef, RecordBatch, StringArray, TimestampMillisecondArray};
        use std::sync::Arc;
        let ts =
            TimestampMillisecondArray::from(vec![Some(1_700_000_000_123)]).with_timezone("UTC");
        let msg = StringArray::from(vec!["NEEDLE_utc here"]);
        let batch = RecordBatch::try_from_iter(vec![
            ("ts", Arc::new(ts) as ArrayRef),
            ("msg", Arc::new(msg) as ArrayRef),
        ])
        .unwrap();
        let mut writer =
            parquet::arrow::ArrowWriter::try_new(Vec::new(), batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        let text = decode_body("k.parquet", writer.into_inner().unwrap()).unwrap();
        let re = regex::bytes::Regex::new("NEEDLE_utc").unwrap();
        assert_eq!(grep_doc(&text, &re, MatchOptions::default()).len(), 1);
        assert!(
            text.windows(20).any(|w| w == b"2023-11-14T22:13:20."),
            "RFC3339 rendering"
        );
    }

    #[test]
    fn avro_nan_record_does_not_poison_siblings() {
        use apache_avro::types::Record;
        let schema = apache_avro::Schema::parse_str(
            r#"{"type":"record","name":"r","fields":[
                {"name":"v","type":"double"},{"name":"msg","type":"string"}]}"#,
        )
        .unwrap();
        let mut writer = apache_avro::Writer::new(&schema, Vec::new());
        for (v, msg) in [
            (1.0f64, "before"),
            (f64::NAN, "poison NEEDLE_nan"),
            (2.0, "after"),
        ] {
            let mut record = Record::new(&schema).unwrap();
            record.put("v", v);
            record.put("msg", msg);
            writer.append(record).unwrap();
        }
        let text = decode_body("k.avro", writer.into_inner().unwrap()).unwrap();
        // NaN renders as null (same as the parquet projection); siblings intact
        assert_eq!(
            text,
            b"{\"msg\":\"before\",\"v\":1.0}\n{\"msg\":\"poison NEEDLE_nan\",\"v\":null}\n{\"msg\":\"after\",\"v\":2.0}\n"
        );
    }

    #[test]
    fn lz4_empty_frames_do_not_swallow_followers() {
        use std::io::Write;
        let lz = |data: &[u8]| {
            let mut enc = lz4_flex::frame::FrameEncoder::new(Vec::new());
            enc.write_all(data).unwrap();
            enc.finish().unwrap()
        };
        let mut leading = lz(b"");
        leading.extend(lz(b"tail data\n"));
        assert_eq!(decode_body("k.lz4", leading).unwrap(), b"tail data\n");
        let mut middle = lz(b"head\n");
        middle.extend(lz(b""));
        middle.extend(lz(b"tail\n"));
        assert_eq!(decode_body("k.lz4", middle).unwrap(), b"head\ntail\n");
        // skippable frame BETWEEN data frames (legal per the frame spec)
        let mut between = lz(b"one\n");
        between.extend([0x50, 0x2a, 0x4d, 0x18, 2, 0, 0, 0, 0xaa, 0xbb]);
        between.extend(lz(b"two\n"));
        assert_eq!(decode_body("k.lz4", between).unwrap(), b"one\ntwo\n");
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
    fn avro_decimal_renders_as_decimal_string() {
        use apache_avro::types::Record;
        let schema = apache_avro::Schema::parse_str(
            r#"{"type":"record","name":"r","fields":[
                {"name":"amount","type":{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}},
                {"name":"msg","type":"string"}]}"#,
        )
        .unwrap();
        let mut writer = apache_avro::Writer::new(&schema, Vec::new());
        let mut record = Record::new(&schema).unwrap();
        record.put(
            "amount",
            apache_avro::types::Value::Decimal(apache_avro::Decimal::from(
                12345i64.to_be_bytes().to_vec(),
            )),
        );
        record.put("msg", "price tag");
        writer.append(record).unwrap();
        let text = decode_body("k.avro", writer.into_inner().unwrap()).unwrap();
        assert_eq!(text, b"{\"amount\":\"123.45\",\"msg\":\"price tag\"}\n");
    }

    #[test]
    fn avro_bzip2_and_xz_codecs_decode() {
        use apache_avro::types::Record;
        let schema = apache_avro::Schema::parse_str(
            r#"{"type":"record","name":"r","fields":[{"name":"msg","type":"string"}]}"#,
        )
        .unwrap();
        for codec in [
            apache_avro::Codec::Bzip2(apache_avro::Bzip2Settings::default()),
            apache_avro::Codec::Xz(apache_avro::XzSettings::default()),
        ] {
            let mut writer = apache_avro::Writer::with_codec(&schema, Vec::new(), codec);
            let mut record = Record::new(&schema).unwrap();
            record.put("msg", "needle in codec");
            writer.append(record).unwrap();
            let text = decode_body("k.avro", writer.into_inner().unwrap()).unwrap();
            assert_eq!(text, b"{\"msg\":\"needle in codec\"}\n");
        }
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
}
