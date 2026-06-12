use anyhow::{Context, Result as AnyhowResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Codec {
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
pub(crate) fn detect_codec(bytes: &[u8]) -> Codec {
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
    } else if is_parquet(bytes) {
        Codec::Parquet
    } else if bytes.starts_with(&[b'O', b'b', b'j', 0x01]) {
        Codec::Avro
    } else {
        Codec::Raw
    }
}

/// PAR1 at both ends is printable ASCII a text file could carry; the footer
/// length field (4 LE bytes before the trailing magic) must also fit the
/// file, which no plain-text impostor satisfies by accident.
fn is_parquet(bytes: &[u8]) -> bool {
    if bytes.len() < 12 || !bytes.starts_with(b"PAR1") || !bytes.ends_with(b"PAR1") {
        return false;
    }
    let len_field = &bytes[bytes.len() - 8..bytes.len() - 4];
    let metadata_len = u32::from_le_bytes(len_field.try_into().expect("4 bytes")) as usize;
    // header magic + metadata + length field + footer magic must fit
    metadata_len
        .checked_add(12)
        .is_some_and(|need| need <= bytes.len())
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
/// Chunked, not `read_to_end`: the `Read` contract discards bytes produced by
/// a FAILING read call, so a small stream decoded in one call would salvage
/// nothing. Salvage is best-effort by design — bytes decoded before the
/// error are kept even when the error proves them unreliable (checksum
/// mismatch); the warning tells the user the coverage is partial.
fn read_salvaging(key: &str, label: &str, reader: &mut dyn std::io::Read) -> AnyhowResult<Vec<u8>> {
    let mut out = Vec::new();
    let mut chunk = [0u8; 64 * 1024];
    loop {
        match reader.read(&mut chunk) {
            Ok(0) => return Ok(out),
            Ok(n) => out.extend_from_slice(&chunk[..n]),
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
fn avro_to_json_lines(key: &str, bytes: &[u8]) -> AnyhowResult<Vec<u8>> {
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
        out.extend_from_slice(json.to_string().as_bytes());
        out.push(b'\n');
    }
    Ok(out)
}

/// Decode a flow of lz4 frames by cursor: one `read_to_end` call decodes one
/// frame, and the remaining input (via `into_inner`) decides whether to
/// continue — `Ok(0)` alone CANNOT signal exhaustion, because a legal empty
/// frame (`lz4 -c` on empty input) also decodes to zero bytes. Skippable
/// frames may legally sit between data frames; trailing garbage after at
/// least one decoded frame salvages with a warning.
fn decode_lz4_frames(key: &str, bytes: &[u8]) -> AnyhowResult<Vec<u8>> {
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
        match std::io::Read::read_to_end(&mut decoder, &mut out) {
            Ok(_) => {}
            Err(err) if decoded_any || !out.is_empty() => {
                eprintln!(
                    "warning: {key}: lz4 stream ends in garbage ({err}); \
                     searching the {} bytes that decoded",
                    out.len()
                );
                return Ok(out);
            }
            Err(err) => {
                return Err(anyhow::Error::new(err).context(format!("lz4 decode failed for {key}")))
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
fn decode_xz_streams(key: &str, bytes: &[u8]) -> AnyhowResult<Vec<u8>> {
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
pub fn decode_body(key: &str, bytes: Vec<u8>) -> AnyhowResult<Vec<u8>> {
    match detect_codec(&bytes) {
        Codec::Raw => Ok(bytes),
        Codec::Gzip => read_salvaging(
            key,
            "gzip",
            &mut flate2::read::MultiGzDecoder::new(bytes.as_slice()),
        ),
        Codec::Zstd => {
            let mut decoder = zstd::stream::read::Decoder::new(bytes.as_slice())
                .with_context(|| format!("zstd decode failed for {key}"))?;
            read_salvaging(key, "zstd", &mut decoder)
        }
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
        Codec::Lz4Frame => decode_lz4_frames(key, &bytes),
        Codec::Lz4Legacy => anyhow::bail!(
            "{key} is an lz4 LEGACY frame (`lz4 -l` output), which holys3 does \
             not decode; re-compress with the default lz4 frame format"
        ),
        Codec::Xz => decode_xz_streams(key, &bytes),
        Codec::Parquet => parquet_to_json_lines(key, bytes),
        Codec::Avro => avro_to_json_lines(key, &bytes),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{grep_doc, MatchOptions};

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
