use super::detect::{skip_skippable_frames, Codec};
use super::{DecodeWriter, DocumentBody, DocumentReader};
use anyhow::{Context, Result as AnyhowResult};
use std::io::Read;

pub(super) fn read_salvaging(
    key: &str,
    label: &str,
    reader: &mut dyn Read,
    limit: Option<u64>,
    capacity: usize,
    memory_limit: usize,
) -> AnyhowResult<DocumentBody> {
    let mut out = DecodeWriter::new(key, limit, capacity, memory_limit);
    let mut chunk = [0u8; 64 * 1024];
    loop {
        match reader.read(&mut chunk) {
            Ok(0) => return out.finish(),
            Ok(n) => out.append(&chunk[..n])?,
            Err(err) if err.kind() == std::io::ErrorKind::Interrupted => {}
            Err(err) if !out.is_empty() => {
                eprintln!(
                    "warning: {key}: {label} stream ends in garbage ({err}); \
                     searching the {} bytes that decoded",
                    out.len()
                );
                return out.finish();
            }
            Err(err) => {
                return Err(
                    anyhow::Error::new(err).context(format!("{label} decode failed for {key}"))
                )
            }
        }
    }
}

pub(super) fn gzip_capacity(bytes: &[u8], limit: Option<u64>) -> usize {
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

pub(super) fn decode_lz4_frames(
    key: &str,
    bytes: &[u8],
    limit: Option<u64>,
    memory_limit: usize,
) -> AnyhowResult<DocumentBody> {
    let mut out = DecodeWriter::new(key, limit, 0, memory_limit);
    let mut rest = bytes;
    let mut decoded_any = false;
    loop {
        let skipped = match skip_skippable_frames(rest) {
            Some(at) => &rest[at..],
            None => &[][..],
        };
        if skipped.is_empty() {
            return out.finish();
        }
        if !skipped.starts_with(&[0x04, 0x22, 0x4d, 0x18]) {
            if decoded_any {
                eprintln!(
                    "warning: {key}: lz4 stream ends in garbage; \
                     searching the {} bytes that decoded",
                    out.len()
                );
                return out.finish();
            }
            anyhow::bail!("lz4 decode failed for {key}: input is not an lz4 frame");
        }
        let mut decoder = lz4_flex::frame::FrameDecoder::new(skipped);
        let mut chunk = [0u8; 64 * 1024];
        loop {
            match decoder.read(&mut chunk) {
                Ok(0) => break,
                Ok(read) => out.append(&chunk[..read])?,
                Err(err) if decoded_any || !out.is_empty() => {
                    eprintln!(
                        "warning: {key}: lz4 stream ends in garbage ({err}); \
                         searching the {} bytes that decoded",
                        out.len()
                    );
                    return out.finish();
                }
                Err(err) => {
                    return Err(
                        anyhow::Error::new(err).context(format!("lz4 decode failed for {key}"))
                    )
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

pub(super) fn decode_xz_streams(
    key: &str,
    bytes: &[u8],
    limit: Option<u64>,
    memory_limit: usize,
) -> AnyhowResult<DocumentBody> {
    let mut out = DecodeWriter::new(key, limit, 0, memory_limit);
    let mut pos = 0usize;
    let mut chunk = vec![0u8; 256 * 1024];
    loop {
        while bytes.get(pos) == Some(&0) {
            pos += 1;
        }
        let rest = &bytes[pos..];
        if rest.is_empty() {
            return out.finish();
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
            return out.finish();
        }
        let mut stream = liblzma::stream::Stream::new_stream_decoder(u64::MAX, 0)
            .with_context(|| format!("xz decoder init failed for {key}"))?;
        let mut emitted = 0u64;
        loop {
            let input = &rest[usize::try_from(stream.total_in())?..];
            let result = stream.process(input, &mut chunk, liblzma::stream::Action::Run);
            let written = usize::try_from(stream.total_out() - emitted)?;
            out.append(&chunk[..written])?;
            emitted = stream.total_out();
            match result {
                Ok(liblzma::stream::Status::StreamEnd) => break,
                Ok(_) if input.is_empty() && written == 0 => {
                    anyhow::ensure!(!out.is_empty(), "xz stream of {key} is truncated");
                    eprintln!(
                        "warning: {key}: xz stream is truncated; \
                         searching the {} bytes that decoded",
                        out.len()
                    );
                    return out.finish();
                }
                Ok(_) => {}
                Err(err) if !out.is_empty() => {
                    eprintln!(
                        "warning: {key}: xz stream ends in garbage ({err}); \
                         searching the {} bytes that decoded",
                        out.len()
                    );
                    return out.finish();
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

pub(super) fn can_decode_stream(codec: Codec) -> bool {
    matches!(
        codec,
        Codec::Gzip | Codec::Zstd | Codec::Bzip2 | Codec::SnappyFrame | Codec::Brotli | Codec::Zlib
    )
}

pub(super) fn decode_stream(
    key: &str,
    codec: Codec,
    reader: DocumentReader,
    limit: Option<u64>,
    memory_limit: usize,
) -> AnyhowResult<DocumentBody> {
    match codec {
        Codec::Gzip => read_salvaging(
            key,
            "gzip",
            &mut flate2::read::MultiGzDecoder::new(std::io::BufReader::with_capacity(
                64 * 1024,
                reader,
            )),
            limit,
            0,
            memory_limit,
        ),
        Codec::Zstd => {
            let mut decoder = zstd::stream::read::Decoder::new(reader)
                .with_context(|| format!("zstd decode failed for {key}"))?;
            read_salvaging(key, "zstd", &mut decoder, limit, 0, memory_limit)
        }
        Codec::Bzip2 => read_salvaging(
            key,
            "bzip2",
            &mut bzip2::read::MultiBzDecoder::new(std::io::BufReader::with_capacity(
                64 * 1024,
                reader,
            )),
            limit,
            0,
            memory_limit,
        ),
        Codec::SnappyFrame => read_salvaging(
            key,
            "snappy",
            &mut snap::read::FrameDecoder::new(std::io::BufReader::with_capacity(
                64 * 1024,
                reader,
            )),
            limit,
            0,
            memory_limit,
        ),
        Codec::Brotli => read_strict(
            key,
            "brotli",
            &mut brotli::Decompressor::new(reader, 64 * 1024),
            limit,
            memory_limit,
        ),
        Codec::Zlib => read_strict(
            key,
            "zlib",
            &mut flate2::read::ZlibDecoder::new(std::io::BufReader::with_capacity(
                64 * 1024,
                reader,
            )),
            limit,
            memory_limit,
        ),
        _ => unreachable!("non-streaming codec passed to decode_stream"),
    }
}

pub(super) fn read_strict(
    key: &str,
    label: &str,
    reader: &mut dyn Read,
    limit: Option<u64>,
    memory_limit: usize,
) -> AnyhowResult<DocumentBody> {
    let mut out = DecodeWriter::new(key, limit, 0, memory_limit);
    let mut chunk = [0u8; 64 * 1024];
    loop {
        let read = reader
            .read(&mut chunk)
            .with_context(|| format!("{label} decode failed for {key}"))?;
        if read == 0 {
            return out.finish();
        }
        out.append(&chunk[..read])?;
    }
}
