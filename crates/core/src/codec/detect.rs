use prost::Message;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Codec {
    Raw,
    Gzip,
    Zstd,
    Bzip2,
    SnappyFrame,
    Lz4Frame,
    Lz4Legacy,
    Xz,
    Parquet,
    Avro,
    Zip,
    Tar,
    ArrowIpc,
    Brotli,
    Zlib,
    Orc,
    ArrowIpcStream,
}

const SNAPPY_FRAME_MAGIC: [u8; 10] = [0xff, 0x06, 0x00, 0x00, b's', b'N', b'a', b'P', b'p', b'Y'];

pub(super) fn skip_skippable_frames(bytes: &[u8]) -> Option<usize> {
    let mut at = 0usize;
    while let Some(rest) = bytes.get(at..) {
        if rest.len() >= 8 && rest[0] & 0xf0 == 0x50 && rest[1..4] == [0x2a, 0x4d, 0x18] {
            let size = u32::from_le_bytes(rest[4..8].try_into().expect("4 bytes")) as usize;
            match 8usize
                .checked_add(size)
                .and_then(|size| at.checked_add(size))
            {
                Some(next) if next <= bytes.len() => at = next,
                _ => return None,
            }
        } else {
            break;
        }
    }
    Some(at)
}

pub(super) fn detect_codec(bytes: &[u8]) -> Codec {
    let Some(at) = skip_skippable_frames(bytes) else {
        return Codec::Raw;
    };
    if at > 0 {
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
    } else if is_orc(bytes) {
        Codec::Orc
    } else if bytes.starts_with(b"PK\x03\x04")
        || bytes.starts_with(b"PK\x05\x06")
        || bytes.starts_with(b"PK\x07\x08")
    {
        Codec::Zip
    } else if is_tar(bytes) {
        Codec::Tar
    } else if is_arrow_ipc(bytes) {
        Codec::ArrowIpc
    } else if is_arrow_ipc_stream(bytes, false) {
        Codec::ArrowIpcStream
    } else {
        Codec::Raw
    }
}

pub(super) fn detect_codec_for_key(key: &str, bytes: &[u8]) -> Codec {
    let codec = detect_codec(bytes);
    if codec != Codec::Raw {
        return codec;
    }
    let key = key.to_ascii_lowercase();
    if (key.ends_with(".arrow") || key.ends_with(".arrows") || key.ends_with(".ipc"))
        && is_arrow_ipc_stream(bytes, true)
    {
        Codec::ArrowIpcStream
    } else if key.ends_with(".br") {
        Codec::Brotli
    } else if key.ends_with(".zlib") || key.ends_with(".zz") {
        Codec::Zlib
    } else {
        Codec::Raw
    }
}

fn is_arrow_ipc(bytes: &[u8]) -> bool {
    bytes.len() >= 12
        && bytes.starts_with(b"ARROW1")
        && bytes.ends_with(b"ARROW1")
        && arrow_ipc::reader::FileReader::try_new(std::io::Cursor::new(bytes), None).is_ok()
}

fn is_orc(bytes: &[u8]) -> bool {
    if !bytes.starts_with(b"ORC") || bytes.len() < 5 {
        return false;
    }
    let postscript_len = bytes[bytes.len() - 1] as usize;
    let Some(postscript_at) = bytes.len().checked_sub(postscript_len + 1) else {
        return false;
    };
    if postscript_len == 0 || postscript_at < 3 {
        return false;
    }
    let Ok(postscript) =
        orc_rust::proto::PostScript::decode(&bytes[postscript_at..bytes.len() - 1])
    else {
        return false;
    };
    if postscript
        .magic
        .as_deref()
        .is_some_and(|magic| magic != "ORC")
        || postscript.compression.is_some_and(|compression| {
            orc_rust::proto::CompressionKind::try_from(compression).is_err()
        })
        || postscript
            .compression_block_size
            .is_some_and(|size| size > 64 * 1024 * 1024)
    {
        return false;
    }
    let Some(footer_len) = postscript.footer_length else {
        return false;
    };
    let Some(metadata_len) = postscript.metadata_length else {
        return false;
    };
    footer_len > 0
        && footer_len
            .checked_add(metadata_len)
            .and_then(|len| len.checked_add(postscript_len as u64 + 1))
            .is_some_and(|len| len <= bytes.len() as u64)
}

fn is_arrow_ipc_stream(bytes: &[u8], allow_legacy: bool) -> bool {
    let (length_at, metadata_at) = if bytes.starts_with(&[0xff; 4]) {
        (4usize, 8usize)
    } else if allow_legacy {
        (0usize, 4usize)
    } else {
        return false;
    };
    let Some(length) = bytes.get(length_at..length_at + 4) else {
        return false;
    };
    let length = i32::from_le_bytes(length.try_into().expect("4 bytes"));
    if length <= 0
        || usize::try_from(length)
            .ok()
            .and_then(|length| metadata_at.checked_add(length))
            .is_none_or(|end| end > bytes.len())
    {
        return false;
    }
    arrow_ipc::reader::StreamReader::try_new(bytes, None).is_ok()
}

fn is_tar(bytes: &[u8]) -> bool {
    let Some(header_bytes) = bytes.get(..512) else {
        return false;
    };
    if header_bytes.get(257..262) != Some(b"ustar") {
        return false;
    }
    let header = tar::Header::from_byte_slice(header_bytes);
    let Ok(stored) = header.cksum() else {
        return false;
    };
    let mut expected = header.clone();
    expected.set_cksum();
    expected.cksum().is_ok_and(|checksum| checksum == stored)
}

fn is_parquet(bytes: &[u8]) -> bool {
    if bytes.len() < 12 || !bytes.starts_with(b"PAR1") || !bytes.ends_with(b"PAR1") {
        return false;
    }
    let len_field = &bytes[bytes.len() - 8..bytes.len() - 4];
    let metadata_len = u32::from_le_bytes(len_field.try_into().expect("4 bytes")) as usize;
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
