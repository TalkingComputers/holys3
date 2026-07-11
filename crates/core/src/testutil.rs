use super::{content_version, Corpus, SourceObject, StaleSource};
use anyhow::Result;
use bytes::Bytes;

pub struct MemCorpus {
    sources: Vec<SourceObject>,
    bodies: Vec<Vec<u8>>,
}

impl MemCorpus {
    pub fn new(keys: Vec<String>, bodies: Vec<Vec<u8>>) -> MemCorpus {
        assert_eq!(keys.len(), bodies.len());
        let sources = keys
            .into_iter()
            .zip(&bodies)
            .map(|(key, body)| SourceObject {
                key,
                version: content_version(body),
                encoded_size: body.len() as u64,
            })
            .collect();
        MemCorpus { sources, bodies }
    }
}

impl Corpus for MemCorpus {
    fn sources(&self) -> &[SourceObject] {
        &self.sources
    }

    fn fetch(&self, idx: usize) -> Result<Bytes> {
        Ok(Bytes::from(self.bodies[idx].clone()))
    }
}

impl crate::DocFetcher for MemCorpus {
    fn fetch_each(
        &self,
        documents: &[crate::DocAddress],
        consume: &mut dyn FnMut(usize, crate::DocumentBody) -> Result<()>,
    ) -> Result<()> {
        let mut groups = std::collections::BTreeMap::new();
        for (idx, document) in documents.iter().enumerate() {
            groups
                .entry((document.source_key.clone(), document.source_version.clone()))
                .or_insert_with(Vec::new)
                .push((idx, document.member_path.clone()));
        }
        for ((key, version), requests) in groups {
            let pos = self
                .sources
                .iter()
                .position(|source| source.key == key)
                .ok_or_else(|| anyhow::anyhow!("unknown key {key}"))?;
            if self.sources[pos].version != version {
                return Err(anyhow::Error::new(StaleSource {
                    key,
                    expected: version,
                }));
            }
            crate::decode_requested(
                &key,
                &requests,
                Bytes::from(self.bodies[pos].clone()),
                consume,
            )?;
        }
        Ok(())
    }
}

/// Format fixtures for differential tests: each encoder produces real
/// bytes of its format so engine-vs-oracle equality covers every codec.
pub mod encode {
    use std::io::Write;

    pub fn zip(entries: &[(&str, &[u8])]) -> Vec<u8> {
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

    pub fn tar(entries: &[(&str, &[u8])]) -> Vec<u8> {
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

    pub fn brotli(data: &[u8]) -> Vec<u8> {
        let mut enc = brotli::CompressorWriter::new(Vec::new(), 64 * 1024, 5, 22);
        enc.write_all(data).unwrap();
        enc.into_inner()
    }

    pub fn zlib(data: &[u8]) -> Vec<u8> {
        let mut enc = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
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

    pub fn arrow_of_lines(lines: &[&str]) -> Vec<u8> {
        use arrow_array::{ArrayRef, RecordBatch, StringArray};
        use std::sync::Arc;
        let batch = RecordBatch::try_from_iter(vec![(
            "line",
            Arc::new(StringArray::from(lines.to_vec())) as ArrayRef,
        )])
        .unwrap();
        let mut writer =
            arrow_ipc::writer::FileWriter::try_new(Vec::new(), batch.schema().as_ref()).unwrap();
        writer.write(&batch).unwrap();
        writer.into_inner().unwrap()
    }

    pub fn orc_of_lines(lines: &[&str]) -> Vec<u8> {
        use arrow_array_58::{ArrayRef, RecordBatch, StringArray};
        use std::sync::Arc;
        let batch = RecordBatch::try_from_iter(vec![(
            "line",
            Arc::new(StringArray::from(lines.to_vec())) as ArrayRef,
        )])
        .unwrap();
        let mut bytes = Vec::new();
        let mut writer = orc_rust::ArrowWriterBuilder::new(&mut bytes, batch.schema())
            .try_build()
            .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        bytes
    }
}
