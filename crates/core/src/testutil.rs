use super::{Corpus, Doc};
use anyhow::Result;

pub struct MemCorpus {
    docs: Vec<Doc>,
    bodies: Vec<Vec<u8>>,
}

impl MemCorpus {
    pub fn new(keys: Vec<String>, bodies: Vec<Vec<u8>>) -> MemCorpus {
        assert_eq!(keys.len(), bodies.len());
        let docs = keys
            .into_iter()
            .zip(&bodies)
            .map(|(key, body)| Doc {
                key,
                size: body.len() as u64,
            })
            .collect();
        MemCorpus { docs, bodies }
    }
}

impl Corpus for MemCorpus {
    fn docs(&self) -> &[Doc] {
        &self.docs
    }

    fn fetch(&self, idx: usize) -> Result<Vec<u8>> {
        Ok(self.bodies[idx].clone())
    }
}

impl crate::DocFetcher for MemCorpus {
    fn fetch_each(
        &self,
        keys: &[String],
        consume: &mut dyn FnMut(usize, Vec<u8>) -> Result<()>,
    ) -> Result<()> {
        for (idx, key) in keys.iter().enumerate() {
            let pos = self
                .docs
                .iter()
                .position(|doc| doc.key == *key)
                .ok_or_else(|| anyhow::anyhow!("unknown key {key}"))?;
            consume(idx, self.bodies[pos].clone())?;
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
