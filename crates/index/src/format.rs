use crate::pack::{PackBlock, PACK_BLOCK_BYTES};
use anyhow::{Context, Result};
use seagrep_core::SourceEncoding;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct SourceEntry {
    pub key: String,
    pub version: String,
    pub encoded_size: u64,
    pub encoding: SourceEncoding,
    pub first_doc: u32,
    pub doc_count: u32,
    pub failed: bool,
    pub retry: bool,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct DocEntry {
    pub display_key: String,
    pub source_id: u32,
    pub member_path: Option<String>,
    pub decoded_size: u64,
    pub first_block: u32,
    pub block_offset: u32,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct SegmentTables {
    pub sources: Vec<SourceEntry>,
    pub documents: Vec<DocEntry>,
    pub blocks: Vec<PackBlock>,
}

#[derive(Default, Serialize, Deserialize, Clone)]
pub(crate) struct DeadSet {
    pub sources: Vec<u32>,
    pub documents: Vec<u32>,
}

impl SegmentTables {
    pub(crate) fn validate(&self) -> Result<()> {
        let mut previous_source = None;
        let mut seen_documents = HashSet::with_capacity(self.documents.len());
        let mut expected_first_doc = 0usize;
        for (source_id, source) in self.sources.iter().enumerate() {
            if let Some(previous) = previous_source {
                anyhow::ensure!(
                    previous < source.key.as_str(),
                    "segment sources are not unique and sorted"
                );
            }
            previous_source = Some(source.key.as_str());
            let start = usize::try_from(source.first_doc)?;
            let count = usize::try_from(source.doc_count)?;
            let end = start
                .checked_add(count)
                .context("source document range overflows")?;
            anyhow::ensure!(
                start == expected_first_doc,
                "source document ranges are not contiguous"
            );
            anyhow::ensure!(
                end <= self.documents.len(),
                "source document range is out of bounds"
            );
            anyhow::ensure!(
                !source.failed || count == 0,
                "failed source contains documents"
            );
            anyhow::ensure!(
                !source.retry || source.failed,
                "retryable source is not failed"
            );
            for document in &self.documents[start..end] {
                anyhow::ensure!(
                    usize::try_from(document.source_id)? == source_id,
                    "document points to the wrong source"
                );
                match &document.member_path {
                    Some(member_path) => anyhow::ensure!(
                        !member_path.is_empty()
                            && document.display_key == format!("{}!/{member_path}", source.key),
                        "document display key does not match its member path"
                    ),
                    None => anyhow::ensure!(
                        document.display_key == source.key,
                        "source document display key does not match its source"
                    ),
                }
                anyhow::ensure!(
                    seen_documents.insert(document.display_key.as_str()),
                    "duplicate document display key"
                );
            }
            anyhow::ensure!(
                self.documents[start..end]
                    .windows(2)
                    .all(|pair| pair[0].display_key < pair[1].display_key),
                "source documents are not sorted"
            );
            expected_first_doc = end;
        }
        anyhow::ensure!(
            expected_first_doc == self.documents.len(),
            "unowned documents remain"
        );
        anyhow::ensure!(
            self.documents.iter().enumerate().all(|(id, document)| {
                self.sources
                    .get(document.source_id as usize)
                    .is_some_and(|source| {
                        let start = source.first_doc as usize;
                        id >= start && id < start + source.doc_count as usize
                    })
            }),
            "document lies outside its source range"
        );
        let mut previous_pack = None;
        let mut expected_offset = 0u64;
        for block in &self.blocks {
            anyhow::ensure!(
                block.compressed_len > 0,
                "pack block compressed length is zero"
            );
            anyhow::ensure!(
                block.decoded_len > 0 && block.decoded_len as usize <= PACK_BLOCK_BYTES,
                "pack block decoded length is invalid"
            );
            if previous_pack != Some(block.pack) {
                anyhow::ensure!(
                    previous_pack.is_none_or(|pack| block.pack == pack + 1),
                    "pack IDs are not contiguous"
                );
                expected_offset = 0;
                previous_pack = Some(block.pack);
            }
            anyhow::ensure!(
                block.offset == expected_offset,
                "pack blocks are not contiguous"
            );
            expected_offset = expected_offset
                .checked_add(u64::from(block.compressed_len))
                .context("pack block offsets overflow")?;
        }
        let mut next_block = 0usize;
        let mut next_offset = 0usize;
        for document in &self.documents {
            if document.decoded_size == 0 {
                anyhow::ensure!(
                    document.first_block == 0 && document.block_offset == 0,
                    "empty document has pack coordinates"
                );
                continue;
            }
            anyhow::ensure!(
                usize::try_from(document.first_block)? == next_block
                    && usize::try_from(document.block_offset)? == next_offset,
                "document pack coordinates are not contiguous"
            );
            let mut remaining = document.decoded_size;
            while remaining > 0 {
                let block = self
                    .blocks
                    .get(next_block)
                    .context("document points outside pack blocks")?;
                let available = usize::try_from(block.decoded_len)?
                    .checked_sub(next_offset)
                    .context("document block offset is out of bounds")?;
                let consumed = remaining.min(u64::try_from(available)?);
                remaining -= consumed;
                next_offset += usize::try_from(consumed)?;
                if next_offset == usize::try_from(block.decoded_len)? {
                    next_block += 1;
                    next_offset = 0;
                }
            }
        }
        anyhow::ensure!(
            next_block == self.blocks.len() && next_offset == 0,
            "unowned decoded bytes remain in pack blocks"
        );
        Ok(())
    }
}

impl DeadSet {
    pub(crate) fn validate(&self, tables: &SegmentTables) -> Result<()> {
        anyhow::ensure!(
            self.sources
                .last()
                .is_none_or(|source| (*source as usize) < tables.sources.len()),
            "dead source ID is out of bounds"
        );
        anyhow::ensure!(
            self.documents
                .last()
                .is_none_or(|document| (*document as usize) < tables.documents.len()),
            "dead document ID is out of bounds"
        );
        for source_id in &self.sources {
            let source = &tables.sources[*source_id as usize];
            let end = source
                .first_doc
                .checked_add(source.doc_count)
                .context("dead source document range overflows")?;
            anyhow::ensure!(
                (source.first_doc..end)
                    .all(|document| self.documents.binary_search(&document).is_ok()),
                "dead source has live documents"
            );
        }
        for document_id in &self.documents {
            let document = &tables.documents[*document_id as usize];
            anyhow::ensure!(
                self.sources.binary_search(&document.source_id).is_ok(),
                "dead document belongs to a live source"
            );
        }
        Ok(())
    }
}

pub(crate) fn parse_tables(bytes: &[u8]) -> Result<SegmentTables> {
    let tables: SegmentTables =
        postcard::from_bytes(bytes).context("segment docs.bin unreadable")?;
    tables.validate()?;
    Ok(tables)
}

pub(crate) fn parse_dead(bytes: &[u8]) -> Result<DeadSet> {
    let dead: DeadSet = postcard::from_bytes(bytes).context("segment dead set unreadable")?;
    anyhow::ensure!(
        dead.sources.windows(2).all(|pair| pair[0] < pair[1]),
        "dead source IDs are not sorted and unique"
    );
    anyhow::ensure!(
        dead.documents.windows(2).all(|pair| pair[0] < pair[1]),
        "dead document IDs are not sorted and unique"
    );
    Ok(dead)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tables() -> SegmentTables {
        SegmentTables {
            sources: vec![
                SourceEntry {
                    key: "a.zip".into(),
                    version: "a1".into(),
                    encoded_size: 10,
                    encoding: SourceEncoding::Zip,
                    first_doc: 0,
                    doc_count: 2,
                    failed: false,
                    retry: false,
                },
                SourceEntry {
                    key: "b".into(),
                    version: "b1".into(),
                    encoded_size: 3,
                    encoding: SourceEncoding::Raw,
                    first_doc: 2,
                    doc_count: 1,
                    failed: false,
                    retry: false,
                },
            ],
            documents: vec![
                DocEntry {
                    display_key: "a.zip!/a".into(),
                    source_id: 0,
                    member_path: Some("a".into()),
                    decoded_size: 1,
                    first_block: 0,
                    block_offset: 0,
                },
                DocEntry {
                    display_key: "a.zip!/b".into(),
                    source_id: 0,
                    member_path: Some("b".into()),
                    decoded_size: 1,
                    first_block: 0,
                    block_offset: 1,
                },
                DocEntry {
                    display_key: "b".into(),
                    source_id: 1,
                    member_path: None,
                    decoded_size: 3,
                    first_block: 0,
                    block_offset: 2,
                },
            ],
            blocks: vec![PackBlock {
                pack: 0,
                offset: 0,
                compressed_len: 1,
                decoded_len: 5,
                hash: [0; 32],
            }],
        }
    }

    #[test]
    fn accepts_valid_tables() {
        tables().validate().unwrap();
    }

    #[test]
    fn rejects_invalid_source_and_document_tables() {
        let mut cases = Vec::new();

        let mut unsorted_sources = tables();
        unsorted_sources.sources.swap(0, 1);
        cases.push(unsorted_sources);

        let mut noncontiguous = tables();
        noncontiguous.sources[1].first_doc = 1;
        cases.push(noncontiguous);

        let mut wrong_source = tables();
        wrong_source.documents[0].source_id = 1;
        cases.push(wrong_source);

        let mut outside_source = tables();
        outside_source.documents[0].display_key = "elsewhere".into();
        cases.push(outside_source);

        let mut mismatched_member = tables();
        mismatched_member.documents[0].member_path = Some("elsewhere".into());
        cases.push(mismatched_member);

        let mut out_of_range = tables();
        out_of_range.sources[1].doc_count = 2;
        cases.push(out_of_range);

        let mut duplicate_document = tables();
        duplicate_document.documents[1].display_key = "a.zip!/a".into();
        cases.push(duplicate_document);

        for case in cases {
            assert!(case.validate().is_err());
        }
    }

    #[test]
    fn rejects_inconsistent_dead_sets() {
        let tables = tables();
        DeadSet {
            sources: vec![0],
            documents: vec![0, 1],
        }
        .validate(&tables)
        .unwrap();
        for dead in [
            DeadSet {
                sources: vec![2],
                documents: Vec::new(),
            },
            DeadSet {
                sources: vec![0],
                documents: vec![0],
            },
            DeadSet {
                sources: Vec::new(),
                documents: vec![2],
            },
        ] {
            assert!(dead.validate(&tables).is_err());
        }
    }
}
