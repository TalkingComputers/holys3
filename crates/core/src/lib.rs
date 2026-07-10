#![cfg_attr(docsrs, feature(doc_auto_cfg))]
//! Shared types, gram extraction, storage traits, and scan verification.

mod codec;
mod grams;
mod grep;
mod store;
#[cfg(any(test, feature = "testutil"))]
pub mod testutil;

pub type DocId = u32;

pub use codec::{
    decode_body, decode_requested, decode_source, is_raw_source, DecodeLimits, DecodeSink,
    DecodeSummary, LogicalDocumentMeta, SourceEncoding, DECODE_LIMITS,
};
pub use grams::{
    grams_index, grams_query, hash_ngram, iterate_sparse_grams, pack_trigram_grams,
    sparse_grams_all_bytes, sparse_grams_covering_bytes, trigram_grams_bytes, Strategy,
};
pub use grep::{
    can_search_as_document, grep_bytes, grep_bytes_fast, grep_doc, has_line_match,
    has_line_match_fast, LineEvent, LineKind, MatchOptions, SubMatch,
};
pub use store::{
    content_version, scan_matching_docs, BlobStore, Corpus, DocAddress, DocFetcher, LocalBlobStore,
    SourceObject, StaleSource,
};
