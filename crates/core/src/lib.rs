#![cfg_attr(docsrs, feature(doc_auto_cfg))]
//! Shared types, gram extraction, storage traits, and scan verification.

mod cache;
mod codec;
mod detect;
mod grams;
mod grep;
mod progress;
mod store;
#[cfg(any(test, feature = "testutil"))]
pub mod testutil;

pub type DocId = u32;

pub use cache::{cache_home, hash_cache_scope, read_cache_home};
pub use codec::{
    decode_body, decode_requested, decode_requested_body, decode_source, decode_source_body,
    is_raw_body, is_raw_source, DecodeLimits, DecodeSink, DecodeSummary, DocumentBody,
    DocumentReader, DocumentSpool, LogicalDocumentMeta, SourceEncoding, DECODE_LIMITS,
};
pub use detect::is_prose_like;
pub use grams::{
    grams_index, grams_query, hash_ngram, iterate_sparse_gram_ranges, iterate_sparse_grams,
    pack_trigram_block_grams, pack_trigram_grams, sparse_grams_all_bytes,
    sparse_grams_covering_bytes, start_sparse_gram_ranges, trigram_grams_bytes, SparseGramRanges,
    Strategy, CANDIDATE_BLOCK_BYTES,
};
pub use grep::{
    bounded_match_len, can_search_as_document, grep_bytes, grep_bytes_fast, grep_doc,
    has_line_match, has_line_match_fast, parse_pattern, LineEvent, LineKind, MatchOptions,
    SubMatch,
};
pub use progress::{ProgressEvent, ProgressSender};
pub use store::{
    content_version, scan_matching_docs, BlobStore, Corpus, DocAddress, DocFetcher, DocumentRegion,
    FetchedDocument, IndexAddress, LocalBlobStore, SourceObject, StaleSource, StreamingPut,
};
