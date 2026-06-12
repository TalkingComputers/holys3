#![cfg_attr(docsrs, feature(doc_auto_cfg))]
//! Shared types, gram extraction, storage traits, and scan verification.

mod codec;
mod grams;
mod grep;
mod store;
#[cfg(any(test, feature = "testutil"))]
pub mod testutil;

pub type DocId = u32;

pub use codec::decode_body;
pub use grams::{
    grams_index, grams_query, hash_ngram, sparse_grams_all_bytes, sparse_grams_covering_bytes,
    trigram_grams_bytes, Strategy,
};
pub use grep::{grep_doc, has_line_match, LineEvent, LineKind, MatchOptions, SubMatch};
pub use store::{
    content_version, scan_matching_docs, BlobStore, Corpus, Doc, DocFetcher, LocalBlobStore,
};
