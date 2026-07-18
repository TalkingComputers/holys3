//! Fuzzes the canonical decode boundary: every byte an index build or a
//! verifier reads from a bucket flows through decode_source, so this is the
//! hostile-input surface. Errors are fine (undecodable objects are excluded
//! loudly); panics, hangs, and unbounded allocation are bugs.

#![no_main]

use libfuzzer_sys::fuzz_target;
use seagrep_core::{decode_source, DecodeLimits, DecodeSink, LogicalDocumentMeta};

struct DrainSink {
    bytes: u64,
}

impl DecodeSink for DrainSink {
    fn begin(&mut self, _document: &LogicalDocumentMeta) -> anyhow::Result<()> {
        Ok(())
    }

    fn write(&mut self, bytes: &[u8]) -> anyhow::Result<()> {
        self.bytes += bytes.len() as u64;
        Ok(())
    }

    fn finish(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

// Small caps keep iterations fast while still exercising the nesting,
// member-count, and expansion-limit paths.
const LIMITS: DecodeLimits = DecodeLimits {
    max_depth: 4,
    max_members: 64,
    max_expanded_bytes: 1 << 22,
};

fuzz_target!(|data: &[u8]| {
    // The key picks extension-hinted decoders (brotli/zlib have no magic);
    // derive it from the first byte so the fuzzer can reach those paths.
    let key = match data.first().map(|b| b % 5) {
        Some(0) => "fuzz.br",
        Some(1) => "fuzz.zlib",
        Some(2) => "fuzz.zz",
        Some(3) => "fuzz.gz",
        _ => "fuzz.bin",
    };
    let mut sink = DrainSink { bytes: 0 };
    let _ = decode_source(key, bytes::Bytes::copy_from_slice(data), LIMITS, &mut sink);
});
