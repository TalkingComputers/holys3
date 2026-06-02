use anyhow::Result;
use clap::{Parser, Subcommand};
use holys3_core::{matches_in, Corpus};
use holys3_index::{build_to_dir, IndexReader, LocalCorpus};
use std::path::{Path, PathBuf};

#[derive(Parser)]
#[command(name = "holys3")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Build the index for a local dir (Stage 1 testable path) or an S3 prefix.
    Index {
        #[arg(long)]
        local_dir: Option<PathBuf>,
        #[arg(long)]
        bucket: Option<String>,
        #[arg(long, default_value = "")]
        prefix: String,
        #[arg(long, default_value = "holys3.idxdir")]
        out: PathBuf,
    },
    /// Search a pattern using a prebuilt index.
    Search {
        pattern: String,
        #[arg(long)]
        local_dir: Option<PathBuf>,
        #[arg(long, default_value = "holys3.idxdir")]
        index: PathBuf,
        #[arg(long)]
        files_only: bool,
    },
    /// Report distinct grams + term-dict bytes (resolves spec section 5 A/B).
    Stats {
        #[arg(long, default_value = "holys3.idxdir")]
        index: PathBuf,
    },
}

fn build_local(dir: &Path, out: &Path) -> Result<()> {
    let corpus = LocalCorpus::new(dir)?;
    build_to_dir(&corpus, out)?;
    eprintln!("indexed {} docs -> {}", corpus.docs().len(), out.display());
    Ok(())
}

fn search_local(pattern: &str, dir: &Path, index: &Path, files_only: bool) -> Result<()> {
    let corpus = LocalCorpus::new(dir)?;
    let reader = IndexReader::open(index)?;
    let q = holys3_query::plan(pattern)?;
    let re = regex::bytes::Regex::new(pattern)?;
    for id in reader.candidates(&q) {
        let bytes = corpus.fetch(id)?;
        let key = &corpus.docs()[id as usize].1;
        if files_only {
            if re.is_match(&bytes) {
                println!("{key}");
            }
        } else {
            for m in matches_in(id, &bytes, &re) {
                println!("{key}:{}:{}:{}", m.line, m.col, m.text);
            }
        }
    }
    Ok(())
}

fn main() -> Result<()> {
    match Cli::parse().cmd {
        Cmd::Index {
            local_dir: Some(dir),
            out,
            ..
        } => build_local(&dir, &out),
        Cmd::Index {
            bucket: Some(_), ..
        } => {
            anyhow::bail!("S3 indexing is wired via holys3-s3::S3Corpus; enable in Stage 1 follow-up with creds")
        }
        Cmd::Index { .. } => anyhow::bail!("provide --local-dir or --bucket"),
        Cmd::Search {
            pattern,
            local_dir: Some(dir),
            index,
            files_only,
        } => search_local(&pattern, &dir, &index, files_only),
        Cmd::Search { .. } => {
            anyhow::bail!("provide --local-dir (S3 search is a Stage 1 follow-up)")
        }
        Cmd::Stats { index } => {
            let reader = IndexReader::open(&index)?;
            let s = reader.stats();
            println!("distinct_grams={}", s.distinct_grams);
            println!("terms_fst_bytes={}", s.terms_fst_bytes);
            println!("postings_bytes={}", s.postings_bytes);
            Ok(())
        }
    }
}
