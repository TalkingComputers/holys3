use anyhow::{Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use holys3_core::{matches_in, Corpus, Strategy};
use holys3_index::{
    build_to_dir, build_to_store, compute_build_id, search, IndexReader, LocalCorpus,
    MmapIndexReader, StoreIndexReader,
};
use holys3_s3::{
    build_index_namespace, is_index_key, normalize_prefix, FetchConfig, ObjectMeta, S3BlobStore,
    S3Client, S3Corpus,
};
use holys3_sigv4::resolve;
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
        #[arg(long)]
        region: Option<String>,
        #[arg(long, default_value = "holys3.idxdir")]
        out: PathBuf,
        #[arg(long, value_enum, default_value = "trigram")]
        strategy: StrategyArg,
        #[arg(long, default_value_t = 750, value_parser = parse_concurrency)]
        concurrency: usize,
    },
    /// Search a pattern using a prebuilt index.
    Search {
        pattern: String,
        #[arg(long)]
        local_dir: Option<PathBuf>,
        #[arg(long)]
        bucket: Option<String>,
        #[arg(long, default_value = "")]
        prefix: String,
        #[arg(long)]
        region: Option<String>,
        #[arg(long, default_value = "holys3.idxdir")]
        index: PathBuf,
        #[arg(long)]
        files_only: bool,
        #[arg(long)]
        stats: bool,
        #[arg(long, default_value_t = 750, value_parser = parse_concurrency)]
        concurrency: usize,
    },
    /// Report distinct grams + term-dict bytes (resolves spec section 5 A/B).
    Stats {
        #[arg(long, default_value = "holys3.idxdir")]
        index: PathBuf,
    },
}

#[derive(Clone, Copy, ValueEnum)]
enum StrategyArg {
    Trigram,
    Sparse,
}

impl From<StrategyArg> for Strategy {
    fn from(value: StrategyArg) -> Strategy {
        match value {
            StrategyArg::Trigram => Strategy::Trigram,
            StrategyArg::Sparse => Strategy::Sparse,
        }
    }
}

fn build_local(dir: &Path, out: &Path, strategy: Strategy) -> Result<()> {
    let corpus = LocalCorpus::new(dir)?;
    build_to_dir(&corpus, out, strategy)?;
    eprintln!("indexed {} docs -> {}", corpus.docs().len(), out.display());
    Ok(())
}

fn build_fetch_config(concurrency: usize) -> FetchConfig {
    let default = FetchConfig::default();
    FetchConfig {
        start: default.start.min(concurrency),
        cap: concurrency,
        ..default
    }
}

fn parse_concurrency(value: &str) -> std::result::Result<usize, String> {
    let concurrency = value.parse::<usize>().map_err(|err| err.to_string())?;
    if concurrency == 0 {
        return Err("concurrency must be greater than 0".to_owned());
    }
    Ok(concurrency)
}

async fn build_s3(
    bucket: String,
    prefix: String,
    region: Option<String>,
    strategy: Strategy,
    concurrency: usize,
) -> Result<()> {
    let prefix = normalize_prefix(&prefix);
    let cfg = build_fetch_config(concurrency);
    let region = read_region(region)?;
    let creds = resolve("default")?;
    let client = S3Client::new(region, creds, None, false);
    let objects = select_user_objects(client.list(&bucket, &prefix).await?, &prefix);
    let object_ids = objects
        .iter()
        .map(|object| (object.key.clone(), object.etag.clone()))
        .collect::<Vec<_>>();
    let build_id = compute_build_id(&object_ids);
    let rt = tokio::runtime::Handle::current();
    let corpus = S3Corpus::new(
        client.clone(),
        bucket.clone(),
        objects,
        rt.clone(),
        cfg.clone(),
    )?;
    let store = S3BlobStore::new(client, bucket.clone(), prefix.clone(), rt, cfg)?;
    build_to_store(&corpus, &store, strategy, &build_id)?;
    eprintln!(
        "indexed {} docs -> s3://{}/{}/builds/{}",
        corpus.docs().len(),
        bucket,
        build_index_namespace(&prefix),
        build_id
    );
    Ok(())
}

fn search_local(
    pattern: &str,
    dir: &Path,
    index: &Path,
    files_only: bool,
    stats: bool,
) -> Result<()> {
    let corpus = LocalCorpus::new(dir)?;
    let reader = MmapIndexReader::open(index)?;
    if stats {
        let q = holys3_query::plan(pattern, reader.strategy())?;
        let candidates = reader.candidates(&q)?;
        eprintln!(
            "candidates={} total={} strategy={:?}",
            candidates.len(),
            reader.docs().len(),
            reader.strategy()
        );
    }
    print_hits(
        &corpus,
        search(&reader, &corpus, pattern)?,
        pattern,
        files_only,
    )
}

fn read_region(region: Option<String>) -> Result<String> {
    match region {
        Some(region) => Ok(region),
        None => Ok(std::env::var("AWS_REGION").context("provide --region or set AWS_REGION")?),
    }
}

fn select_user_objects(objects: Vec<ObjectMeta>, prefix: &str) -> Vec<ObjectMeta> {
    objects
        .into_iter()
        .filter(|object| !is_index_key(prefix, &object.key))
        .collect()
}

fn build_cache_dir(bucket: &str, prefix: &str) -> Result<PathBuf> {
    let mut path = PathBuf::from(std::env::var("HOME")?);
    path.push(".cache");
    path.push("holys3");
    path.push(bucket);
    let prefix = prefix.replace('/', "__");
    if !prefix.is_empty() {
        path.push(prefix);
    }
    Ok(path)
}

fn search_s3(
    pattern: &str,
    bucket: String,
    prefix: String,
    region: Option<String>,
    files_only: bool,
    stats: bool,
    concurrency: usize,
) -> Result<()> {
    let prefix = normalize_prefix(&prefix);
    let cfg = build_fetch_config(concurrency);
    let region = read_region(region)?;
    let creds = resolve("default")?;
    let client = S3Client::new(region, creds, None, false);
    let rt = tokio::runtime::Handle::current();
    let store = S3BlobStore::new(
        client.clone(),
        bucket.clone(),
        prefix.clone(),
        rt.clone(),
        cfg.clone(),
    )?;
    let cache_dir = build_cache_dir(&bucket, &prefix)?;
    let reader = StoreIndexReader::open(Box::new(store), &cache_dir)?;
    if stats {
        let q = holys3_query::plan(pattern, reader.strategy())?;
        let candidates = reader.candidates(&q)?;
        let index_stats = reader.stats();
        eprintln!(
            "candidates={} total={} strategy={:?} distinct_grams={} terms_fst_bytes={} postings_bytes={}",
            candidates.len(),
            reader.docs().len(),
            reader.strategy(),
            index_stats.distinct_grams,
            index_stats.terms_fst_bytes,
            index_stats.postings_bytes
        );
    }
    let corpus = S3Corpus::from_docs(client, bucket, reader.docs().to_vec(), rt, cfg)?;
    print_hits(
        &corpus,
        search(&reader, &corpus, pattern)?,
        pattern,
        files_only,
    )
}

fn print_hits(
    corpus: &dyn Corpus,
    hits: std::collections::BTreeSet<holys3_core::DocId>,
    pattern: &str,
    files_only: bool,
) -> Result<()> {
    if files_only {
        for id in hits {
            println!("{}", corpus.docs()[id as usize].1);
        }
        return Ok(());
    }
    let re = regex::bytes::Regex::new(pattern)?;
    for id in hits {
        let bytes = corpus.fetch(id)?;
        let key = &corpus.docs()[id as usize].1;
        for matched in matches_in(id, &bytes, &re) {
            println!("{key}:{}:{}:{}", matched.line, matched.col, matched.text);
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    match Cli::parse().cmd {
        Cmd::Index {
            local_dir: Some(dir),
            out,
            strategy,
            ..
        } => build_local(&dir, &out, strategy.into()),
        Cmd::Index {
            bucket: Some(bucket),
            prefix,
            region,
            strategy,
            concurrency,
            ..
        } => build_s3(bucket, prefix, region, strategy.into(), concurrency).await,
        Cmd::Index { .. } => anyhow::bail!("provide --local-dir or --bucket"),
        Cmd::Search {
            pattern,
            local_dir: Some(dir),
            index,
            files_only,
            stats,
            ..
        } => search_local(&pattern, &dir, &index, files_only, stats),
        Cmd::Search {
            pattern,
            bucket: Some(bucket),
            prefix,
            region,
            files_only,
            stats,
            concurrency,
            ..
        } => search_s3(
            &pattern,
            bucket,
            prefix,
            region,
            files_only,
            stats,
            concurrency,
        ),
        Cmd::Search { .. } => anyhow::bail!("provide --local-dir or --bucket"),
        Cmd::Stats { index } => {
            let reader = MmapIndexReader::open(&index)?;
            let s = reader.stats();
            println!("distinct_grams={}", s.distinct_grams);
            println!("terms_fst_bytes={}", s.terms_fst_bytes);
            println!("postings_bytes={}", s.postings_bytes);
            Ok(())
        }
    }
}
