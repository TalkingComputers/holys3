use anyhow::Result;
use clap::{Parser, Subcommand, ValueEnum};
use holys3_core::{Corpus, Match, Strategy};
use holys3_index::{
    build_to_dir, build_to_store, compute_build_id, search_matches, search_with_stats, IndexReader,
    LocalCorpus, MmapIndexReader, StoreIndexReader,
};
use holys3_s3::{
    build_fetch_config, build_index_namespace, is_index_key, normalize_prefix, region_from_env,
    s3_client_from_env, ObjectMeta, S3BlobStore, S3Corpus,
};
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
    let client = s3_client_from_env(&region, None)?;
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
    if files_only {
        let search_stats = search_with_stats(&reader, &corpus, pattern)?;
        if stats {
            eprintln!(
                "candidates={} total={} strategy={:?}",
                search_stats.candidates,
                search_stats.total_docs,
                reader.strategy()
            );
        }
        return print_hits(&corpus, search_stats.hits);
    }
    let (matches, search_stats) = search_matches(&reader, &corpus, pattern)?;
    if stats {
        eprintln!(
            "candidates={} total={} strategy={:?}",
            search_stats.candidates,
            search_stats.total_docs,
            reader.strategy()
        );
    }
    print_matches(&corpus, matches)
}

fn read_region(region: Option<String>) -> Result<String> {
    match region {
        Some(region) => Ok(region),
        None => region_from_env(),
    }
}

fn select_user_objects(objects: Vec<ObjectMeta>, prefix: &str) -> Vec<ObjectMeta> {
    objects
        .into_iter()
        .filter(|object| !is_index_key(prefix, &object.key))
        .collect()
}

fn build_cache_dir(bucket: &str, prefix: &str) -> Result<PathBuf> {
    let mut path = read_cache_home(std::env::var("XDG_CACHE_HOME"), std::env::var("HOME"))?;
    path.push("holys3");
    path.push(bucket);
    let prefix = prefix.replace('/', "__");
    if !prefix.is_empty() {
        path.push(prefix);
    }
    Ok(path)
}

fn read_cache_home(
    xdg_cache_home: std::result::Result<String, std::env::VarError>,
    home: std::result::Result<String, std::env::VarError>,
) -> Result<PathBuf> {
    match xdg_cache_home {
        Ok(path) => Ok(PathBuf::from(path)),
        Err(std::env::VarError::NotPresent) => Ok(PathBuf::from(home?).join(".cache")),
        Err(err) => Err(err.into()),
    }
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
    let client = s3_client_from_env(&region, None)?;
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
    let corpus = S3Corpus::from_docs(client, bucket, reader.docs().to_vec(), rt, cfg)?;
    if files_only {
        let search_stats = search_with_stats(&reader, &corpus, pattern)?;
        if stats {
            let index_stats = reader.stats();
            eprintln!(
                "candidates={} total={} strategy={:?} distinct_grams={} terms_fst_bytes={} postings_bytes={}",
                search_stats.candidates,
                search_stats.total_docs,
                reader.strategy(),
                index_stats.distinct_grams,
                index_stats.terms_fst_bytes,
                index_stats.postings_bytes
            );
        }
        return print_hits(&corpus, search_stats.hits);
    }
    let (matches, search_stats) = search_matches(&reader, &corpus, pattern)?;
    if stats {
        let index_stats = reader.stats();
        eprintln!(
            "candidates={} total={} strategy={:?} distinct_grams={} terms_fst_bytes={} postings_bytes={}",
            search_stats.candidates,
            search_stats.total_docs,
            reader.strategy(),
            index_stats.distinct_grams,
            index_stats.terms_fst_bytes,
            index_stats.postings_bytes
        );
    }
    print_matches(&corpus, matches)
}

fn print_hits(
    corpus: &dyn Corpus,
    hits: std::collections::BTreeSet<holys3_core::DocId>,
) -> Result<()> {
    for id in hits {
        println!("{}", corpus.docs()[id as usize].1);
    }
    Ok(())
}

fn print_matches(corpus: &dyn Corpus, matches: Vec<Match>) -> Result<()> {
    for matched in matches {
        let key = &corpus.docs()[matched.doc as usize].1;
        println!("{key}:{}:{}:{}", matched.line, matched.col, matched.text);
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::env::VarError;

    #[test]
    fn read_cache_home_uses_xdg_cache_home() {
        let path = read_cache_home(Err(VarError::NotPresent), Ok("/home/me".to_owned())).unwrap();
        assert_eq!(path, PathBuf::from("/home/me/.cache"));

        let path = read_cache_home(Ok("/cache".to_owned()), Err(VarError::NotPresent)).unwrap();
        assert_eq!(path, PathBuf::from("/cache"));
    }
}
