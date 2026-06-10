mod scope;

use anyhow::Result;
use clap::{ArgGroup, Parser, Subcommand, ValueEnum};
use holys3_core::{Corpus, DocFetcher, Match, Strategy};
use holys3_index::{
    build_to_dir, search_streaming, update_index, IndexReader, KeyScope, LocalCorpus, LocalFetcher,
    MatchSink, MmapIndexReader, SegmentedReader, SinkFlow,
};
use holys3_s3::{
    build_fetch_config, build_index_namespace, list_prefix, normalize_prefix, region_from_env,
    s3_client_from_env, ObjectMeta, S3BlobStore, S3Client, S3Corpus, S3Fetcher,
};
use scope::Scope;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

#[derive(Parser)]
#[command(name = "holys3")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

// Doc comments here are clap help text; markdown formatting would leak into --help.
#[allow(clippy::doc_markdown)]
#[derive(clap::Args)]
#[command(group(ArgGroup::new("source").required(true).args(["local_dir", "bucket"])))]
struct SourceArgs {
    /// Local directory to index or search.
    #[arg(long)]
    local_dir: Option<PathBuf>,
    /// S3 bucket to index or search.
    #[arg(long)]
    bucket: Option<String>,
    /// S3 key prefix (directory semantics: "logs" only matches "logs/...").
    #[arg(long, default_value = "", conflicts_with = "local_dir")]
    prefix: String,
    /// AWS region. If omitted, AWS_REGION is required.
    #[arg(long, conflicts_with = "local_dir")]
    region: Option<String>,
    /// Custom S3-compatible endpoint (e.g. http://127.0.0.1:9000 for MinIO).
    #[arg(long, conflicts_with = "local_dir")]
    endpoint: Option<String>,
    /// Peak S3 fetch concurrency.
    #[arg(long, default_value_t = 750, value_parser = parse_concurrency, conflicts_with = "local_dir")]
    concurrency: usize,
}

enum Source {
    Local(PathBuf),
    S3(S3Source),
}

struct S3Source {
    client: S3Client,
    bucket: String,
    prefix: String,
}

impl SourceArgs {
    fn open(self) -> Result<Source> {
        match (self.local_dir, self.bucket) {
            (Some(dir), None) => Ok(Source::Local(dir)),
            (None, Some(bucket)) => {
                let region = match self.region {
                    Some(region) => region,
                    None => region_from_env()?,
                };
                let client = s3_client_from_env(
                    &region,
                    self.endpoint,
                    build_fetch_config(self.concurrency),
                )?;
                Ok(Source::S3(S3Source {
                    client,
                    bucket,
                    prefix: normalize_prefix(&self.prefix),
                }))
            }
            _ => anyhow::bail!("provide --local-dir or --bucket"),
        }
    }
}

#[derive(Subcommand)]
enum Cmd {
    /// Build the index for a local dir or an S3 prefix.
    Index {
        #[command(flatten)]
        source: SourceArgs,
        /// Local index directory (local source only).
        #[arg(long, default_value = "holys3.idxdir", conflicts_with = "bucket")]
        out: PathBuf,
        #[arg(long, value_enum, default_value = "trigram")]
        strategy: StrategyArg,
    },
    /// Search a pattern using a prebuilt index.
    Search {
        pattern: String,
        #[command(flatten)]
        source: SourceArgs,
        /// Local index directory (local source only).
        #[arg(long, default_value = "holys3.idxdir", conflicts_with = "bucket")]
        index: PathBuf,
        /// Only search objects whose key starts with this prefix.
        #[arg(long)]
        key_prefix: Option<String>,
        /// Only search objects whose key matches this regex.
        #[arg(long)]
        key_regex: Option<String>,
        /// Only search objects covering times at or after this instant:
        /// 2026-06-09, 2026-06-09T14:30[:00][Z], or relative like 6h / 2d (ago, UTC).
        #[arg(long)]
        since: Option<String>,
        /// Only search objects covering times at or before this instant (same formats).
        #[arg(long)]
        until: Option<String>,
        #[arg(long)]
        files_only: bool,
        #[arg(long)]
        stats: bool,
    },
    /// Report distinct grams + term-dict bytes for a local index.
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

fn parse_concurrency(value: &str) -> std::result::Result<usize, String> {
    let concurrency = value.parse::<usize>().map_err(|err| err.to_string())?;
    if concurrency == 0 {
        return Err("concurrency must be greater than 0".to_owned());
    }
    Ok(concurrency)
}

fn build_local(dir: &Path, out: &Path, strategy: Strategy) -> Result<()> {
    let corpus = LocalCorpus::new(dir)?;
    build_to_dir(&corpus, out, strategy)?;
    eprintln!("indexed {} docs -> {}", corpus.docs().len(), out.display());
    Ok(())
}

fn list_user_objects(src: &S3Source) -> Result<Vec<ObjectMeta>> {
    let namespace = format!("{}/", build_index_namespace(&src.prefix));
    Ok(src
        .client
        .list(&src.bucket, &list_prefix(&src.prefix))?
        .into_iter()
        .filter(|object| !object.key.starts_with(&namespace))
        .collect())
}

fn build_s3(src: S3Source, strategy: Strategy) -> Result<()> {
    let listing = list_user_objects(&src)?
        .into_iter()
        .map(|object| (object.key, object.etag))
        .collect::<Vec<_>>();
    let cache_dir = build_cache_dir(&src.bucket, &src.prefix)?;
    let store = S3BlobStore::new(src.client.clone(), src.bucket.clone(), src.prefix.clone());
    let client = src.client;
    let bucket = src.bucket.clone();
    let report = update_index(&store, &cache_dir, strategy, &listing, &|keys| {
        let objects = keys
            .iter()
            .map(|key| ObjectMeta {
                key: key.clone(),
                etag: String::new(),
                size: 0,
            })
            .collect::<Vec<_>>();
        Ok(Box::new(S3Corpus::new(
            client.clone(),
            bucket.clone(),
            &objects,
        )))
    })?;
    let namespace = build_index_namespace(&src.prefix);
    if report.up_to_date {
        eprintln!(
            "index up to date: {} docs in {} segments at s3://{}/{namespace}",
            report.total_docs, report.segments, src.bucket
        );
    } else {
        eprintln!(
            "indexed +{} -{} -> {} docs in {} segments{} at s3://{}/{namespace}",
            report.added,
            report.removed,
            report.total_docs,
            report.segments,
            if report.compacted { " (compacted)" } else { "" },
            src.bucket
        );
    }
    Ok(())
}

fn search_local(
    pattern: &str,
    index: &Path,
    files_only: bool,
    stats: bool,
    scope: Option<&Scope>,
) -> Result<()> {
    let reader = MmapIndexReader::open(index)?;
    emit_results(&reader, &LocalFetcher, pattern, files_only, stats, scope)
}

fn search_s3(
    src: S3Source,
    pattern: &str,
    files_only: bool,
    stats: bool,
    scope: Option<&Scope>,
) -> Result<()> {
    let cache_dir = build_cache_dir(&src.bucket, &src.prefix)?;
    let store = S3BlobStore::new(src.client.clone(), src.bucket.clone(), src.prefix.clone());
    let reader = SegmentedReader::open(Box::new(store), &cache_dir)?;
    let fetcher = S3Fetcher::new(src.client, src.bucket);
    emit_results(&reader, &fetcher, pattern, files_only, stats, scope)
}

/// Prints each doc's results as verification completes (unordered across
/// docs, like grep over many files). A closed downstream pipe stops the
/// search instead of erroring.
struct PrintSink {
    out: Mutex<std::io::BufWriter<std::io::Stdout>>,
    files_only: bool,
}

impl MatchSink for PrintSink {
    fn wants_matches(&self) -> bool {
        !self.files_only
    }

    fn on_doc(&self, key: &str, matches: &[Match]) -> Result<SinkFlow> {
        let mut out = self
            .out
            .lock()
            .map_err(|_| anyhow::anyhow!("output writer poisoned"))?;
        let written = if self.files_only {
            writeln!(out, "{key}")
        } else {
            matches.iter().try_for_each(|matched| {
                writeln!(
                    out,
                    "{key}:{}:{}:{}",
                    matched.line, matched.col, matched.text
                )
            })
        }
        .and_then(|()| out.flush());
        match written {
            Ok(()) => Ok(SinkFlow::Continue),
            Err(err) if err.kind() == std::io::ErrorKind::BrokenPipe => Ok(SinkFlow::Stop),
            Err(err) => Err(err.into()),
        }
    }
}

fn emit_results(
    reader: &dyn IndexReader,
    fetcher: &dyn DocFetcher,
    pattern: &str,
    files_only: bool,
    stats: bool,
    scope: Option<&Scope>,
) -> Result<()> {
    let sink = PrintSink {
        out: Mutex::new(std::io::BufWriter::new(std::io::stdout())),
        files_only,
    };
    let key_filter = scope.map(|scope| move |key: &str| scope.matches(key));
    let key_scope = KeyScope {
        prefix: scope.and_then(Scope::key_prefix),
        matches: key_filter
            .as_ref()
            .map(|filter| filter as &(dyn Fn(&str) -> bool + Sync)),
    };
    let search_stats = search_streaming(reader, fetcher, pattern, key_scope, &sink)?;
    if let Some(scope) = scope {
        scope.report();
    }
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
    Ok(())
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

fn main() -> Result<()> {
    match Cli::parse().cmd {
        Cmd::Index {
            source,
            out,
            strategy,
        } => match source.open()? {
            Source::Local(dir) => build_local(&dir, &out, strategy.into()),
            Source::S3(src) => build_s3(src, strategy.into()),
        },
        Cmd::Search {
            pattern,
            source,
            index,
            key_prefix,
            key_regex,
            since,
            until,
            files_only,
            stats,
        } => {
            let scope = Scope::from_args(key_prefix, key_regex, since, until)?;
            match source.open()? {
                Source::Local(_) => {
                    search_local(&pattern, &index, files_only, stats, scope.as_ref())
                }
                Source::S3(src) => search_s3(src, &pattern, files_only, stats, scope.as_ref()),
            }
        }
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
    fn cli_args_are_consistent() {
        use clap::CommandFactory;
        Cli::command().debug_assert();
    }

    #[test]
    fn read_cache_home_uses_xdg_cache_home() {
        let path = read_cache_home(Err(VarError::NotPresent), Ok("/home/me".to_owned())).unwrap();
        assert_eq!(path, PathBuf::from("/home/me/.cache"));

        let path = read_cache_home(Ok("/cache".to_owned()), Err(VarError::NotPresent)).unwrap();
        assert_eq!(path, PathBuf::from("/cache"));
    }
}
