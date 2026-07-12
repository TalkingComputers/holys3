mod globs;
mod index;
mod json;
mod patterns;
mod printer;
mod scope;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use holys3_core::{BlobStore, LocalBlobStore, MatchOptions, Strategy};
use holys3_index::{
    search_streaming, update_index, IndexChanged, IndexReader, KeyScope, LocalCorpus, LocalFetcher,
    MatchSink, SearchStats, SegmentedReader, SourceIdentity,
};
use holys3_s3::{
    build_fetch_config, build_index_namespace, is_index_key, list_prefix, ObjectCacheConfig,
    ObjectMeta, S3BlobStore, S3Client, S3Corpus, S3Fetcher,
};
use scope::Scope;
use std::io::IsTerminal;
use std::path::{Path, PathBuf};
use std::time::Duration;

#[derive(Parser)]
#[command(
    name = "holys3",
    version,
    args_conflicts_with_subcommands = true,
    subcommand_negates_reqs = true,
    about = "Indexed regex search over S3 buckets and local files",
    long_about = "holys3 PATTERN TARGET searches a prebuilt index.\n\
        TARGET is s3://bucket[/prefix] or a local path.\n\
        To search for a pattern named like a subcommand (`index`, `stats`),\n\
        use -e: `holys3 -e index s3://bucket`."
)]
struct Cli {
    #[command(subcommand)]
    cmd: Option<Cmd>,
    #[command(flatten)]
    search: SearchArgs,
}

// Doc comments on clap structs are --help text; markdown would leak into it.
#[allow(clippy::doc_markdown)]
#[derive(Subcommand)]
enum Cmd {
    /// Build or update the index for TARGET (s3://bucket[/prefix] or a local directory).
    Index {
        #[arg(value_name = "TARGET")]
        target: String,
        #[command(flatten)]
        index: IndexArgs,
        #[arg(long, value_name = "PATH", conflicts_with = "location")]
        out: Option<PathBuf>,
        #[arg(long, value_enum, default_value = "trigram")]
        strategy: StrategyArg,
        /// Ignore any existing index and re-ingest everything.
        #[arg(long)]
        rebuild: bool,
        #[arg(long, requires = "interval", help = "Continuously update the index")]
        watch: bool,
        #[arg(
            long,
            value_name = "SECONDS",
            requires = "watch",
            value_parser = parse_positive_u64,
            help = "Wait SECONDS after each index attempt"
        )]
        interval: Option<u64>,
        #[arg(long, help = "Emit one JSON status object per line")]
        json: bool,
        #[command(flatten)]
        connect: ConnectArgs,
    },
    /// Report distinct grams + term-dict bytes for a local index.
    Stats {
        #[arg(long, default_value = "holys3.idxdir")]
        index: PathBuf,
    },
}

// Doc comments on clap structs are --help text; markdown would leak into it.
#[allow(clippy::doc_markdown)]
#[derive(clap::Args)]
struct ConnectArgs {
    /// AWS region (s3:// targets only). Uses the AWS SDK chain when omitted.
    #[arg(long)]
    region: Option<String>,
    /// Custom S3-compatible endpoint (e.g. http://127.0.0.1:9000 for MinIO).
    #[arg(long)]
    endpoint: Option<String>,
    /// Peak S3 fetch concurrency.
    #[arg(long, default_value_t = 750, value_parser = parse_concurrency)]
    concurrency: usize,
}

#[derive(clap::Args)]
struct IndexArgs {
    /// Index location (local path or `s3://bucket/prefix`).
    #[arg(long = "index", value_name = "LOCATION")]
    location: Option<String>,
    /// AWS region for an s3:// index location.
    #[arg(long = "index-region", requires = "location")]
    index_region: Option<String>,
    /// Custom endpoint for an s3:// index location.
    #[arg(long = "index-endpoint", requires = "location")]
    index_endpoint: Option<String>,
}

#[derive(Clone, Copy, PartialEq, Eq, ValueEnum)]
enum ColorArg {
    Never,
    Auto,
    Always,
    Ansi,
}

// Doc comments on clap structs are --help text; markdown would leak into it.
#[allow(clippy::doc_markdown)]
#[derive(clap::Args)]
struct SearchArgs {
    /// PATTERN then TARGET. With -e, the single positional is the TARGET.
    #[arg(value_name = "PATTERN|TARGET", required = true)]
    args: Vec<String>,
    /// A pattern to search for (repeatable; a line matching any pattern is printed).
    #[arg(short = 'e', long = "regexp", value_name = "PATTERN")]
    regexp: Vec<String>,
    /// Treat all patterns as literal strings.
    #[arg(short = 'F', long)]
    fixed_strings: bool,
    /// Case-insensitive search.
    #[arg(short = 'i', long, overrides_with_all = ["smart_case", "case_sensitive"])]
    ignore_case: bool,
    /// Case-insensitive when all patterns are lowercase.
    #[arg(short = 'S', long, overrides_with_all = ["ignore_case", "case_sensitive"])]
    smart_case: bool,
    /// Case-sensitive search (default).
    #[arg(short = 's', long, overrides_with_all = ["ignore_case", "smart_case"])]
    case_sensitive: bool,
    /// Wrap every pattern in word boundaries.
    #[arg(short = 'w', long)]
    word_regexp: bool,
    /// Print only the keys of matching objects.
    #[arg(short = 'l', long, conflicts_with_all = ["count", "count_matches", "json"])]
    files_with_matches: bool,
    /// Print the count of matching lines per object.
    #[arg(
        short = 'c',
        long,
        overrides_with = "count_matches",
        conflicts_with = "json"
    )]
    count: bool,
    /// Print the count of individual matches per object.
    #[arg(long, overrides_with = "count", conflicts_with = "json")]
    count_matches: bool,
    /// Limit the number of matching lines per object.
    #[arg(short = 'm', long, value_name = "NUM")]
    max_count: Option<u64>,
    /// Print NUM lines after each match.
    #[arg(short = 'A', long, value_name = "NUM")]
    after_context: Option<usize>,
    /// Print NUM lines before each match.
    #[arg(short = 'B', long, value_name = "NUM")]
    before_context: Option<usize>,
    /// Print NUM lines before and after each match.
    #[arg(short = 'C', long, value_name = "NUM")]
    context: Option<usize>,
    /// Show line numbers (default: on when printing to a terminal).
    #[arg(short = 'n', long, overrides_with = "no_line_number")]
    line_number: bool,
    /// Suppress line numbers.
    #[arg(short = 'N', long, overrides_with = "line_number")]
    no_line_number: bool,
    /// Show the 1-based byte column of the first match per line. Implies --line-number.
    #[arg(long)]
    column: bool,
    /// Group matches under their object key (default: on when printing to a terminal).
    #[arg(long, overrides_with = "no_heading")]
    heading: bool,
    /// One line per match: key:line:text.
    #[arg(long, overrides_with = "heading")]
    no_heading: bool,
    /// Include or exclude keys (gitignore-style glob; prefix with ! to exclude; repeatable).
    #[arg(short = 'g', long = "glob", value_name = "GLOB")]
    glob: Vec<String>,
    /// Print nothing; exit 0 at the first match.
    #[arg(short = 'q', long)]
    quiet: bool,
    /// When to use colors.
    #[arg(long, value_enum, default_value_t = ColorArg::Auto, value_name = "WHEN")]
    color: ColorArg,
    /// Emit results as JSON Lines (ripgrep-compatible wire format).
    #[arg(long)]
    json: bool,
    /// Print search statistics to stderr (with --json: the summary message).
    #[arg(long)]
    stats: bool,
    #[command(flatten)]
    index: IndexArgs,
    /// Only search objects whose key starts with this prefix.
    #[arg(long)]
    key_prefix: Option<String>,
    /// Only search objects whose key matches this regex.
    #[arg(long)]
    key_regex: Option<String>,
    /// Only search objects covering times at or after this instant
    /// (`2026-06-09`, `2026-06-09T14:30[:00][Z]`, or relative 30m/6h/2d/1w).
    #[arg(long)]
    since: Option<String>,
    /// Only search objects covering times at or before this instant (same formats).
    #[arg(long)]
    until: Option<String>,
    #[arg(
        long,
        requires = "object_cache_cap",
        value_name = "DIR",
        help = "Cache immutable S3 source bodies under DIR"
    )]
    object_cache: Option<PathBuf>,
    #[arg(
        long,
        requires = "object_cache",
        value_name = "BYTES",
        value_parser = parse_positive_u64,
        help = "Limit the source-object cache to BYTES"
    )]
    object_cache_cap: Option<u64>,
    #[command(flatten)]
    connect: ConnectArgs,
}

enum Target {
    Local(PathBuf),
    S3 { bucket: String, prefix: String },
}

/// Single choke point turning a TARGET string into local-vs-S3.
fn parse_target(raw: &str) -> Result<Target> {
    match raw.strip_prefix("s3://") {
        Some(rest) => {
            let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
            anyhow::ensure!(!bucket.is_empty(), "s3:// target needs a bucket");
            Ok(Target::S3 {
                bucket: bucket.to_owned(),
                prefix: prefix.to_owned(),
            })
        }
        None => Ok(Target::Local(PathBuf::from(raw))),
    }
}

enum Source {
    Local(PathBuf),
    S3(S3Source),
}

struct S3Source {
    client: S3Client,
    endpoint: String,
    bucket: String,
    prefix: String,
}

enum IndexStorage {
    Local {
        root: PathBuf,
        cache: PathBuf,
    },
    S3 {
        client: S3Client,
        endpoint: String,
        bucket: String,
        root: String,
        cache: PathBuf,
    },
}

impl IndexStorage {
    fn store(&self) -> Box<dyn BlobStore> {
        match self {
            Self::Local { root, .. } => Box::new(LocalBlobStore::new(root)),
            Self::S3 {
                client,
                bucket,
                root,
                ..
            } => Box::new(S3BlobStore::at(
                client.clone(),
                bucket.clone(),
                root.clone(),
            )),
        }
    }

    fn cache(&self) -> &Path {
        match self {
            Self::Local { cache, .. } | Self::S3 { cache, .. } => cache,
        }
    }

    fn location(&self) -> String {
        match self {
            Self::Local { root, .. } => root.display().to_string(),
            Self::S3 { bucket, root, .. } if root.is_empty() => format!("s3://{bucket}"),
            Self::S3 { bucket, root, .. } => format!("s3://{bucket}/{root}"),
        }
    }

    fn contains_source_key(&self, source: &S3Source, key: &str) -> bool {
        let Self::S3 {
            endpoint,
            bucket,
            root,
            ..
        } = self
        else {
            return false;
        };
        is_same_s3_bucket(endpoint, bucket, &source.endpoint, &source.bucket)
            && (key == root
                || key
                    .strip_prefix(root)
                    .is_some_and(|relative| relative.starts_with('/')))
    }
}

fn open_source(target: Target, connect: &ConnectArgs) -> Result<Source> {
    match target {
        Target::Local(dir) => {
            anyhow::ensure!(
                connect.region.is_none() && connect.endpoint.is_none(),
                "--region/--endpoint only apply to s3:// targets"
            );
            anyhow::ensure!(
                dir.is_dir(),
                "local target {} is not a directory",
                dir.display()
            );
            Ok(Source::Local(dir))
        }
        Target::S3 { bucket, prefix } => {
            let client = S3Client::connect(
                connect.region.clone(),
                connect.endpoint.clone(),
                build_fetch_config(connect.concurrency),
            )?;
            let endpoint = client.endpoint_identity();
            Ok(Source::S3(S3Source {
                client,
                endpoint,
                bucket,
                prefix,
            }))
        }
    }
}

fn validate_index_namespace(
    source_endpoint: &str,
    source_bucket: &str,
    source_prefix: &str,
    index_endpoint: &str,
    index_bucket: &str,
    index_root: &str,
) -> Result<()> {
    anyhow::ensure!(
        !index_root.is_empty(),
        "s3:// index location needs a prefix"
    );
    let covers_source =
        is_same_s3_bucket(source_endpoint, source_bucket, index_endpoint, index_bucket)
            && (source_prefix == index_root || source_prefix.starts_with(&list_prefix(index_root)));
    anyhow::ensure!(
        !covers_source,
        "index namespace s3://{index_bucket}/{index_root} contains source s3://{source_bucket}/{source_prefix}"
    );
    Ok(())
}

fn is_same_s3_bucket(
    first_endpoint: &str,
    first_bucket: &str,
    second_endpoint: &str,
    second_bucket: &str,
) -> bool {
    first_endpoint == second_endpoint && first_bucket == second_bucket
}

fn open_index_storage(
    source: &Source,
    index: &IndexArgs,
    local_out: Option<&Path>,
    concurrency: usize,
    create: bool,
) -> Result<IndexStorage> {
    let target = match (index.location.as_deref(), local_out) {
        (Some(location), None) => parse_target(location)?,
        (None, Some(path)) => Target::Local(path.to_path_buf()),
        (None, None) => match source {
            Source::Local(_) => Target::Local(PathBuf::from("holys3.idxdir")),
            Source::S3(source) => Target::S3 {
                bucket: source.bucket.clone(),
                prefix: build_index_namespace(&source.prefix),
            },
        },
        (Some(_), Some(_)) => anyhow::bail!("--index conflicts with --out"),
    };
    match target {
        Target::Local(root) => {
            anyhow::ensure!(
                index.index_region.is_none() && index.index_endpoint.is_none(),
                "--index-region/--index-endpoint require an s3:// index location"
            );
            if create {
                std::fs::create_dir_all(&root)?;
            }
            let cache = local_cache_dir(&root)?;
            Ok(IndexStorage::Local { root, cache })
        }
        Target::S3 { bucket, prefix } => {
            let root = prefix.trim_matches('/').to_owned();
            let client = match source {
                Source::S3(source)
                    if index.index_region.is_none() && index.index_endpoint.is_none() =>
                {
                    source.client.clone()
                }
                _ => S3Client::connect(
                    index.index_region.clone(),
                    index.index_endpoint.clone(),
                    build_fetch_config(concurrency),
                )?,
            };
            let endpoint = client.endpoint_identity();
            match source {
                Source::S3(source) => validate_index_namespace(
                    &source.endpoint,
                    &source.bucket,
                    &source.prefix,
                    &endpoint,
                    &bucket,
                    &root,
                )?,
                Source::Local(_) => {
                    anyhow::ensure!(!root.is_empty(), "s3:// index location needs a prefix");
                }
            }
            let cache = build_cache_dir(Some(&endpoint), &bucket, &root)?;
            Ok(IndexStorage::S3 {
                client,
                endpoint,
                bucket,
                root,
                cache,
            })
        }
    }
}

/// rg's rule: once any -e is given, every positional is a TARGET.
fn split_pattern_target(args: Vec<String>, regexp: Vec<String>) -> Result<(Vec<String>, String)> {
    if !regexp.is_empty() {
        let [target] = <[String; 1]>::try_from(args)
            .map_err(|_| anyhow::anyhow!("with -e/--regexp, provide exactly one TARGET"))?;
        return Ok((regexp, target));
    }
    let [pattern, target] = <[String; 2]>::try_from(args)
        .map_err(|_| anyhow::anyhow!("usage: holys3 PATTERN TARGET"))?;
    Ok((vec![pattern], target))
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

fn parse_positive_u64(value: &str) -> std::result::Result<u64, String> {
    let value = value.parse::<u64>().map_err(|error| error.to_string())?;
    if value == 0 {
        return Err("value must be greater than 0".to_owned());
    }
    Ok(value)
}

fn build_local_key_prefix(dir: &Path) -> Result<String> {
    let canonical = std::fs::canonicalize(dir)?;
    let mut prefix = canonical
        .to_str()
        .with_context(|| format!("local target is not valid UTF-8: {}", canonical.display()))?
        .to_owned();
    #[cfg(windows)]
    {
        prefix = prefix.replace('\\', "/");
    }
    if !prefix.ends_with('/') {
        prefix.push('/');
    }
    Ok(prefix)
}

fn build_source_identity(source: &Source) -> Result<SourceIdentity> {
    match source {
        Source::Local(dir) => Ok(SourceIdentity::Local {
            prefix: build_local_key_prefix(dir)?,
        }),
        Source::S3(source) => Ok(SourceIdentity::S3 {
            endpoint: source.endpoint.clone(),
            bucket: source.bucket.clone(),
            prefix: list_prefix(&source.prefix),
        }),
    }
}

fn build_local(
    dir: &Path,
    index: &IndexStorage,
    strategy: Strategy,
    rebuild: bool,
) -> Result<index::IndexResult> {
    // Canonical target root: `./logs` and `logs` must produce identical
    // index keys, or invocation spelling would churn the incremental diff.
    let dir = std::fs::canonicalize(dir)?;
    let excluded = match index {
        IndexStorage::Local { root, .. } => {
            let root = std::fs::canonicalize(root)?;
            anyhow::ensure!(
                !dir.starts_with(&root),
                "local index directory must not contain the target directory"
            );
            Some(root)
        }
        IndexStorage::S3 { .. } => None,
    };
    let corpus = LocalCorpus::new_excluding(&dir, excluded.as_deref())?;
    let listing = corpus.listing()?;
    let store = index.store();
    let source = SourceIdentity::Local {
        prefix: build_local_key_prefix(&dir)?,
    };
    let report = update_index(
        store.as_ref(),
        index.cache(),
        &source,
        strategy,
        &listing,
        rebuild,
        &|shard| Ok(Box::new(LocalCorpus::from_listing(shard))),
    )?;
    Ok(index::IndexResult {
        report,
        location: index.location(),
    })
}

fn list_user_objects(src: &S3Source, index: &IndexStorage) -> Result<Vec<ObjectMeta>> {
    Ok(src
        .client
        .list(&src.bucket, &list_prefix(&src.prefix))?
        .into_iter()
        .filter(|object| {
            !is_index_key(&src.prefix, &object.key) && !index.contains_source_key(src, &object.key)
        })
        .collect())
}

fn build_s3(
    src: &S3Source,
    index: &IndexStorage,
    strategy: Strategy,
    rebuild: bool,
) -> Result<index::IndexResult> {
    // Real sizes ride the listing so the build bounds its fetch chunks by
    // bytes, not just doc count — a bucket of huge objects must not OOM.
    let listing = list_user_objects(src, index)?
        .into_iter()
        .map(|object| (object.key, object.etag, object.size))
        .collect::<Vec<_>>();
    let store = index.store();
    let source = SourceIdentity::S3 {
        endpoint: src.endpoint.clone(),
        bucket: src.bucket.clone(),
        prefix: list_prefix(&src.prefix),
    };
    let client = src.client.clone();
    let bucket = src.bucket.clone();
    let report = update_index(
        store.as_ref(),
        index.cache(),
        &source,
        strategy,
        &listing,
        rebuild,
        &|shard| {
            Ok(Box::new(S3Corpus::new(
                client.clone(),
                bucket.clone(),
                shard,
            )))
        },
    )?;
    Ok(index::IndexResult {
        report,
        location: index.location(),
    })
}

/// Run one search against the opened source. Scope filtering, the optional
/// stats line, and the undated-keys note are shared across all output modes.
#[derive(Clone, Copy)]
struct SearchExecution<'a> {
    pattern: &'a str,
    scope: Option<&'a Scope>,
    options: MatchOptions,
    stats_line: bool,
    object_cache: Option<&'a ObjectCacheConfig>,
}

fn execute_search(
    source: Source,
    index: &IndexStorage,
    execution: SearchExecution<'_>,
    sink: &dyn MatchSink,
) -> Result<SearchStats> {
    let source_identity = build_source_identity(&source)?;
    let key_filter = execution.scope.map(|scope| {
        move |key: &str| {
            scope
                .key_prefix()
                .is_none_or(|prefix| key.starts_with(prefix))
                && scope.matches(key)
        }
    });
    let key_matches = key_filter
        .as_ref()
        .map(|filter| filter as &(dyn Fn(&str) -> bool + Sync));
    let search_stats = match source {
        Source::Local(dir) => {
            anyhow::ensure!(
                execution.object_cache.is_none(),
                "--object-cache only applies to s3:// targets"
            );
            let target_prefix = build_local_key_prefix(&dir)?;
            let fetcher = LocalFetcher::new(std::thread::available_parallelism()?.get())?;
            search_with_reopen(
                || SegmentedReader::open(index.store(), index.cache(), &source_identity),
                &fetcher,
                execution.pattern,
                KeyScope {
                    prefix: Some(&target_prefix),
                    matches: key_matches,
                },
                execution.options,
                sink,
            )?
        }
        Source::S3(src) => {
            let target_prefix = list_prefix(&src.prefix);
            let candidate_prefix = if target_prefix.is_empty() {
                execution.scope.and_then(Scope::key_prefix)
            } else {
                Some(target_prefix.as_str())
            };
            let fetcher = match execution.object_cache {
                Some(config) => S3Fetcher::with_cache(src.client, src.bucket, config.clone())?,
                None => S3Fetcher::new(src.client, src.bucket),
            };
            search_with_reopen(
                || SegmentedReader::open(index.store(), index.cache(), &source_identity),
                &fetcher,
                execution.pattern,
                KeyScope {
                    prefix: candidate_prefix,
                    matches: key_matches,
                },
                execution.options,
                sink,
            )?
        }
    };
    if let Some(scope) = execution.scope {
        scope.report();
    }
    if execution.stats_line {
        eprintln!(
            "candidates={} total={} hits={}",
            search_stats.candidates,
            search_stats.total_docs,
            search_stats.hits.len(),
        );
    }
    Ok(search_stats)
}

fn search_with_reopen(
    mut open: impl FnMut() -> Result<SegmentedReader>,
    fetcher: &dyn holys3_core::DocFetcher,
    pattern: &str,
    scope: KeyScope<'_>,
    options: MatchOptions,
    sink: &dyn MatchSink,
) -> Result<SearchStats> {
    let reader = open()?;
    match search_streaming(&reader, fetcher, pattern, scope, options, sink) {
        Err(error) if error.is::<IndexChanged>() => {
            let reader = open()?;
            search_streaming(&reader, fetcher, pattern, scope, options, sink)
        }
        result => result,
    }
}

/// Returns whether anything matched (drives the exit code).
fn run_search(args: SearchArgs) -> Result<bool> {
    let (patterns, target_raw) = split_pattern_target(args.args, args.regexp)?;
    let pattern = patterns::build_pattern(
        &patterns,
        args.fixed_strings,
        args.word_regexp,
        args.ignore_case,
        args.smart_case,
    )
    .with_context(|| format!("invalid pattern {:?}", patterns.join("|")))?;
    let globs = globs::build_glob_filter(&args.glob)?;
    let scope = Scope::from_args(
        args.key_prefix,
        args.key_regex,
        args.since,
        args.until,
        globs,
    )?;
    let object_cache = match (args.object_cache, args.object_cache_cap) {
        (Some(root), Some(cap_bytes)) => Some(ObjectCacheConfig { root, cap_bytes }),
        (None, None) => None,
        _ => anyhow::bail!("--object-cache and --object-cache-cap must be supplied together"),
    };

    let standard_mode =
        !args.quiet && !args.json && !args.files_with_matches && !args.count && !args.count_matches;
    // standard output AND --json render context (rg emits context messages
    // on the JSON wire too); count/quiet/-l modes do not
    let renders_context = standard_mode || args.json;
    let before = args.before_context.or(args.context).unwrap_or(0);
    let after = args.after_context.or(args.context).unwrap_or(0);
    let options = MatchOptions {
        before_context: if renders_context { before } else { 0 },
        after_context: if renders_context { after } else { 0 },
        max_count: args.max_count,
    };

    let is_tty = std::io::stdout().is_terminal();
    let heading = if args.heading {
        true
    } else if args.no_heading {
        false
    } else {
        is_tty
    };
    let line_numbers = if args.line_number {
        true
    } else if args.no_line_number {
        false
    } else {
        args.column || is_tty
    };
    let color = printer::resolve_color(args.color, is_tty);

    let source = open_source(parse_target(&target_raw)?, &args.connect)?;
    let index = open_index_storage(&source, &args.index, None, args.connect.concurrency, false)?;
    let stats_line = args.stats && !args.json;
    let execution = SearchExecution {
        pattern: &pattern,
        scope: scope.as_ref(),
        options,
        stats_line,
        object_cache: object_cache.as_ref(),
    };

    if args.quiet {
        let sink = printer::QuietSink::new(!args.stats);
        let result = execute_search(source, &index, execution, &sink);
        return match result {
            Ok(_) => Ok(sink.matched()),
            // rg's quiet error-mask: a found match wins over later errors
            Err(_) if sink.matched() => Ok(true),
            Err(err) => Err(err),
        };
    }
    if args.json {
        let started = std::time::Instant::now();
        let sink = json::JsonSink::new();
        let stats = execute_search(source, &index, execution, &sink)?;
        sink.write_summary(&stats, started.elapsed())?;
        return Ok(!stats.hits.is_empty());
    }
    let sink: Box<dyn MatchSink> = if args.files_with_matches {
        Box::new(printer::PathSink::new(color))
    } else if args.count || args.count_matches {
        Box::new(printer::CountSink::new(args.count_matches, color))
    } else {
        Box::new(printer::StandardSink::new(
            printer::RenderConfig {
                heading,
                line_numbers,
                column: args.column,
                context_active: options.before_context > 0 || options.after_context > 0,
            },
            color,
        ))
    };
    let stats = execute_search(source, &index, execution, sink.as_ref())?;
    Ok(!stats.hits.is_empty())
}

fn run() -> Result<bool> {
    let cli = Cli::parse();
    match cli.cmd {
        Some(Cmd::Index {
            target,
            index,
            out,
            strategy,
            rebuild,
            watch: _,
            interval,
            json,
            connect,
        }) => {
            let interval = interval.map(Duration::from_secs);
            let strategy = strategy.into();
            let config = index::IndexConfig {
                target: &target,
                interval,
                rebuild,
                json,
            };
            let started = std::time::Instant::now();
            let (source, storage) = match (|| -> Result<(Source, IndexStorage)> {
                let parsed = parse_target(&target)?;
                anyhow::ensure!(
                    out.is_none() || matches!(&parsed, Target::Local(_)),
                    "--out only applies to local targets; use --index for S3"
                );
                let source = open_source(parsed, &connect)?;
                let storage =
                    open_index_storage(&source, &index, out.as_deref(), connect.concurrency, true)?;
                Ok((source, storage))
            })() {
                Ok(opened) => opened,
                Err(error) => {
                    index::write_start_error(&target, json, started.elapsed(), &error)?;
                    return Err(error);
                }
            };
            match source {
                Source::Local(dir) => index::run_index(config, |cycle_rebuild| {
                    build_local(&dir, &storage, strategy, cycle_rebuild)
                })?,
                Source::S3(src) => {
                    index::run_index(config, |cycle_rebuild| {
                        build_s3(&src, &storage, strategy, cycle_rebuild)
                    })?;
                }
            }
            Ok(true)
        }
        Some(Cmd::Stats { index }) => {
            let reader = open_local_reader(&index)?;
            let s = reader.stats();
            println!("distinct_grams={}", s.distinct_grams);
            println!("terms_fst_bytes={}", s.terms_fst_bytes);
            println!("postings_bytes={}", s.postings_bytes);
            Ok(true)
        }
        None => run_search(cli.search),
    }
}

fn main() -> std::process::ExitCode {
    match run() {
        Ok(true) => std::process::ExitCode::SUCCESS,
        Ok(false) => std::process::ExitCode::from(1),
        Err(err) => {
            eprintln!("holys3: {err:#}");
            std::process::ExitCode::from(2)
        }
    }
}

/// Cache dir per (endpoint, bucket, prefix): readable bucket name plus a
/// short hash so `a/b` vs `a__b` prefixes (or the same bucket name on two
/// endpoints) can never share state.
fn build_cache_dir(endpoint: Option<&str>, bucket: &str, prefix: &str) -> Result<PathBuf> {
    let mut path = cache_home()?;
    path.push("holys3");
    let scope = format!("{}\0{bucket}\0{prefix}", endpoint.unwrap_or(""));
    path.push(format!(
        "{bucket}-{:016x}",
        holys3_core::hash_ngram(scope.as_bytes())
    ));
    Ok(path)
}

/// Cache dir for a local index, keyed on its canonicalized path. A REAL
/// cache directory, never `<out>/segments` itself: `SegmentedReader`'s
/// self-heal and stale-segment eviction delete cache entries, which must
/// never be the store's own blobs.
fn local_cache_dir(index_dir: &Path) -> Result<PathBuf> {
    let canonical = std::fs::canonicalize(index_dir).with_context(|| {
        format!(
            "no index at {} (run `holys3 index <TARGET> --index {0}`)",
            index_dir.display()
        )
    })?;
    let mut path = cache_home()?;
    path.push("holys3");
    path.push(format!(
        "local-{:016x}",
        holys3_core::hash_ngram(canonical.to_string_lossy().as_bytes())
    ));
    Ok(path)
}

fn open_local_reader(index_dir: &Path) -> Result<SegmentedReader> {
    let cache_dir = local_cache_dir(index_dir)?;
    SegmentedReader::inspect(Box::new(LocalBlobStore::new(index_dir)), &cache_dir)
}

fn cache_home() -> Result<PathBuf> {
    read_cache_home(
        std::env::var("XDG_CACHE_HOME"),
        // HOME on unix; USERPROFILE is the Windows equivalent
        std::env::var("HOME").or_else(|_| std::env::var("USERPROFILE")),
    )
}

fn read_cache_home(
    xdg_cache_home: std::result::Result<String, std::env::VarError>,
    home: std::result::Result<String, std::env::VarError>,
) -> Result<PathBuf> {
    match xdg_cache_home {
        Ok(path) => Ok(PathBuf::from(path)),
        Err(std::env::VarError::NotPresent) => Ok(PathBuf::from(home.map_err(|_| {
            anyhow::anyhow!("neither XDG_CACHE_HOME, HOME, nor USERPROFILE is set")
        })?)
        .join(".cache")),
        Err(err) => Err(err.into()),
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

    #[test]
    fn parse_target_forms() {
        assert!(matches!(
            parse_target("s3://bkt").unwrap(),
            Target::S3 { bucket, prefix } if bucket == "bkt" && prefix.is_empty()
        ));
        assert!(matches!(
            parse_target("s3://bkt/a/b").unwrap(),
            Target::S3 { bucket, prefix } if bucket == "bkt" && prefix == "a/b"
        ));
        assert!(matches!(
            parse_target("s3://bkt/a//b/").unwrap(),
            Target::S3 { bucket, prefix } if bucket == "bkt" && prefix == "a//b/"
        ));
        assert!(parse_target("s3://").is_err());
        assert!(matches!(
            parse_target("./logs").unwrap(),
            Target::Local(p) if p == Path::new("./logs")
        ));
    }

    #[test]
    fn split_pattern_target_rules() {
        let (pats, target) =
            split_pattern_target(vec!["ERROR".into(), "s3://b".into()], vec![]).unwrap();
        assert_eq!(pats, vec!["ERROR"]);
        assert_eq!(target, "s3://b");
        let (pats, target) =
            split_pattern_target(vec!["s3://b".into()], vec!["a".into(), "b".into()]).unwrap();
        assert_eq!(pats, vec!["a", "b"]);
        assert_eq!(target, "s3://b");
        assert!(split_pattern_target(vec!["onlypattern".into()], vec![]).is_err());
        assert!(split_pattern_target(vec!["t1".into(), "t2".into()], vec!["p".into()]).is_err());
    }

    #[test]
    fn clap_parses_rg_style_invocations() {
        // subcommand wins the first positional
        let cli = Cli::try_parse_from(["holys3", "index", "s3://b"]).unwrap();
        assert!(matches!(cli.cmd, Some(Cmd::Index { .. })));
        // -e escape hatch searches for the literal word "index"
        let cli = Cli::try_parse_from(["holys3", "-e", "index", "s3://b"]).unwrap();
        assert!(cli.cmd.is_none());
        assert_eq!(cli.search.regexp, vec!["index"]);
        // last case flag wins
        let cli = Cli::try_parse_from(["holys3", "-i", "-s", "p", "t"]).unwrap();
        assert!(cli.search.case_sensitive && !cli.search.ignore_case);
        // --json conflicts with -c
        assert!(Cli::try_parse_from(["holys3", "--json", "-c", "p", "t"]).is_err());
    }

    #[test]
    fn clap_parses_independent_index_locations() {
        let cli = Cli::try_parse_from([
            "holys3",
            "index",
            "s3://source/logs",
            "--index",
            "s3://search-index/holys3/logs",
            "--index-region",
            "us-west-2",
            "--index-endpoint",
            "http://127.0.0.1:9000",
        ])
        .unwrap();
        let Some(Cmd::Index { index, .. }) = cli.cmd else {
            panic!("expected index command");
        };
        assert_eq!(
            index.location.as_deref(),
            Some("s3://search-index/holys3/logs")
        );
        assert_eq!(index.index_region.as_deref(), Some("us-west-2"));
        assert_eq!(
            index.index_endpoint.as_deref(),
            Some("http://127.0.0.1:9000")
        );

        let cli = Cli::try_parse_from([
            "holys3",
            "needle",
            "s3://source/logs",
            "--index",
            "/tmp/holys3-index",
        ])
        .unwrap();
        assert_eq!(
            cli.search.index.location.as_deref(),
            Some("/tmp/holys3-index")
        );
        assert!(Cli::try_parse_from([
            "holys3",
            "needle",
            "s3://source/logs",
            "--index-region",
            "us-west-2"
        ])
        .is_err());
    }

    #[test]
    fn s3_index_location_rejects_bucket_root() {
        let error = validate_index_namespace(
            "https://source",
            "source",
            "logs",
            "https://index",
            "index",
            "",
        )
        .unwrap_err();
        assert_eq!(error.to_string(), "s3:// index location needs a prefix");
    }

    #[test]
    fn s3_index_location_rejects_a_source_covered_by_its_namespace() {
        let error = validate_index_namespace(
            "https://s3.us-east-1.amazonaws.com",
            "bucket",
            "logs/app",
            "https://s3.us-east-1.amazonaws.com",
            "bucket",
            "logs",
        )
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            "index namespace s3://bucket/logs contains source s3://bucket/logs/app"
        );
        validate_index_namespace(
            "https://s3",
            "bucket",
            "logs",
            "https://s3",
            "bucket",
            "logs/index",
        )
        .unwrap();
        validate_index_namespace(
            "https://s3",
            "source",
            "logs",
            "https://s3",
            "index",
            "logs",
        )
        .unwrap();
        validate_index_namespace(
            "https://aws",
            "bucket",
            "logs/app",
            "http://minio",
            "bucket",
            "logs",
        )
        .unwrap();
    }
}
