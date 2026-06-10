mod gen;
mod scenarios;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use gen::{
    doc_path, latest_run_path, local_index_dir, objects_dir, read_manifest, remove_dir,
    reports_dir, write_seed, SeedManifest,
};
use holys3_core::{Corpus, DocFetcher, Strategy};
use holys3_index::{
    build_to_dir, search_collect, update_index, IndexReader, LocalCorpus, LocalFetcher,
    MmapIndexReader, SegmentedReader,
};
use holys3_s3::{
    build_fetch_config, build_index_namespace, s3_client_from_env, S3BlobStore, S3Client, S3Corpus,
    S3Fetcher,
};
use scenarios::{read_scenarios, Scenario};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

const S3_PREFIX: &str = "xbench";
const DEFAULT_CONCURRENCY: usize = 64;

#[derive(Parser)]
#[command(name = "holys3-bench")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Seed {
        #[arg(long)]
        seed: u64,
        #[arg(long)]
        objects: usize,
        #[arg(long)]
        size: usize,
    },
    Upload {
        #[arg(long, value_enum)]
        target: UploadTarget,
    },
    Run {
        #[arg(long)]
        scenarios: PathBuf,
        #[arg(long)]
        iterations: usize,
        #[arg(long)]
        warmup: usize,
        #[arg(long, default_value_t = DEFAULT_CONCURRENCY)]
        concurrency: usize,
    },
    Report {
        #[arg(long)]
        out: PathBuf,
    },
    Compare {
        base: PathBuf,
        candidate: PathBuf,
    },
    Render {
        #[arg(long)]
        input: PathBuf,
    },
}

#[derive(Clone, Copy, ValueEnum)]
enum UploadTarget {
    Dir,
    S3,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BackendSummary {
    kind: String,
    bucket: Option<String>,
    region: Option<String>,
    endpoint: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ScenarioResult {
    name: String,
    pattern: String,
    expected_hits: usize,
    hits: usize,
    candidates: usize,
    total_docs: usize,
    prune_ratio: f64,
    bytes_fetched: u64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    sequential_p50_ms: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RunSummary {
    seed: u64,
    objects: usize,
    object_size: usize,
    total_bytes: u64,
    iterations: usize,
    warmup: usize,
    concurrency: usize,
    backend: BackendSummary,
    scenarios: Vec<ScenarioResult>,
}

struct SearchMeasurement {
    elapsed: Duration,
    hits: Vec<String>,
    candidates: usize,
    total_docs: usize,
    bytes_fetched: u64,
}

struct TimedScenario {
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    hits: usize,
    candidates: usize,
    total_docs: usize,
    bytes_fetched: u64,
}

struct S3Backend {
    bucket: String,
    region: String,
    endpoint: Option<String>,
    client: S3Client,
}

fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Seed {
            seed,
            objects,
            size,
        } => {
            let manifest = write_seed(seed, objects, size)?;
            println!("{}", gen::manifest_path().display());
            println!("objects={}", manifest.objects);
            Ok(())
        }
        Command::Upload { target } => upload(target),
        Command::Run {
            scenarios,
            iterations,
            warmup,
            concurrency,
        } => run(&scenarios, iterations, warmup, concurrency),
        Command::Report { out } => report(&out),
        Command::Compare { base, candidate } => compare(&base, &candidate),
        Command::Render { input } => render(&input),
    }
}

fn upload(target: UploadTarget) -> Result<()> {
    let manifest = read_manifest()?;
    match target {
        UploadTarget::Dir => upload_dir(&manifest),
        UploadTarget::S3 => upload_s3(&manifest),
    }
}

fn upload_dir(manifest: &SeedManifest) -> Result<()> {
    remove_dir(&local_index_dir())?;
    let corpus = LocalCorpus::new(&objects_dir())?;
    anyhow::ensure!(
        corpus.docs().len() == manifest.docs.len(),
        "seed manifest mismatch"
    );
    build_to_dir(&corpus, &local_index_dir(), Strategy::Sparse)?;
    println!("{}", local_index_dir().display());
    Ok(())
}

fn upload_s3(manifest: &SeedManifest) -> Result<()> {
    let backend = read_s3_backend(DEFAULT_CONCURRENCY)?;
    let uploads = manifest
        .docs
        .iter()
        .map(|doc| {
            Ok((
                format!("{S3_PREFIX}/{}", doc.key),
                std::fs::read(doc_path(doc))?,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    backend.client.put_many(&backend.bucket, uploads)?;
    let manifest_keys = manifest
        .docs
        .iter()
        .map(|doc| format!("{S3_PREFIX}/{}", doc.key))
        .collect::<BTreeSet<_>>();
    let objects = backend
        .client
        .list(&backend.bucket, &format!("{S3_PREFIX}/"))?
        .into_iter()
        .filter(|object| manifest_keys.contains(&object.key))
        .collect::<Vec<_>>();
    anyhow::ensure!(
        objects.len() == manifest.docs.len(),
        "uploaded {} objects but listed {} of them",
        manifest.docs.len(),
        objects.len()
    );
    let listing = objects
        .iter()
        .map(|object| (object.key.clone(), object.etag.clone()))
        .collect::<Vec<_>>();
    let cache_dir = reports_dir().join("s3-cache");
    let store = S3BlobStore::new(
        backend.client.clone(),
        backend.bucket.clone(),
        S3_PREFIX.to_owned(),
    );
    let client = backend.client;
    let bucket = backend.bucket.clone();
    let report = update_index(&store, &cache_dir, Strategy::Sparse, &listing, &|keys| {
        let objects = keys
            .iter()
            .map(|key| holys3_s3::ObjectMeta {
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
    println!(
        "s3://{}/{} ({} docs, {} segments)",
        backend.bucket,
        build_index_namespace(S3_PREFIX),
        report.total_docs,
        report.segments
    );
    Ok(())
}

fn run(scenarios_path: &Path, iterations: usize, warmup: usize, concurrency: usize) -> Result<()> {
    anyhow::ensure!(iterations > 0, "iterations must be greater than 0");
    anyhow::ensure!(concurrency > 0, "concurrency must be greater than 0");
    let scenarios = read_scenarios(scenarios_path)?;
    let manifest = read_manifest()?;
    let has_bucket = read_optional_env("HOLYS3_BENCH_BUCKET")?.is_some();
    let has_endpoint = read_optional_env("HOLYS3_BENCH_ENDPOINT")?.is_some();
    let summary = match (has_bucket, has_endpoint) {
        (true, _) => run_s3(scenarios, &manifest, iterations, warmup, concurrency)?,
        (false, true) => anyhow::bail!("set HOLYS3_BENCH_BUCKET with HOLYS3_BENCH_ENDPOINT"),
        (false, false) => run_dir(scenarios, &manifest, iterations, warmup, concurrency)?,
    };
    std::fs::create_dir_all(reports_dir())?;
    let file = std::fs::File::create(latest_run_path())?;
    serde_json::to_writer_pretty(file, &summary)?;
    println!("{}", latest_run_path().display());
    Ok(())
}

fn run_dir(
    scenarios: Vec<Scenario>,
    manifest: &SeedManifest,
    iterations: usize,
    warmup: usize,
    concurrency: usize,
) -> Result<RunSummary> {
    let reader = MmapIndexReader::open(&local_index_dir())?;
    let results = run_all(
        &reader,
        &LocalFetcher,
        &LocalFetcher,
        &scenarios,
        manifest,
        iterations,
        warmup,
    )?;
    Ok(RunSummary {
        seed: manifest.seed,
        objects: manifest.objects,
        object_size: manifest.size,
        total_bytes: manifest.total_bytes,
        iterations,
        warmup,
        concurrency,
        backend: BackendSummary {
            kind: "dir".to_owned(),
            bucket: None,
            region: None,
            endpoint: None,
        },
        scenarios: results,
    })
}

fn run_s3(
    scenarios: Vec<Scenario>,
    manifest: &SeedManifest,
    iterations: usize,
    warmup: usize,
    concurrency: usize,
) -> Result<RunSummary> {
    let backend = read_s3_backend(concurrency)?;
    let single_backend = read_s3_backend(1)?;
    let store = S3BlobStore::new(
        backend.client.clone(),
        backend.bucket.clone(),
        S3_PREFIX.to_owned(),
    );
    let cache_dir = reports_dir().join("s3-cache");
    let reader = SegmentedReader::open(Box::new(store), &cache_dir)?;
    let fetcher = S3Fetcher::new(backend.client.clone(), backend.bucket.clone());
    let single_fetcher = S3Fetcher::new(single_backend.client, backend.bucket.clone());
    let results = run_all(
        &reader,
        &fetcher,
        &single_fetcher,
        &scenarios,
        manifest,
        iterations,
        warmup,
    )?;
    Ok(RunSummary {
        seed: manifest.seed,
        objects: manifest.objects,
        object_size: manifest.size,
        total_bytes: manifest.total_bytes,
        iterations,
        warmup,
        concurrency,
        backend: BackendSummary {
            kind: "s3".to_owned(),
            bucket: Some(backend.bucket),
            region: Some(backend.region),
            endpoint: backend.endpoint,
        },
        scenarios: results,
    })
}

#[allow(clippy::too_many_arguments)]
fn run_all(
    reader: &dyn IndexReader,
    fetcher: &dyn DocFetcher,
    single_fetcher: &dyn DocFetcher,
    scenarios: &[Scenario],
    manifest: &SeedManifest,
    iterations: usize,
    warmup: usize,
) -> Result<Vec<ScenarioResult>> {
    scenarios
        .iter()
        .map(|scenario| {
            let expected = *manifest
                .expected_hits
                .get(&scenario.name)
                .with_context(|| format!("missing expected hit count for {}", scenario.name))?;
            let timed = time_scenario(reader, fetcher, scenario, expected, iterations, warmup)?;
            let single = time_scenario(
                reader,
                single_fetcher,
                scenario,
                expected,
                iterations,
                warmup,
            )?;
            anyhow::ensure!(
                timed.hits == single.hits,
                "{} indexed hits {} != concurrency=1 hits {}",
                scenario.name,
                timed.hits,
                single.hits
            );
            Ok(ScenarioResult {
                name: scenario.name.clone(),
                pattern: scenario.pattern.clone(),
                expected_hits: expected,
                hits: timed.hits,
                candidates: timed.candidates,
                total_docs: timed.total_docs,
                prune_ratio: timed.candidates as f64 / timed.total_docs as f64,
                bytes_fetched: timed.bytes_fetched,
                p50_ms: timed.p50_ms,
                p95_ms: timed.p95_ms,
                p99_ms: timed.p99_ms,
                sequential_p50_ms: single.p50_ms,
            })
        })
        .collect()
}

fn time_scenario(
    reader: &dyn IndexReader,
    fetcher: &dyn DocFetcher,
    scenario: &Scenario,
    expected: usize,
    iterations: usize,
    warmup: usize,
) -> Result<TimedScenario> {
    for _ in 0..warmup {
        let measurement = measure_search(reader, fetcher, &scenario.pattern)?;
        anyhow::ensure!(
            measurement.hits.len() == expected,
            "{} expected {} hits, got {}",
            scenario.name,
            expected,
            measurement.hits.len()
        );
    }
    let mut elapsed = Vec::with_capacity(iterations);
    let mut last = None;
    for _ in 0..iterations {
        let measurement = measure_search(reader, fetcher, &scenario.pattern)?;
        anyhow::ensure!(
            measurement.hits.len() == expected,
            "{} expected {} hits, got {}",
            scenario.name,
            expected,
            measurement.hits.len()
        );
        elapsed.push(measurement.elapsed);
        last = Some(measurement);
    }
    elapsed.sort_unstable();
    let measurement = last.context("missing measurement")?;
    Ok(TimedScenario {
        p50_ms: percentile_ms(&elapsed, 50),
        p95_ms: percentile_ms(&elapsed, 95),
        p99_ms: percentile_ms(&elapsed, 99),
        hits: measurement.hits.len(),
        candidates: measurement.candidates,
        total_docs: measurement.total_docs,
        bytes_fetched: measurement.bytes_fetched,
    })
}

fn measure_search(
    reader: &dyn IndexReader,
    fetcher: &dyn DocFetcher,
    pattern: &str,
) -> Result<SearchMeasurement> {
    let start = Instant::now();
    let (_, stats) = search_collect(reader, fetcher, pattern)?;
    Ok(SearchMeasurement {
        elapsed: start.elapsed(),
        hits: stats.hits,
        candidates: stats.candidates,
        total_docs: stats.total_docs,
        bytes_fetched: u64::try_from(stats.bytes_fetched)?,
    })
}

fn percentile_ms(values: &[Duration], percentile: usize) -> f64 {
    let len = values.len();
    let index = len
        .saturating_mul(percentile)
        .div_ceil(100)
        .saturating_sub(1);
    values[index.min(len - 1)].as_secs_f64() * 1000.0
}

fn report(out: &Path) -> Result<()> {
    let summary = read_summary(&latest_run_path())?;
    if let Some(parent) = out.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let file = std::fs::File::create(out)?;
    serde_json::to_writer_pretty(file, &summary)?;
    println!("{}", out.display());
    Ok(())
}

fn compare(base: &Path, candidate: &Path) -> Result<()> {
    let base = read_summary(base)?;
    let candidate = read_summary(candidate)?;
    let base_by_name = base
        .scenarios
        .iter()
        .map(|scenario| (scenario.name.as_str(), scenario))
        .collect::<BTreeMap<_, _>>();
    println!("| scenario | base p50 ms | candidate p50 ms | delta | p95 delta |");
    println!("|---|---:|---:|---:|---:|");
    for scenario in &candidate.scenarios {
        let base = base_by_name
            .get(scenario.name.as_str())
            .with_context(|| format!("missing base scenario {}", scenario.name))?;
        println!(
            "| {} | {:.3} | {:.3} | {:+.1}% | {:+.1}% |",
            scenario.name,
            base.p50_ms,
            scenario.p50_ms,
            percent_delta(base.p50_ms, scenario.p50_ms),
            percent_delta(base.p95_ms, scenario.p95_ms)
        );
    }
    Ok(())
}

fn render(input: &Path) -> Result<()> {
    let summary = read_summary(input)?;
    print!("{}", render_table(&summary));
    Ok(())
}

fn render_table(summary: &RunSummary) -> String {
    let mut out = String::new();
    out.push_str("| scenario | hits | candidates/total | prune ratio | bytes | p50 ms | p95 ms | p99 ms | concurrency=1 p50 ms |\n");
    out.push_str("|---|---:|---:|---:|---:|---:|---:|---:|---:|\n");
    for scenario in &summary.scenarios {
        out.push_str(&format!(
            "| {} | {} | {}/{} | {:.3} | {} | {:.3} | {:.3} | {:.3} | {:.3} |\n",
            scenario.name,
            scenario.hits,
            scenario.candidates,
            scenario.total_docs,
            scenario.prune_ratio,
            scenario.bytes_fetched,
            scenario.p50_ms,
            scenario.p95_ms,
            scenario.p99_ms,
            scenario.sequential_p50_ms
        ));
    }
    out
}

fn read_summary(path: &Path) -> Result<RunSummary> {
    let file = std::fs::File::open(path)?;
    Ok(serde_json::from_reader(file)?)
}

fn percent_delta(base: f64, candidate: f64) -> f64 {
    ((candidate - base) / base) * 100.0
}

fn read_s3_backend(concurrency: usize) -> Result<S3Backend> {
    let bucket = std::env::var("HOLYS3_BENCH_BUCKET")?;
    let region = std::env::var("HOLYS3_BENCH_REGION")?;
    let endpoint = read_optional_env("HOLYS3_BENCH_ENDPOINT")?;
    let client = s3_client_from_env(&region, endpoint.clone(), build_fetch_config(concurrency))?;
    Ok(S3Backend {
        bucket,
        region,
        endpoint,
        client,
    })
}

fn read_optional_env(name: &str) -> Result<Option<String>> {
    match std::env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(err) => Err(err.into()),
    }
}
