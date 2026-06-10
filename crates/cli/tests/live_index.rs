use holys3_core::{DocFetcher, Strategy};
use holys3_index::{build_to_store, compute_build_id, search_collect, StoreIndexReader};
use holys3_s3::resolve_credentials;
use holys3_s3::{is_index_key, FetchConfig, S3BlobStore, S3Client, S3Corpus, S3Fetcher};

#[test]
fn live_s3_index_search_roundtrip() -> anyhow::Result<()> {
    let bucket = match std::env::var("HOLYS3_TEST_BUCKET") {
        Ok(bucket) => bucket,
        Err(_) => {
            eprintln!("skipping: set HOLYS3_TEST_BUCKET to run");
            return Ok(());
        }
    };
    let region = std::env::var("AWS_REGION")?;
    let creds = resolve_credentials()?.credentials;
    let client = S3Client::new(region, creds, None, FetchConfig::default())?;
    let objects = client
        .list(&bucket, "")?
        .into_iter()
        .filter(|object| !is_index_key("", &object.key))
        .collect::<Vec<_>>();
    let object_ids = objects
        .iter()
        .map(|object| (object.key.clone(), object.etag.clone()))
        .collect::<Vec<_>>();
    let build_id = compute_build_id(&object_ids, Strategy::Trigram);
    let corpus = S3Corpus::new(client.clone(), bucket.clone(), &objects);
    let store = S3BlobStore::new(client.clone(), bucket.clone(), String::new());
    build_to_store(&corpus, &store, Strategy::Trigram, &build_id)?;
    let cache_dir = tempfile::tempdir()?;
    let reader = StoreIndexReader::open(
        Box::new(S3BlobStore::new(
            client.clone(),
            bucket.clone(),
            String::new(),
        )),
        cache_dir.path(),
    )?;
    let fetcher = S3Fetcher::new(client, bucket);
    assert_hit(&reader, &fetcher, "world", "b.txt")?;
    assert_hit(&reader, &fetcher, "handleClick", "a.rs")?;
    assert_hit(&reader, &fetcher, "EMAIL", "c/d.log")?;
    Ok(())
}

fn assert_hit(
    reader: &StoreIndexReader,
    fetcher: &dyn DocFetcher,
    pattern: &str,
    expected_key: &str,
) -> anyhow::Result<()> {
    let hits = search_collect(reader, fetcher, pattern)?.1.hits;
    assert!(
        hits.iter().any(|key| key == expected_key),
        "pattern {pattern} expected {expected_key}, got {hits:?}"
    );
    Ok(())
}
