use holys3_core::Strategy;
use holys3_index::{search_collect, update_index, SegmentedReader, SourceIdentity, UpdateOptions};
use holys3_s3::{is_index_key, FetchConfig, S3BlobStore, S3Client, S3Corpus};

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
    let client = S3Client::connect(Some(region), None, FetchConfig::default())?;
    let source = SourceIdentity::S3 {
        endpoint: client.endpoint_identity(),
        bucket: bucket.clone(),
        prefix: String::new(),
    };
    let listing = client
        .list(&bucket, "")?
        .into_iter()
        .filter(|object| !is_index_key("", &object.key))
        .map(|object| (object.key, object.etag, object.size))
        .collect::<Vec<_>>();
    let store = S3BlobStore::new(client.clone(), bucket.clone(), String::new());
    let cache_dir = tempfile::tempdir()?;
    let factory_client = client.clone();
    let factory_bucket = bucket.clone();
    update_index(
        &store,
        cache_dir.path(),
        &source,
        Some(Strategy::Trigram),
        &listing,
        UpdateOptions::default(),
        &|shard| {
            Ok(Box::new(S3Corpus::new(
                factory_client.clone(),
                factory_bucket.clone(),
                shard,
            )))
        },
    )?;
    let reader = SegmentedReader::open(
        Box::new(S3BlobStore::new(
            client.clone(),
            bucket.clone(),
            String::new(),
        )),
        cache_dir.path(),
        &source,
    )?;
    assert_hit(&reader, "world", "b.txt")?;
    assert_hit(&reader, "handleClick", "a.rs")?;
    assert_hit(&reader, "EMAIL", "c/d.log")?;
    Ok(())
}

fn assert_hit(reader: &SegmentedReader, pattern: &str, expected_key: &str) -> anyhow::Result<()> {
    let hits = search_collect(reader, pattern)?.1.hits;
    assert!(
        hits.iter().any(|key| key == expected_key),
        "pattern {pattern} expected {expected_key}, got {hits:?}"
    );
    Ok(())
}
