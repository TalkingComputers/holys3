use holys3_core::{Corpus, DocId, Strategy};
use holys3_index::{build_to_store, compute_build_id, search_via_store, StoreIndexReader};
use holys3_s3::{is_index_key, S3BlobStore, S3Client, S3Corpus};
use holys3_sigv4::resolve;

#[tokio::test(flavor = "multi_thread")]
async fn live_s3_index_search_roundtrip() -> anyhow::Result<()> {
    let bucket = match std::env::var("HOLYS3_TEST_BUCKET") {
        Ok(bucket) => bucket,
        Err(_) => {
            eprintln!("skipping: set HOLYS3_TEST_BUCKET to run");
            return Ok(());
        }
    };
    let region = std::env::var("AWS_REGION")?;
    let creds = resolve("default")?;
    let client = S3Client::new(region, creds);
    let objects = client
        .list(&bucket, "")
        .await?
        .into_iter()
        .filter(|object| !is_index_key("", &object.key))
        .collect::<Vec<_>>();
    let object_ids = objects
        .iter()
        .map(|object| (object.key.clone(), object.etag.clone()))
        .collect::<Vec<_>>();
    let build_id = compute_build_id(&object_ids);
    let rt = tokio::runtime::Handle::current();
    let corpus = S3Corpus::new(client.clone(), bucket.clone(), objects, rt.clone());
    let store = S3BlobStore::new(client.clone(), bucket.clone(), String::new(), rt.clone());
    build_to_store(&corpus, &store, Strategy::Trigram, &build_id)?;
    let cache_dir = tempfile::tempdir()?;
    let reader = StoreIndexReader::open(
        Box::new(S3BlobStore::new(client, bucket, String::new(), rt)),
        cache_dir.path(),
    )?;
    assert_hit(&reader, &corpus, "world", "b.txt")?;
    assert_hit(&reader, &corpus, "handleClick", "a.rs")?;
    assert_hit(&reader, &corpus, "EMAIL", "c/d.log")?;
    Ok(())
}

fn assert_hit(
    reader: &StoreIndexReader,
    corpus: &dyn Corpus,
    pattern: &str,
    expected_key: &str,
) -> anyhow::Result<()> {
    let hits = search_via_store(reader, corpus, pattern)?;
    let keys = hits
        .iter()
        .map(|id| key_for_doc(reader, *id))
        .collect::<Vec<_>>();
    assert!(
        keys.iter().any(|key| key == expected_key),
        "pattern {pattern} expected {expected_key}, got {keys:?}"
    );
    Ok(())
}

fn key_for_doc(reader: &StoreIndexReader, id: DocId) -> String {
    reader.docs()[id as usize].1.clone()
}
