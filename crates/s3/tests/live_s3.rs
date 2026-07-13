use holys3_s3::{FetchConfig, S3Client};

#[test]
fn list_and_get_roundtrip() {
    let Ok(bucket) = std::env::var("HOLYS3_TEST_BUCKET") else {
        eprintln!("skipping: set HOLYS3_TEST_BUCKET to run");
        return;
    };
    let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".into());
    let client = S3Client::connect(Some(region), None, FetchConfig::default()).unwrap();
    let objs = client.list(&bucket, "").unwrap();
    assert!(!objs.is_empty(), "bucket should have at least one object");
    let bytes = client.get(&bucket, &objs[0].key).unwrap().unwrap();
    assert!(!bytes.is_empty());
}

#[test]
fn special_key_roundtrip() -> anyhow::Result<()> {
    let Ok(bucket) = std::env::var("HOLYS3_TEST_BUCKET") else {
        eprintln!("skipping: set HOLYS3_TEST_BUCKET to run");
        return Ok(());
    };
    let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".into());
    let client = S3Client::connect(Some(region), None, FetchConfig::default())?;
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let prefix = format!(".holys3-live-test/{nonce}/");
    let key = format!("{prefix}space and+plus%.txt");
    let expected = b"special key".to_vec();
    client.put_many(&bucket, vec![(key.clone(), expected.clone())])?;
    let listed = client.list(&bucket, &prefix);
    let fetched = client.get(&bucket, &key);
    client.delete(&bucket, &key)?;
    let listed = listed?;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].key, key);
    assert_eq!(listed[0].size, expected.len() as u64);
    assert_eq!(fetched?, Some(expected));
    Ok(())
}
