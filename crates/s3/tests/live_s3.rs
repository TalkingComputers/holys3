use holys3_s3::S3Client;
use holys3_sigv4::resolve;

#[tokio::test]
async fn list_and_get_roundtrip() {
    let Ok(bucket) = std::env::var("HOLYS3_TEST_BUCKET") else {
        eprintln!("skipping: set HOLYS3_TEST_BUCKET to run");
        return;
    };
    let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".into());
    let creds = resolve("default").unwrap();
    let client = S3Client::new(region, creds);
    let objs = client.list(&bucket, "").await.unwrap();
    assert!(!objs.is_empty(), "bucket should have at least one object");
    let bytes = client.get(&bucket, &objs[0].key, None).await.unwrap();
    assert!(!bytes.is_empty());
}
