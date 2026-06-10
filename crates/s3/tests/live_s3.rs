use holys3_s3::resolve_credentials;
use holys3_s3::{FetchConfig, S3Client};

#[test]
fn list_and_get_roundtrip() {
    let Ok(bucket) = std::env::var("HOLYS3_TEST_BUCKET") else {
        eprintln!("skipping: set HOLYS3_TEST_BUCKET to run");
        return;
    };
    let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".into());
    let creds = resolve_credentials().unwrap().credentials;
    let client = S3Client::new(region, creds, None, FetchConfig::default()).unwrap();
    let objs = client.list(&bucket, "").unwrap();
    assert!(!objs.is_empty(), "bucket should have at least one object");
    let bytes = client.get(&bucket, &objs[0].key).unwrap().unwrap();
    assert!(!bytes.is_empty());
}
