use holys3_core::{BlobStore, Corpus, DocId};
use holys3_sigv4::{sign_get, sign_request, Credentials};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMeta {
    pub key: String,
    pub etag: String,
    pub size: u64,
}

/// Parse one `ListObjectsV2` XML page: returns (`objects`, `next_continuation_token`).
pub fn parse_list_v2(xml: &str) -> anyhow::Result<(Vec<ObjectMeta>, Option<String>)> {
    use quick_xml::events::Event;
    use quick_xml::Reader;
    let mut reader = Reader::from_str(xml);
    let mut objs = Vec::new();
    let mut next = None;
    let (mut key, mut etag, mut size) = (String::new(), String::new(), 0u64);
    let mut cur = String::new();
    let mut in_contents = false;
    loop {
        match reader.read_event()? {
            Event::Start(e) => {
                cur = String::from_utf8_lossy(e.name().as_ref()).into_owned();
                if cur == "Contents" {
                    in_contents = true;
                    key.clear();
                    etag.clear();
                    size = 0;
                }
            }
            Event::Text(t) => {
                let txt = t.unescape()?.into_owned();
                match cur.as_str() {
                    "Key" if in_contents => key = txt,
                    "ETag" if in_contents => etag = txt.trim_matches('"').to_owned(),
                    "Size" if in_contents => size = txt.parse().unwrap_or(0),
                    "NextContinuationToken" => next = Some(txt),
                    _ => {}
                }
            }
            Event::End(e) => {
                if String::from_utf8_lossy(e.name().as_ref()) == "Contents" {
                    in_contents = false;
                    objs.push(ObjectMeta {
                        key: key.clone(),
                        etag: etag.clone(),
                        size,
                    });
                }
                cur.clear();
            }
            Event::Eof => break,
            _ => {}
        }
    }
    Ok((objs, next))
}

#[derive(Clone)]
pub struct S3Client {
    pub region: String,
    pub creds: Credentials,
    http: reqwest::Client,
}

impl S3Client {
    pub fn new(region: String, creds: Credentials) -> S3Client {
        S3Client {
            region,
            creds,
            http: reqwest::Client::new(),
        }
    }

    fn host(&self, bucket: &str) -> String {
        format!("{bucket}.s3.{}.amazonaws.com", self.region)
    }

    /// Timestamp helper: returns (`amz_date`, `date`).
    fn now() -> (String, String) {
        let dt = time::OffsetDateTime::now_utc();
        let amz = dt
            .format(
                &time::format_description::parse("[year][month][day]T[hour][minute][second]Z")
                    .unwrap(),
            )
            .unwrap();
        let date = amz[..8].to_string();
        (amz, date)
    }

    pub async fn get(
        &self,
        bucket: &str,
        key: &str,
        range: Option<(u64, u64)>,
    ) -> anyhow::Result<Vec<u8>> {
        let host = self.host(bucket);
        let path = format!("/{key}");
        let (amz, date) = Self::now();
        let range_hdr = range.map(|(a, b)| format!("bytes={a}-{b}"));
        let extra: Vec<(&str, &str)> = match &range_hdr {
            Some(r) => vec![("range", r.as_str())],
            None => vec![],
        };
        let signed = sign_get(
            &self.creds,
            &self.region,
            &host,
            &path,
            "",
            &extra,
            &amz,
            &date,
        );
        let mut req = self
            .http
            .get(format!("https://{host}{path}"))
            .header("host", &host)
            .header("x-amz-date", &signed.x_amz_date)
            .header("x-amz-content-sha256", &signed.x_amz_content_sha256)
            .header("authorization", &signed.authorization);
        if let Some(r) = &range_hdr {
            req = req.header("range", r);
        }
        if let Some(tok) = &self.creds.session_token {
            req = req.header("x-amz-security-token", tok);
        }
        let resp = req.send().await?.error_for_status()?;
        Ok(resp.bytes().await?.to_vec())
    }

    pub async fn get_range(
        &self,
        bucket: &str,
        key: &str,
        start: u64,
        len: u64,
    ) -> anyhow::Result<Vec<u8>> {
        let end = start
            .checked_add(len)
            .and_then(|v| v.checked_sub(1))
            .ok_or_else(|| anyhow::anyhow!("invalid empty S3 range"))?;
        self.get(bucket, key, Some((start, end))).await
    }

    pub async fn put(&self, bucket: &str, key: &str, body: &[u8]) -> anyhow::Result<()> {
        let host = self.host(bucket);
        let path = format!("/{key}");
        let (amz, date) = Self::now();
        let signed = sign_request(
            "PUT",
            &self.creds,
            &self.region,
            &host,
            &path,
            "",
            &[],
            &amz,
            &date,
            "UNSIGNED-PAYLOAD",
        );
        let mut req = self
            .http
            .put(format!("https://{host}{path}"))
            .header("host", &host)
            .header("x-amz-date", &signed.x_amz_date)
            .header("x-amz-content-sha256", &signed.x_amz_content_sha256)
            .header("authorization", &signed.authorization)
            .body(body.to_vec());
        if let Some(tok) = &self.creds.session_token {
            req = req.header("x-amz-security-token", tok);
        }
        req.send().await?.error_for_status()?;
        Ok(())
    }

    pub async fn list(&self, bucket: &str, prefix: &str) -> anyhow::Result<Vec<ObjectMeta>> {
        let host = self.host(bucket);
        let mut all = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut params = vec![
                ("list-type", "2".to_owned()),
                ("prefix", prefix.to_owned()),
            ];
            if let Some(t) = &token {
                params.push(("continuation-token", t.clone()));
            }
            params.sort_by(|a, b| a.0.cmp(b.0));
            let canonical_query = params
                .iter()
                .map(|(k, v)| format!("{}={}", enc(k), enc(v)))
                .collect::<Vec<_>>()
                .join("&");
            let (amz, date) = Self::now();
            let signed = sign_get(
                &self.creds,
                &self.region,
                &host,
                "/",
                &canonical_query,
                &[],
                &amz,
                &date,
            );
            let mut req = self
                .http
                .get(format!("https://{host}/?{canonical_query}"))
                .header("host", &host)
                .header("x-amz-date", &signed.x_amz_date)
                .header("x-amz-content-sha256", &signed.x_amz_content_sha256)
                .header("authorization", &signed.authorization);
            if let Some(tok) = &self.creds.session_token {
                req = req.header("x-amz-security-token", tok);
            }
            let body = req.send().await?.error_for_status()?.text().await?;
            let (objs, next) = parse_list_v2(&body)?;
            all.extend(objs);
            match next {
                Some(t) => token = Some(t),
                None => break,
            }
        }
        Ok(all)
    }
}

pub struct S3BlobStore {
    client: S3Client,
    bucket: String,
    prefix: String,
    rt: tokio::runtime::Handle,
}

impl S3BlobStore {
    pub fn new(
        client: S3Client,
        bucket: String,
        prefix: String,
        rt: tokio::runtime::Handle,
    ) -> S3BlobStore {
        S3BlobStore {
            client,
            bucket,
            prefix,
            rt,
        }
    }

    fn build_key(&self, name: &str) -> String {
        build_index_key(&self.prefix, name)
    }
}

impl BlobStore for S3BlobStore {
    fn put(&self, name: &str, bytes: &[u8]) -> anyhow::Result<()> {
        tokio::task::block_in_place(|| {
            self.rt
                .block_on(self.client.put(&self.bucket, &self.build_key(name), bytes))
        })
    }

    fn get(&self, name: &str) -> anyhow::Result<Vec<u8>> {
        tokio::task::block_in_place(|| {
            self.rt
                .block_on(self.client.get(&self.bucket, &self.build_key(name), None))
        })
    }

    fn get_range(&self, name: &str, start: u64, len: u64) -> anyhow::Result<Vec<u8>> {
        tokio::task::block_in_place(|| {
            self.rt.block_on(
                self.client
                    .get_range(&self.bucket, &self.build_key(name), start, len),
            )
        })
    }
}

pub fn build_index_key(prefix: &str, name: &str) -> String {
    format!(
        "{}/{}",
        build_index_namespace(prefix),
        name.trim_start_matches('/')
    )
}

pub fn build_index_namespace(prefix: &str) -> String {
    let prefix = prefix
        .split('/')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("/");
    if prefix.is_empty() {
        ".holys3".into()
    } else {
        format!("{prefix}/.holys3")
    }
}

pub fn is_index_key(prefix: &str, key: &str) -> bool {
    key.starts_with(&format!("{}/", build_index_namespace(prefix)))
}

/// Corpus over an S3 prefix. Loads object list eagerly; fetches bytes on demand.
pub struct S3Corpus {
    client: S3Client,
    bucket: String,
    docs: Vec<(DocId, String)>,
    rt: tokio::runtime::Handle,
}

impl S3Corpus {
    pub fn new(
        client: S3Client,
        bucket: String,
        objects: Vec<ObjectMeta>,
        rt: tokio::runtime::Handle,
    ) -> S3Corpus {
        let docs = objects
            .iter()
            .enumerate()
            .map(|(i, o)| (i as DocId, o.key.clone()))
            .collect();
        S3Corpus {
            client,
            bucket,
            docs,
            rt,
        }
    }

    pub fn from_docs(
        client: S3Client,
        bucket: String,
        docs: Vec<(DocId, String)>,
        rt: tokio::runtime::Handle,
    ) -> S3Corpus {
        S3Corpus {
            client,
            bucket,
            docs,
            rt,
        }
    }
}

impl Corpus for S3Corpus {
    fn docs(&self) -> &[(DocId, String)] {
        &self.docs
    }

    fn fetch(&self, id: DocId) -> anyhow::Result<Vec<u8>> {
        let key = self.docs[id as usize].1.clone();
        tokio::task::block_in_place(|| self.rt.block_on(self.client.get(&self.bucket, &key, None)))
    }
}

/// AWS-style query-component encoding (space -> %20, etc.).
fn enc(s: &str) -> String {
    let mut out = String::new();
    for &b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_two_objects_with_token() {
        let xml = r#"<?xml version="1.0"?>
        <ListBucketResult>
          <Contents><Key>a.txt</Key><Size>10</Size><ETag>"abc"</ETag></Contents>
          <Contents><Key>b/c.log</Key><Size>20</Size><ETag>"def"</ETag></Contents>
          <NextContinuationToken>TOK</NextContinuationToken>
        </ListBucketResult>"#;
        let (objs, next) = parse_list_v2(xml).unwrap();
        assert_eq!(
            objs,
            vec![
                ObjectMeta {
                    key: "a.txt".into(),
                    etag: "abc".into(),
                    size: 10
                },
                ObjectMeta {
                    key: "b/c.log".into(),
                    etag: "def".into(),
                    size: 20
                },
            ]
        );
        assert_eq!(next.as_deref(), Some("TOK"));
    }

    #[test]
    fn index_keys_are_normalized() {
        assert_eq!(build_index_key("", "CURRENT"), ".holys3/CURRENT");
        assert_eq!(
            build_index_key("/root//path/", "/builds/1/footer.bin"),
            "root/path/.holys3/builds/1/footer.bin"
        );
        assert!(is_index_key("root/path", "root/path/.holys3/CURRENT"));
        assert!(!is_index_key("root/path", "root/path/file.txt"));
    }
}
