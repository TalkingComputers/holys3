use super::*;
use std::io::SeekFrom;
use std::path::Path;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

impl S3Client {
    pub fn put(&self, bucket: &str, key: &str, body: &[u8]) -> Result<()> {
        self.0.rt.block_on(self.put_async(bucket, key, body))
    }

    pub fn put_file(&self, bucket: &str, key: &str, path: &Path) -> Result<()> {
        let len = std::fs::metadata(path)?.len();
        if len <= MULTIPART_PART_SIZE as u64 {
            return self.put(bucket, key, &std::fs::read(path)?);
        }
        self.0
            .rt
            .block_on(self.put_file_multipart(bucket, key, path, len))
    }

    pub(super) async fn put_async(&self, bucket: &str, key: &str, body: &[u8]) -> Result<()> {
        if body.len() > MULTIPART_PART_SIZE {
            return self.put_multipart(bucket, key, body).await;
        }
        let req = S3Request {
            method: "PUT",
            bucket,
            key: Some(key),
            canonical_query: "",
            range: None,
            body: Some(Bytes::copy_from_slice(body)),
            precondition: None,
        };
        self.send_resilient(&req, None)
            .await?
            .with_context(|| format!("PUT s3://{bucket}/{key} returned HTTP 404"))?;
        Ok(())
    }

    async fn put_multipart(&self, bucket: &str, key: &str, body: &[u8]) -> Result<()> {
        let part_size = multipart_part_size(u64::try_from(body.len())?)?;
        let upload_id = self.start_multipart(bucket, key).await?;
        let parts = self
            .upload_parts(bucket, key, &upload_id, body, part_size)
            .await;
        let parts = match parts {
            Ok(parts) => parts,
            Err(error) => {
                self.abort_multipart(bucket, key, &upload_id).await;
                return Err(error);
            }
        };
        self.finish_multipart(bucket, key, &upload_id, &parts).await
    }

    async fn put_file_multipart(
        &self,
        bucket: &str,
        key: &str,
        path: &Path,
        len: u64,
    ) -> Result<()> {
        let part_size = multipart_part_size(len)?;
        let upload_id = self.start_multipart(bucket, key).await?;
        let parts = self
            .upload_file_parts(bucket, key, &upload_id, path, len, part_size)
            .await;
        let parts = match parts {
            Ok(parts) => parts,
            Err(error) => {
                self.abort_multipart(bucket, key, &upload_id).await;
                return Err(error);
            }
        };
        self.finish_multipart(bucket, key, &upload_id, &parts).await
    }

    async fn start_multipart(&self, bucket: &str, key: &str) -> Result<String> {
        let initiate = canonical_query(&mut vec![("uploads", String::new())]);
        let (_, _, body) = self
            .send_resilient(
                &S3Request {
                    method: "POST",
                    bucket,
                    key: Some(key),
                    canonical_query: &initiate,
                    range: None,
                    body: None,
                    precondition: None,
                },
                None,
            )
            .await?
            .with_context(|| format!("initiate multipart s3://{bucket}/{key}: HTTP 404"))?;
        read_xml_text(&body, b"UploadId")?
            .with_context(|| format!("initiate multipart s3://{bucket}/{key}: no UploadId"))
    }

    async fn finish_multipart(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        parts: &[(usize, String)],
    ) -> Result<()> {
        let mut body = String::from(
            r#"<CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/">"#,
        );
        for (number, etag) in parts {
            body.push_str(&format!(
                "<Part><PartNumber>{number}</PartNumber><ETag>{}</ETag></Part>",
                quick_xml::escape::escape(etag)
            ));
        }
        body.push_str("</CompleteMultipartUpload>");
        let query = canonical_query(&mut vec![("uploadId", upload_id.to_owned())]);
        let completed = self
            .send_resilient(
                &S3Request {
                    method: "POST",
                    bucket,
                    key: Some(key),
                    canonical_query: &query,
                    range: None,
                    body: Some(Bytes::from(body)),
                    precondition: None,
                },
                None,
            )
            .await;
        match completed {
            Ok(Some((_, _, response))) => {
                if let Err(error) = validate_complete_multipart(&response) {
                    self.abort_multipart(bucket, key, upload_id).await;
                    return Err(error).with_context(|| {
                        format!("complete multipart s3://{bucket}/{key} returned an error")
                    });
                }
                Ok(())
            }
            Ok(None) => {
                self.abort_multipart(bucket, key, upload_id).await;
                anyhow::bail!("complete multipart s3://{bucket}/{key}: HTTP 404")
            }
            Err(error) => {
                self.abort_multipart(bucket, key, upload_id).await;
                Err(error)
            }
        }
    }

    async fn upload_parts(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        body: &[u8],
        part_size: usize,
    ) -> Result<Vec<(usize, String)>> {
        let mut uploads = stream::iter(body.chunks(part_size).enumerate().map(
            |(index, chunk)| async move {
                let number = index + 1;
                let query = canonical_query(&mut vec![
                    ("partNumber", number.to_string()),
                    ("uploadId", upload_id.to_owned()),
                ]);
                let (_, etag, _) = self
                    .send_resilient(
                        &S3Request {
                            method: "PUT",
                            bucket,
                            key: Some(key),
                            canonical_query: &query,
                            range: None,
                            body: Some(Bytes::copy_from_slice(chunk)),
                            precondition: None,
                        },
                        None,
                    )
                    .await?
                    .with_context(|| format!("upload part {number} of s3://{bucket}/{key}"))?;
                let etag =
                    etag.with_context(|| format!("part {number} of s3://{bucket}/{key}: no ETag"))?;
                Ok::<_, anyhow::Error>((number, etag))
            },
        ))
        .buffer_unordered(multipart_concurrency(part_size));
        let mut parts = Vec::new();
        while let Some(result) = uploads.next().await {
            parts.push(result?);
        }
        parts.sort_unstable_by_key(|(number, _)| *number);
        Ok(parts)
    }

    async fn upload_file_parts(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        path: &Path,
        len: u64,
        part_size: usize,
    ) -> Result<Vec<(usize, String)>> {
        let part_len = part_size as u64;
        let part_count = len.div_ceil(part_len);
        let mut uploads = stream::iter((0..part_count).map(|part_index| {
            let path = path.to_path_buf();
            async move {
                let start = part_index * part_len;
                let read_len = usize::try_from((len - start).min(part_len))?;
                let mut body = vec![0u8; read_len];
                let mut file = tokio::fs::File::open(&path).await?;
                file.seek(SeekFrom::Start(start)).await?;
                file.read_exact(&mut body).await?;
                let number = usize::try_from(part_index + 1)?;
                let query = canonical_query(&mut vec![
                    ("partNumber", number.to_string()),
                    ("uploadId", upload_id.to_owned()),
                ]);
                let (_, etag, _) = self
                    .send_resilient(
                        &S3Request {
                            method: "PUT",
                            bucket,
                            key: Some(key),
                            canonical_query: &query,
                            range: None,
                            body: Some(Bytes::from(body)),
                            precondition: None,
                        },
                        None,
                    )
                    .await?
                    .with_context(|| format!("upload part {number} of s3://{bucket}/{key}"))?;
                let etag =
                    etag.with_context(|| format!("part {number} of s3://{bucket}/{key}: no ETag"))?;
                Ok::<_, anyhow::Error>((number, etag))
            }
        }))
        .buffer_unordered(multipart_concurrency(part_size));
        let mut parts = Vec::with_capacity(usize::try_from(part_count)?);
        while let Some(result) = uploads.next().await {
            parts.push(result?);
        }
        parts.sort_unstable_by_key(|(number, _)| *number);
        Ok(parts)
    }

    async fn abort_multipart(&self, bucket: &str, key: &str, upload_id: &str) {
        let query = canonical_query(&mut vec![("uploadId", upload_id.to_owned())]);
        let request = S3Request {
            method: "DELETE",
            bucket,
            key: Some(key),
            canonical_query: &query,
            range: None,
            body: None,
            precondition: None,
        };
        if self.send_resilient(&request, None).await.is_err() {
            eprintln!("warning: failed to abort multipart upload of s3://{bucket}/{key}");
        }
    }
}
