use super::{
    classify_sdk_error, multipart_concurrency, multipart_part_size, upload_deadline, Outcome,
    S3Client, MULTIPART_PART_SIZE,
};
use anyhow::{Context, Result};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_smithy_types::byte_stream::Length;
use bytes::Bytes;
use futures::stream::{self, StreamExt};
use holys3_core::{ProgressEvent, ProgressSender};
use std::path::{Path, PathBuf};

const FILE_STREAM_BUFFER_SIZE: usize = 1024 * 1024;

#[derive(Clone)]
enum PartSource {
    Bytes(Bytes),
    File { path: PathBuf, start: u64, len: u64 },
}

impl PartSource {
    fn count_bytes(&self) -> Result<usize> {
        match self {
            Self::Bytes(bytes) => Ok(bytes.len()),
            Self::File { len, .. } => Ok(usize::try_from(*len)?),
        }
    }

    async fn build_stream(&self) -> Result<ByteStream> {
        match self {
            Self::Bytes(bytes) => Ok(ByteStream::from(bytes.clone())),
            Self::File { path, start, len } => Ok(ByteStream::read_from()
                .path(path)
                .offset(*start)
                .length(Length::Exact(*len))
                .buffer_size(FILE_STREAM_BUFFER_SIZE)
                .build()
                .await?),
        }
    }
}

impl S3Client {
    pub fn put(&self, bucket: &str, key: &str, body: &[u8]) -> Result<()> {
        self.put_with_progress(bucket, key, body, None)
    }

    pub fn put_with_progress(
        &self,
        bucket: &str,
        key: &str,
        body: &[u8],
        progress: Option<&ProgressSender>,
    ) -> Result<()> {
        if let Some(progress) = progress {
            progress.emit(ProgressEvent::UploadStarted {
                bytes: body.len() as u64,
            });
        }
        self.0
            .rt
            .block_on(self.put_async(bucket, key, body, progress))
    }

    pub fn put_file(&self, bucket: &str, key: &str, path: &Path) -> Result<()> {
        self.put_file_with_progress(bucket, key, path, None)
    }

    pub fn put_file_with_progress(
        &self,
        bucket: &str,
        key: &str,
        path: &Path,
        progress: Option<&ProgressSender>,
    ) -> Result<()> {
        let len = std::fs::metadata(path)?.len();
        if let Some(progress) = progress {
            progress.emit(ProgressEvent::UploadStarted { bytes: len });
        }
        if len <= MULTIPART_PART_SIZE as u64 {
            return self.0.rt.block_on(self.put_async(
                bucket,
                key,
                &std::fs::read(path)?,
                progress,
            ));
        }
        self.0
            .rt
            .block_on(self.put_file_multipart(bucket, key, path, len, progress))
    }

    pub(super) async fn put_async(
        &self,
        bucket: &str,
        key: &str,
        body: &[u8],
        progress: Option<&ProgressSender>,
    ) -> Result<()> {
        if body.len() > MULTIPART_PART_SIZE {
            return self.put_multipart(bucket, key, body, progress).await;
        }
        self.put_bytes(bucket, key, Bytes::copy_from_slice(body), progress)
            .await
    }

    async fn put_bytes(
        &self,
        bucket: &str,
        key: &str,
        body: Bytes,
        progress: Option<&ProgressSender>,
    ) -> Result<()> {
        let label = format!("PUT s3://{bucket}/{key}");
        let body_len = body.len();
        self.run_resilient(&label, None, || {
            let request = self
                .0
                .upload_sdk
                .put_object()
                .bucket(bucket)
                .key(key)
                .body(ByteStream::from(body.clone()));
            async move {
                match tokio::time::timeout(upload_deadline(body_len), request.send()).await {
                    Ok(Ok(output)) => Outcome::Success(output),
                    Ok(Err(error)) => classify_sdk_error(error),
                    Err(error) => Outcome::Transient(error.into()),
                }
            }
        })
        .await?
        .with_context(|| format!("{label}: bucket not found"))?;
        if let Some(progress) = progress {
            progress.emit(ProgressEvent::UploadedChunk {
                bytes: body_len as u64,
            });
        }
        Ok(())
    }

    async fn put_multipart(
        &self,
        bucket: &str,
        key: &str,
        body: &[u8],
        progress: Option<&ProgressSender>,
    ) -> Result<()> {
        let part_size = multipart_part_size(u64::try_from(body.len())?)?;
        let upload_id = self.start_multipart(bucket, key).await?;
        let parts = self
            .upload_parts(bucket, key, &upload_id, body, part_size, progress)
            .await;
        let parts = match parts {
            Ok(parts) => parts,
            Err(error) => {
                self.abort_multipart(bucket, key, &upload_id).await;
                return Err(error);
            }
        };
        self.finish_multipart(bucket, key, &upload_id, parts).await
    }

    async fn put_file_multipart(
        &self,
        bucket: &str,
        key: &str,
        path: &Path,
        len: u64,
        progress: Option<&ProgressSender>,
    ) -> Result<()> {
        let part_size = multipart_part_size(len)?;
        let upload_id = self.start_multipart(bucket, key).await?;
        let parts = self
            .upload_file_parts(bucket, key, &upload_id, path, len, part_size, progress)
            .await;
        let parts = match parts {
            Ok(parts) => parts,
            Err(error) => {
                self.abort_multipart(bucket, key, &upload_id).await;
                return Err(error);
            }
        };
        self.finish_multipart(bucket, key, &upload_id, parts).await
    }

    async fn start_multipart(&self, bucket: &str, key: &str) -> Result<String> {
        let label = format!("initiate multipart s3://{bucket}/{key}");
        self.run_resilient(&label, None, || {
            let request = self.0.sdk.create_multipart_upload().bucket(bucket).key(key);
            async move {
                match request.send().await {
                    Ok(output) => Outcome::Success(output),
                    Err(error) => classify_sdk_error(error),
                }
            }
        })
        .await?
        .with_context(|| format!("{label}: bucket not found"))?
        .upload_id()
        .with_context(|| format!("{label}: no UploadId"))
        .map(str::to_owned)
    }

    async fn finish_multipart(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        parts: Vec<CompletedPart>,
    ) -> Result<()> {
        let label = format!("complete multipart s3://{bucket}/{key}");
        let upload = CompletedMultipartUpload::builder()
            .set_parts(Some(parts))
            .build();
        let result = self
            .run_resilient(&label, None, || {
                let request = self
                    .0
                    .sdk
                    .complete_multipart_upload()
                    .bucket(bucket)
                    .key(key)
                    .upload_id(upload_id)
                    .multipart_upload(upload.clone());
                async move {
                    match request.send().await {
                        Ok(output) => Outcome::Success(output),
                        Err(error) => classify_sdk_error(error),
                    }
                }
            })
            .await;
        match result {
            Ok(Some(_)) => Ok(()),
            Ok(None) => {
                self.abort_multipart(bucket, key, upload_id).await;
                anyhow::bail!("{label}: bucket not found")
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
        progress: Option<&ProgressSender>,
    ) -> Result<Vec<CompletedPart>> {
        let mut uploads = stream::iter(body.chunks(part_size).enumerate().map(
            |(index, chunk)| async move {
                let number = i32::try_from(index + 1)?;
                let part = self
                    .upload_part(
                        bucket,
                        key,
                        upload_id,
                        number,
                        PartSource::Bytes(Bytes::copy_from_slice(chunk)),
                    )
                    .await?;
                if let Some(progress) = progress {
                    progress.emit(ProgressEvent::UploadedChunk {
                        bytes: chunk.len() as u64,
                    });
                }
                anyhow::Ok(part)
            },
        ))
        .buffer_unordered(multipart_concurrency(part_size));
        let mut parts = Vec::new();
        while let Some(result) = uploads.next().await {
            parts.push(result?);
        }
        parts.sort_unstable_by_key(|part| part.part_number().unwrap_or_default());
        Ok(parts)
    }

    #[allow(clippy::too_many_arguments)]
    async fn upload_file_parts(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        path: &Path,
        len: u64,
        part_size: usize,
        progress: Option<&ProgressSender>,
    ) -> Result<Vec<CompletedPart>> {
        let part_len = part_size as u64;
        let part_count = len.div_ceil(part_len);
        let mut uploads = stream::iter((0..part_count).map(|part_index| {
            let path = path.to_path_buf();
            async move {
                let start = part_index * part_len;
                let read_len = (len - start).min(part_len);
                let part = self
                    .upload_part(
                        bucket,
                        key,
                        upload_id,
                        i32::try_from(part_index + 1)?,
                        PartSource::File {
                            path,
                            start,
                            len: read_len,
                        },
                    )
                    .await?;
                if let Some(progress) = progress {
                    progress.emit(ProgressEvent::UploadedChunk { bytes: read_len });
                }
                anyhow::Ok(part)
            }
        }))
        .buffer_unordered(multipart_concurrency(part_size));
        let mut parts = Vec::with_capacity(usize::try_from(part_count)?);
        while let Some(result) = uploads.next().await {
            parts.push(result?);
        }
        parts.sort_unstable_by_key(|part| part.part_number().unwrap_or_default());
        Ok(parts)
    }

    async fn upload_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        number: i32,
        source: PartSource,
    ) -> Result<CompletedPart> {
        let label = format!("upload part {number} of s3://{bucket}/{key}");
        let deadline = upload_deadline(source.count_bytes()?);
        let output = self
            .run_resilient(&label, None, || {
                let source = source.clone();
                async move {
                    let body = match source.build_stream().await {
                        Ok(body) => body,
                        Err(error) => return Outcome::Fatal(error),
                    };
                    let request = self
                        .0
                        .upload_sdk
                        .upload_part()
                        .bucket(bucket)
                        .key(key)
                        .upload_id(upload_id)
                        .part_number(number)
                        .body(body);
                    match tokio::time::timeout(deadline, request.send()).await {
                        Ok(Ok(output)) => Outcome::Success(output),
                        Ok(Err(error)) => classify_sdk_error(error),
                        Err(error) => Outcome::Transient(error.into()),
                    }
                }
            })
            .await?
            .with_context(|| format!("{label}: bucket not found"))?;
        let etag = output
            .e_tag()
            .with_context(|| format!("{label}: no ETag"))?;
        Ok(CompletedPart::builder()
            .part_number(number)
            .e_tag(etag)
            .build())
    }

    async fn abort_multipart(&self, bucket: &str, key: &str, upload_id: &str) {
        let label = format!("abort multipart s3://{bucket}/{key}");
        let result = self
            .run_resilient(&label, None, || {
                let request = self
                    .0
                    .sdk
                    .abort_multipart_upload()
                    .bucket(bucket)
                    .key(key)
                    .upload_id(upload_id);
                async move {
                    match request.send().await {
                        Ok(output) => Outcome::Success(output),
                        Err(error) => classify_sdk_error(error),
                    }
                }
            })
            .await;
        if !matches!(result, Ok(Some(_))) {
            eprintln!("warning: failed to abort multipart upload of s3://{bucket}/{key}");
        }
    }
}
