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
use seagrep_core::{ProgressEvent, ProgressSender};
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

const STREAM_PART_SIZE: usize = 16 * 1024 * 1024;
/// Matches `MULTIPART_CONCURRENCY`: the producer blocks when the window is
/// full, so a narrower window would throttle the merge to per-connection
/// upload speed instead of the link's aggregate.
const STREAM_PARTS_IN_FLIGHT: usize = 4;

/// A multipart upload fed incrementally: 16 MiB parts dispatch onto the
/// client runtime as they fill, with a bounded in-flight window so the
/// producer overlaps compute with upload without unbounded memory. Exactly
/// one of `finish`/`abort` ends it; dropping an unfinished upload aborts it
/// so no orphaned parts accrue storage.
pub(crate) struct StreamingUpload {
    client: S3Client,
    bucket: String,
    key: String,
    upload_id: String,
    buffer: bytes::BytesMut,
    next_part: i32,
    in_flight: std::collections::VecDeque<tokio::task::JoinHandle<Result<CompletedPart>>>,
    completed: Vec<CompletedPart>,
    progress: Option<ProgressSender>,
    done: bool,
}

impl S3Client {
    pub(crate) fn start_streaming_upload(
        &self,
        bucket: &str,
        key: &str,
        progress: Option<ProgressSender>,
    ) -> Result<StreamingUpload> {
        let upload_id = self.0.rt.block_on(self.start_multipart(bucket, key))?;
        Ok(StreamingUpload {
            client: self.clone(),
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            upload_id,
            buffer: bytes::BytesMut::new(),
            next_part: 1,
            in_flight: std::collections::VecDeque::new(),
            completed: Vec::new(),
            progress,
            done: false,
        })
    }
}

impl StreamingUpload {
    pub(crate) fn write(&mut self, bytes: &[u8]) -> Result<()> {
        self.buffer.extend_from_slice(bytes);
        while self.buffer.len() >= STREAM_PART_SIZE {
            let part = self.buffer.split_to(STREAM_PART_SIZE).freeze();
            self.dispatch(part)?;
        }
        Ok(())
    }

    fn dispatch(&mut self, part: Bytes) -> Result<()> {
        while self.in_flight.len() >= STREAM_PARTS_IN_FLIGHT {
            self.harvest_one()?;
        }
        let number = self.next_part;
        anyhow::ensure!(
            number <= 10_000,
            "streamed blob exceeds the S3 10,000-part limit ({} MiB parts): raise the part size",
            STREAM_PART_SIZE / (1024 * 1024)
        );
        self.next_part = number
            .checked_add(1)
            .context("multipart upload part number overflows")?;
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let key = self.key.clone();
        let upload_id = self.upload_id.clone();
        let progress = self.progress.clone();
        let part_len = part.len() as u64;
        if let Some(progress) = &self.progress {
            progress.emit(ProgressEvent::UploadStarted { bytes: part_len });
        }
        let handle = self.client.0.rt.spawn(async move {
            let completed = client
                .upload_part(&bucket, &key, &upload_id, number, PartSource::Bytes(part))
                .await?;
            if let Some(progress) = &progress {
                progress.emit(ProgressEvent::UploadedChunk { bytes: part_len });
            }
            Ok(completed)
        });
        self.in_flight.push_back(handle);
        Ok(())
    }

    fn harvest_one(&mut self) -> Result<()> {
        if let Some(handle) = self.in_flight.pop_front() {
            self.completed.push(self.client.0.rt.block_on(handle)??);
        }
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<()> {
        if self.completed.is_empty() && self.in_flight.is_empty() {
            // Zero dispatched parts: S3 refuses to complete a partless
            // multipart, and a plain put is cheaper for small blobs anyway.
            // An empty postings.bin (a segment whose documents produced no
            // grams) is valid and must round-trip.
            let body = self.buffer.split_off(0).freeze();
            let (client, bucket, key) =
                (self.client.clone(), self.bucket.clone(), self.key.clone());
            let progress = self.progress.clone();
            self.abort_inner();
            if let Some(progress) = &progress {
                progress.emit(ProgressEvent::UploadStarted {
                    bytes: body.len() as u64,
                });
            }
            client
                .0
                .rt
                .block_on(client.put_bytes(&bucket, &key, body, progress.as_ref()))?;
            return Ok(());
        }
        if !self.buffer.is_empty() {
            let part = self.buffer.split_off(0).freeze();
            self.dispatch(part)?;
        }
        while !self.in_flight.is_empty() {
            self.harvest_one()?;
        }
        let mut parts = std::mem::take(&mut self.completed);
        parts.sort_unstable_by_key(|part| part.part_number().unwrap_or_default());
        let result = self.client.0.rt.block_on(self.client.finish_multipart(
            &self.bucket,
            &self.key,
            &self.upload_id,
            parts,
        ));
        self.done = result.is_ok();
        result
    }

    pub(crate) fn abort(mut self) {
        self.abort_inner();
    }

    fn abort_inner(&mut self) {
        if self.done {
            return;
        }
        self.done = true;
        // Let in-flight parts settle before aborting: a part that lands
        // after AbortMultipartUpload would linger as billed storage.
        for handle in self.in_flight.drain(..) {
            self.client.0.rt.block_on(handle).ok();
        }
        self.client.0.rt.block_on(self.client.abort_multipart(
            &self.bucket,
            &self.key,
            &self.upload_id,
        ));
    }
}

impl Drop for StreamingUpload {
    fn drop(&mut self) {
        self.abort_inner();
    }
}
