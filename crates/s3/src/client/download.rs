use super::{
    classify_sdk_error, read_body, Outcome, PreconditionFailed, S3Client, BYTE_PERMIT_SIZE,
    SMALL_READ_HEDGE, SMALL_READ_MAX,
};
use anyhow::{Context, Result};
use bytes::Bytes;
use futures::stream::{self, StreamExt};
use holys3_core::{DocId, DocumentBody, DocumentSpool, StaleSource};
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::Arc;
use tokio::sync::Notify;

const RANGE_COALESCE_GAP: u64 = 512 * 1024;
const SOURCE_RANGE_MIN: u64 = 64 * 1024 * 1024;
const SOURCE_RANGE_SIZE: u64 = 8 * 1024 * 1024;
const SOURCE_RANGE_CONCURRENCY: usize = 4;

pub(super) type Placement = (usize, usize);

pub(super) struct CoalescedRanges {
    pub(super) ranges: Vec<(u64, u64)>,
    pub(super) placements: Vec<Placement>,
}

pub(super) fn coalesce_ranges(ranges: &[(u64, u64)], max_gap: u64) -> Result<CoalescedRanges> {
    let mut merged: Vec<(u64, u64)> = Vec::new();
    let mut placements = Vec::with_capacity(ranges.len());
    for &(offset, len) in ranges {
        anyhow::ensure!(len > 0, "invalid empty S3 range");
        let end = offset
            .checked_add(len)
            .context("S3 range end overflows u64")?;
        let next = merged.len();
        match merged.last_mut() {
            Some((merged_offset, merged_len))
                if offset
                    <= merged_offset
                        .checked_add(*merged_len)
                        .context("merged S3 range end overflows u64")?
                        .saturating_add(max_gap) =>
            {
                placements.push((next - 1, usize::try_from(offset - *merged_offset)?));
                let merged_end = merged_offset
                    .checked_add(*merged_len)
                    .context("merged S3 range end overflows u64")?;
                *merged_len = end.max(merged_end) - *merged_offset;
            }
            _ => {
                placements.push((next, 0));
                merged.push((offset, len));
            }
        }
    }
    Ok(CoalescedRanges {
        ranges: merged,
        placements,
    })
}

fn byte_range(start: u64, len: u64) -> Result<(u64, u64)> {
    anyhow::ensure!(len > 0, "range length must be greater than 0");
    Ok((
        start,
        start
            .checked_add(len - 1)
            .context("range end overflows u64")?,
    ))
}

fn check_range_version(
    expected: &mut Option<String>,
    fetched: Option<String>,
    bucket: &str,
    key: &str,
) -> Result<()> {
    let fetched = fetched.with_context(|| format!("range GET s3://{bucket}/{key}: no ETag"))?;
    match expected {
        Some(expected) if expected != &fetched => {
            return Err(anyhow::Error::new(StaleSource {
                key: key.to_owned(),
                expected: expected.clone(),
            }));
        }
        Some(_) => {}
        None => *expected = Some(fetched),
    }
    Ok(())
}

impl S3Client {
    async fn send_get(
        &self,
        bucket: &str,
        key: &str,
        range: Option<(u64, u64)>,
        etag: Option<&str>,
    ) -> Outcome<(DocumentBody, Option<String>)> {
        let mut request = self.0.sdk.get_object().bucket(bucket).key(key);
        if let Some((start, end)) = range {
            request = request.range(format!("bytes={start}-{end}"));
        }
        if let Some(etag) = etag {
            request = request.if_match(etag);
        }
        match request.send().await {
            Ok(output) => {
                if let Some((start, end)) = range {
                    let expected = format!("bytes {start}-{end}/");
                    match output.content_range() {
                        Some(content_range) if content_range.starts_with(&expected) => {}
                        Some(content_range) => {
                            return Outcome::Fatal(anyhow::anyhow!(
                                "range GET s3://{bucket}/{key} returned wrong Content-Range {content_range}, expected {expected}..."
                            ));
                        }
                        None => {
                            return Outcome::Fatal(anyhow::anyhow!(
                                "range GET s3://{bucket}/{key} did not return Content-Range"
                            ));
                        }
                    }
                }
                let version = output.e_tag().map(str::to_owned);
                let hint = output
                    .content_length()
                    .and_then(|length| u64::try_from(length).ok())
                    .unwrap_or(0);
                match read_body(output.body, hint).await {
                    Ok(body) => Outcome::Success((body, version)),
                    Err(error) => Outcome::Transient(error),
                }
            }
            Err(error) => match classify_sdk_error(error) {
                Outcome::Fatal(error)
                    if etag.is_some() && error.root_cause().is::<PreconditionFailed>() =>
                {
                    Outcome::Fatal(anyhow::Error::new(StaleSource {
                        key: key.to_owned(),
                        expected: etag.unwrap_or_default().to_owned(),
                    }))
                }
                outcome => outcome,
            },
        }
    }

    async fn fetch_hedged_version(
        &self,
        bucket: &str,
        key: &str,
        range: Option<(u64, u64)>,
        etag: Option<&str>,
    ) -> Result<Option<(DocumentBody, Option<String>)>> {
        let hedge_after = match range {
            Some((start, end)) if end.saturating_sub(start) < SMALL_READ_MAX => {
                SMALL_READ_HEDGE.min(self.0.cfg.hedge_after)
            }
            _ => self.0.cfg.hedge_after,
        };
        let label = format!("GET s3://{bucket}/{key}");
        let started = Notify::new();
        let primary = self.run_resilient(&label, Some(&started), || {
            self.send_get(bucket, key, range, etag)
        });
        tokio::pin!(primary);
        let result = tokio::select! {
            biased;
            result = &mut primary => result,
            () = async { started.notified().await; tokio::time::sleep(hedge_after).await } => {
                match self.0.hedges.try_take() {
                    None => primary.await,
                    Some(token) => {
                        let hedge = async {
                            let _token = token;
                            self.run_resilient(&label, None, || {
                                self.send_get(bucket, key, range, etag)
                            })
                            .await
                        };
                        tokio::pin!(hedge);
                        tokio::select! {
                            biased;
                            result = &mut primary => result,
                            result = &mut hedge => result,
                        }
                    }
                }
            }
        };
        let Some((body, version)) = result? else {
            return Ok(None);
        };
        if let Some((start, end)) = range {
            let expected = end - start + 1;
            anyhow::ensure!(
                body.len() == expected,
                "range GET s3://{bucket}/{key} returned {} bytes, expected {expected}",
                body.len()
            );
        }
        Ok(Some((body, version)))
    }

    pub(super) async fn fetch_hedged(
        &self,
        bucket: &str,
        key: &str,
        range: Option<(u64, u64)>,
        etag: Option<&str>,
    ) -> Result<Option<DocumentBody>> {
        Ok(self
            .fetch_hedged_version(bucket, key, range, etag)
            .await?
            .map(|(body, _)| body))
    }

    async fn fetch_source_parts(
        &self,
        bucket: &str,
        key: &str,
        etag: Option<&str>,
        size: u64,
        part_size: u64,
        consume: &mut (dyn FnMut(u64, DocumentBody) -> Result<()> + Send),
    ) -> Result<bool> {
        anyhow::ensure!(size > 0, "source range size must be greater than 0");
        anyhow::ensure!(
            part_size > 0,
            "source range part size must be greater than 0"
        );
        let parts = size.div_ceil(part_size);
        let mut version = None;
        let mut fetches = stream::iter((0..parts).map(|part| async move {
            let start = part
                .checked_mul(part_size)
                .context("source range start overflows u64")?;
            let len = part_size.min(
                size.checked_sub(start)
                    .context("source range starts after object end")?,
            );
            let range = byte_range(start, len)?;
            let response = self
                .fetch_hedged_version(bucket, key, Some(range), etag)
                .await?;
            Ok::<_, anyhow::Error>((start, response))
        }))
        .buffer_unordered(SOURCE_RANGE_CONCURRENCY.min(self.0.cfg.cap));
        while let Some(result) = fetches.next().await {
            let (start, response) = result?;
            let Some((part, fetched_version)) = response else {
                return Ok(false);
            };
            if parts > 1 && etag.is_none() {
                check_range_version(&mut version, fetched_version, bucket, key)?;
            }
            consume(start, part)?;
        }
        Ok(true)
    }

    pub(super) async fn fetch_source_ranges(
        &self,
        bucket: &str,
        key: &str,
        etag: Option<&str>,
        size: u64,
        part_size: u64,
    ) -> Result<Option<DocumentBody>> {
        let mut body = DocumentSpool::new(size)?;
        let found = self
            .fetch_source_parts(bucket, key, etag, size, part_size, &mut |start, part| {
                let part_len = part.len();
                let mut reader = part.into_reader();
                let mut copied = 0u64;
                let mut chunk = [0u8; 64 * 1024];
                loop {
                    let read = reader.read(&mut chunk)?;
                    if read == 0 {
                        break;
                    }
                    body.write_at(
                        start
                            .checked_add(copied)
                            .context("source range write offset overflows u64")?,
                        &chunk[..read],
                    )?;
                    copied = copied
                        .checked_add(u64::try_from(read)?)
                        .context("source range length overflows u64")?;
                }
                anyhow::ensure!(copied == part_len, "source range body length changed");
                Ok(())
            })
            .await?;
        if found {
            Ok(Some(body.finish()?))
        } else {
            Ok(None)
        }
    }

    async fn fetch_source(
        &self,
        bucket: &str,
        key: &str,
        etag: Option<&str>,
        size: u64,
    ) -> Result<Option<DocumentBody>> {
        match etag {
            Some(etag) if size >= SOURCE_RANGE_MIN => {
                self.fetch_source_ranges(bucket, key, Some(etag), size, SOURCE_RANGE_SIZE)
                    .await
            }
            _ => self.fetch_hedged(bucket, key, None, etag).await,
        }
    }

    pub fn get(&self, bucket: &str, key: &str) -> Result<Option<Vec<u8>>> {
        self.0
            .rt
            .block_on(self.fetch_hedged(bucket, key, None, None))?
            .map(|body| body.into_bytes().map(|bytes| bytes.to_vec()))
            .transpose()
    }

    pub(crate) fn get_file(
        &self,
        bucket: &str,
        key: &str,
        output: &mut std::fs::File,
        size: u64,
    ) -> Result<bool> {
        if size == 0 {
            let body = self
                .0
                .rt
                .block_on(self.fetch_hedged(bucket, key, None, None))?;
            let Some(body) = body else {
                return Ok(false);
            };
            anyhow::ensure!(body.is_empty(), "S3 object length changed while copying");
            output.set_len(0)?;
        } else {
            output.set_len(size)?;
            let found = self.0.rt.block_on(self.fetch_source_parts(
                bucket,
                key,
                None,
                size,
                SOURCE_RANGE_SIZE,
                &mut |start, part| {
                    let expected = part.len();
                    output.seek(SeekFrom::Start(start))?;
                    let copied = std::io::copy(&mut part.into_reader(), output)?;
                    anyhow::ensure!(copied == expected, "S3 range length changed while copying");
                    Ok(())
                },
            ))?;
            if !found {
                return Ok(false);
            }
        }
        output.seek(SeekFrom::Start(0))?;
        output.flush()?;
        Ok(true)
    }

    pub fn get_if_match(&self, bucket: &str, key: &str, etag: &str) -> Result<Option<Bytes>> {
        self.0
            .rt
            .block_on(self.fetch_hedged(bucket, key, None, Some(etag)))
            .and_then(|body| body.map(DocumentBody::into_bytes).transpose())
    }

    pub fn get_range(
        &self,
        bucket: &str,
        key: &str,
        start: u64,
        len: u64,
    ) -> Result<Option<Vec<u8>>> {
        let range = byte_range(start, len)?;
        self.0
            .rt
            .block_on(self.fetch_hedged(bucket, key, Some(range), None))?
            .map(|body| body.into_bytes().map(|bytes| bytes.to_vec()))
            .transpose()
    }

    pub fn get_ranges(
        &self,
        bucket: &str,
        key: &str,
        ranges: &[(u64, u64)],
    ) -> Result<Option<Vec<Bytes>>> {
        let mut order: Vec<usize> = (0..ranges.len()).collect();
        order.sort_by_key(|&index| ranges[index]);
        let sorted: Vec<(u64, u64)> = order.iter().map(|&index| ranges[index]).collect();
        let coalesced = coalesce_ranges(&sorted, RANGE_COALESCE_GAP)?;
        let merged = &coalesced.ranges;
        let blobs = self.0.rt.block_on(async {
            let mut blobs: Vec<Option<Bytes>> = vec![None; merged.len()];
            let mut version = None;
            let mut fetches = stream::iter(merged.iter().enumerate().map(
                |(index, &(start, len))| async move {
                    let range = byte_range(start, len)?;
                    let response = self
                        .fetch_hedged_version(bucket, key, Some(range), None)
                        .await?;
                    let (body, version) = match response {
                        Some((body, version)) => (Some(body.into_bytes()?), version),
                        None => (None, None),
                    };
                    Ok::<_, anyhow::Error>((index, body, version))
                },
            ))
            .buffer_unordered(self.0.cfg.cap);
            while let Some(result) = fetches.next().await {
                let (index, bytes, fetched_version) = result?;
                match bytes {
                    Some(bytes) => blobs[index] = Some(bytes),
                    None => return Ok(None),
                }
                if merged.len() > 1 {
                    check_range_version(&mut version, fetched_version, bucket, key)?;
                }
            }
            drop(fetches);
            Ok::<_, anyhow::Error>(Some(blobs))
        })?;
        let Some(blobs) = blobs else {
            return Ok(None);
        };
        let mut output = vec![Bytes::new(); ranges.len()];
        for (index, &original) in order.iter().enumerate() {
            let (blob_index, start) = coalesced.placements[index];
            let len = usize::try_from(sorted[index].1)?;
            let blob = blobs[blob_index].as_ref().expect("all ranges fetched");
            let end = start
                .checked_add(len)
                .context("coalesced S3 range overflows usize")?;
            let slice = blob.get(start..end).with_context(|| {
                format!(
                    "coalesced read of {key} is {} bytes, range needs {}",
                    blob.len(),
                    end
                )
            })?;
            output[original] = blob.slice_ref(slice);
        }
        Ok(Some(output))
    }

    pub fn get_each(
        &self,
        bucket: &str,
        keys: Vec<(DocId, String, u64)>,
        consume: &mut dyn FnMut(DocId, Option<Bytes>) -> Result<()>,
    ) -> Result<()> {
        let requests = keys
            .into_iter()
            .map(|(id, key, encoded_size)| (id, key, None, encoded_size))
            .collect();
        let mut consume_body = |id, body: Option<DocumentBody>| {
            consume(id, body.map(DocumentBody::into_bytes).transpose()?)
        };
        self.get_each_requests(bucket, requests, &mut consume_body)
    }

    pub fn get_each_if_match(
        &self,
        bucket: &str,
        keys: Vec<(DocId, String, String, u64)>,
        consume: &mut dyn FnMut(DocId, Option<Bytes>) -> Result<()>,
    ) -> Result<()> {
        let mut consume_body = |id, body: Option<DocumentBody>| {
            consume(id, body.map(DocumentBody::into_bytes).transpose()?)
        };
        self.get_each_requests(
            bucket,
            keys.into_iter()
                .map(|(id, key, etag, encoded_size)| (id, key, Some(etag), encoded_size))
                .collect(),
            &mut consume_body,
        )
    }

    pub(crate) fn get_each_bodies_if_match(
        &self,
        bucket: &str,
        keys: Vec<(DocId, String, String, u64)>,
        consume: &mut dyn FnMut(DocId, Option<DocumentBody>) -> Result<()>,
    ) -> Result<()> {
        self.get_each_requests(
            bucket,
            keys.into_iter()
                .map(|(id, key, etag, encoded_size)| (id, key, Some(etag), encoded_size))
                .collect(),
            consume,
        )
    }

    fn get_each_requests(
        &self,
        bucket: &str,
        requests: Vec<(DocId, String, Option<String>, u64)>,
        consume: &mut dyn FnMut(DocId, Option<DocumentBody>) -> Result<()>,
    ) -> Result<()> {
        let byte_permits = u32::try_from(self.0.cfg.max_inflight_bytes.div_ceil(BYTE_PERMIT_SIZE))?;
        let bytes_limit = Arc::new(tokio::sync::Semaphore::new(byte_permits as usize));
        let (tx, mut rx) = tokio::sync::mpsc::channel::<(
            DocId,
            Option<DocumentBody>,
            tokio::sync::OwnedSemaphorePermit,
        )>(64);
        let cap = self.0.cfg.cap;
        let bucket_shared: Arc<str> = Arc::from(bucket);
        let client = self.clone();
        let driver = self.0.rt.spawn(async move {
            let mut fetches = stream::iter(requests.into_iter().map(|(id, key, etag, size)| {
                let client = client.clone();
                let bucket = Arc::clone(&bucket_shared);
                let bytes_limit = Arc::clone(&bytes_limit);
                async move {
                    let needed = u32::try_from(size.div_ceil(BYTE_PERMIT_SIZE))?
                        .max(1)
                        .min(byte_permits);
                    let permit = bytes_limit.acquire_many_owned(needed).await?;
                    let result = client
                        .fetch_source(&bucket, &key, etag.as_deref(), size)
                        .await;
                    Ok::<_, anyhow::Error>((id, result, permit))
                }
            }))
            .buffer_unordered(cap);
            loop {
                tokio::select! {
                    biased;
                    () = tx.closed() => break,
                    next = fetches.next() => match next {
                        Some(result) => {
                            let (id, result, permit) = result?;
                            if tx.send((id, result?, permit)).await.is_err() {
                                break;
                            }
                        }
                        None => break,
                    },
                }
            }
            Ok::<_, anyhow::Error>(())
        });
        let mut consume_result = Ok(());
        while let Some((id, bytes, permit)) = rx.blocking_recv() {
            if let Err(err) = consume(id, bytes) {
                consume_result = Err(err);
                break;
            }
            drop(permit);
        }
        drop(rx);
        let driver_result = self
            .0
            .rt
            .block_on(driver)
            .map_err(|err| anyhow::anyhow!("fetch driver panicked: {err}"))?;
        consume_result?;
        driver_result
    }

    pub fn get_with_version(&self, bucket: &str, key: &str) -> Result<Option<(Vec<u8>, String)>> {
        match self
            .0
            .rt
            .block_on(self.fetch_hedged_version(bucket, key, None, None))?
        {
            None => Ok(None),
            Some((bytes, etag)) => {
                let etag = etag.with_context(|| format!("GET s3://{bucket}/{key}: no ETag"))?;
                Ok(Some((bytes.into_bytes()?.to_vec(), etag)))
            }
        }
    }
}
