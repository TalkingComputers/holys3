use crate::S3Client;
use anyhow::Result;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[derive(Clone)]
pub struct FetchConfig {
    pub start: usize,
    pub cap: usize,
    pub buffer: usize,
    pub max_retries: u32,
    pub backoff_base_ms: u64,
    pub backoff_cap_ms: u64,
    pub hedge_after: Duration,
    pub retry_tokens: usize,
}

impl Default for FetchConfig {
    fn default() -> FetchConfig {
        FetchConfig {
            start: 64,
            cap: 750,
            buffer: 1000,
            max_retries: 5,
            backoff_base_ms: 50,
            backoff_cap_ms: 20_000,
            hedge_after: Duration::from_secs(2),
            retry_tokens: 500,
        }
    }
}

pub(crate) struct AimdLimiter {
    semaphore: Arc<Semaphore>,
    limit: AtomicUsize,
    cap: usize,
}

impl AimdLimiter {
    pub(crate) fn new(start: usize, cap: usize) -> AimdLimiter {
        AimdLimiter {
            semaphore: Arc::new(Semaphore::new(start)),
            limit: AtomicUsize::new(start),
            cap,
        }
    }

    pub(crate) async fn acquire(&self) -> Result<OwnedSemaphorePermit> {
        Ok(Arc::clone(&self.semaphore).acquire_owned().await?)
    }

    pub(crate) fn on_success(&self) {
        let mut current = self.limit.load(Ordering::Relaxed);
        while current < self.cap {
            match self.limit.compare_exchange(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    self.semaphore.add_permits(1);
                    return;
                }
                Err(next) => current = next,
            }
        }
    }

    pub(crate) fn on_throttle(&self) {
        let mut current = self.limit.load(Ordering::Relaxed);
        while current > 1 {
            let next = current / 2;
            match self
                .limit
                .compare_exchange(current, next, Ordering::AcqRel, Ordering::Relaxed)
            {
                Ok(_) => {
                    let diff = current - next;
                    if let Ok(diff) = u32::try_from(diff) {
                        if let Ok(permits) = self.semaphore.try_acquire_many(diff) {
                            permits.forget();
                        }
                    }
                    return;
                }
                Err(observed) => current = observed,
            }
        }
    }
}

pub(crate) struct RetryBudget {
    tokens: AtomicUsize,
    cap: usize,
}

pub(crate) struct RetryToken<'a> {
    budget: &'a RetryBudget,
}

impl Drop for RetryToken<'_> {
    fn drop(&mut self) {
        self.budget.refund();
    }
}

impl RetryBudget {
    pub(crate) fn new(tokens: usize) -> RetryBudget {
        RetryBudget {
            tokens: AtomicUsize::new(tokens),
            cap: tokens,
        }
    }

    pub(crate) fn try_take(&self) -> Option<RetryToken<'_>> {
        let mut current = self.tokens.load(Ordering::Relaxed);
        while current > 0 {
            match self.tokens.compare_exchange(
                current,
                current - 1,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Some(RetryToken { budget: self }),
                Err(observed) => current = observed,
            }
        }
        None
    }

    fn refund(&self) {
        let mut current = self.tokens.load(Ordering::Relaxed);
        while current < self.cap {
            match self.tokens.compare_exchange(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(observed) => current = observed,
            }
        }
    }
}

enum GetAttempt {
    Bytes(Vec<u8>),
    SlowDown,
}

pub(crate) async fn get_with_retry(
    client: &S3Client,
    bucket: &str,
    key: &str,
    limiter: &AimdLimiter,
    budget: &RetryBudget,
    cfg: &FetchConfig,
) -> Result<Vec<u8>> {
    let mut attempt = 0;
    let mut retry_token = None;
    loop {
        let http = if attempt == 0 {
            &client.http
        } else {
            &client.retry_http
        };
        let result = get_once(client, http, bucket, key, limiter).await;
        drop(retry_token.take());
        match result? {
            GetAttempt::Bytes(bytes) => return Ok(bytes),
            GetAttempt::SlowDown => {
                if attempt >= cfg.max_retries {
                    anyhow::bail!("S3 GET s3://{bucket}/{key} returned HTTP 503");
                }
                retry_token = budget.try_take();
                if retry_token.is_none() {
                    anyhow::bail!("S3 GET s3://{bucket}/{key} exhausted retry budget");
                }
                let exponential = cfg
                    .backoff_base_ms
                    .saturating_mul(2_u64.saturating_pow(attempt));
                let cap = exponential.min(cfg.backoff_cap_ms);
                let delay = rand::random_range(0..=cap);
                tokio::time::sleep(Duration::from_millis(delay)).await;
                attempt += 1;
            }
        }
    }
}

async fn get_once(
    client: &S3Client,
    http: &reqwest::Client,
    bucket: &str,
    key: &str,
    limiter: &AimdLimiter,
) -> Result<GetAttempt> {
    let permit = limiter.acquire().await?;
    let resp = client.send_get(http, bucket, key, None).await?;
    if resp.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE {
        limiter.on_throttle();
        drop(permit);
        return Ok(GetAttempt::SlowDown);
    }
    let resp = resp.error_for_status()?;
    let bytes = resp.bytes().await?.to_vec();
    limiter.on_success();
    drop(permit);
    Ok(GetAttempt::Bytes(bytes))
}

pub(crate) async fn fetch_one_hedged(
    client: &S3Client,
    bucket: &str,
    key: &str,
    limiter: &AimdLimiter,
    budget: &RetryBudget,
    cfg: &FetchConfig,
) -> Result<Vec<u8>> {
    let primary = get_with_retry(client, bucket, key, limiter, budget, cfg);
    tokio::pin!(primary);
    tokio::select! {
        biased;
        result = &mut primary => result,
        () = tokio::time::sleep(cfg.hedge_after) => {
            let Some(hedge_token) = budget.try_take() else {
                return primary.await;
            };
            let hedge = async {
                let hedge_token_guard = hedge_token;
                let result = get_with_retry(client, bucket, key, limiter, budget, cfg).await;
                drop(hedge_token_guard);
                result
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_token_drop_refunds_capacity() {
        let budget = RetryBudget::new(3);
        let token = budget.try_take().unwrap();

        assert_eq!(budget.tokens.load(Ordering::Relaxed), 2);

        drop(token);

        assert_eq!(budget.tokens.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn retry_token_drop_restores_exhausted_capacity() {
        let budget = RetryBudget::new(2);
        let first = budget.try_take().unwrap();
        let second = budget.try_take().unwrap();

        assert!(budget.try_take().is_none());
        assert_eq!(budget.tokens.load(Ordering::Relaxed), 0);

        drop(first);
        drop(second);

        assert_eq!(budget.tokens.load(Ordering::Relaxed), 2);
    }
}
