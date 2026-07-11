use anyhow::Result;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Shrink the AIMD window at most once per cooldown so one burst of 503s
/// counts as a single congestion event instead of collapsing the limit to 1.
const SHRINK_COOLDOWN_MS: u64 = 500;

#[derive(Clone)]
pub struct FetchConfig {
    pub start: usize,
    pub cap: usize,
    pub max_retries: u32,
    pub backoff_base_ms: u64,
    pub backoff_cap_ms: u64,
    pub hedge_after: Duration,
    pub hedge_tokens: usize,
    pub max_inflight_bytes: u64,
}

impl Default for FetchConfig {
    fn default() -> FetchConfig {
        FetchConfig {
            start: 64,
            cap: 750,
            max_retries: 5,
            backoff_base_ms: 50,
            backoff_cap_ms: 20_000,
            hedge_after: Duration::from_secs(2),
            hedge_tokens: 500,
            max_inflight_bytes: 512 * 1024 * 1024,
        }
    }
}

pub(crate) struct AimdLimiter {
    semaphore: Arc<Semaphore>,
    limit: AtomicUsize,
    cap: usize,
    pending_shrink: AtomicUsize,
    started: Instant,
    next_shrink_allowed_ms: AtomicU64,
}

impl AimdLimiter {
    pub(crate) fn new(start: usize, cap: usize) -> AimdLimiter {
        AimdLimiter {
            semaphore: Arc::new(Semaphore::new(start)),
            limit: AtomicUsize::new(start),
            cap,
            pending_shrink: AtomicUsize::new(0),
            started: Instant::now(),
            next_shrink_allowed_ms: AtomicU64::new(0),
        }
    }

    pub(crate) async fn acquire(&self) -> Result<OwnedSemaphorePermit> {
        loop {
            let permit = Arc::clone(&self.semaphore).acquire_owned().await?;
            if self.consume_pending_shrink() {
                permit.forget();
                continue;
            }
            return Ok(permit);
        }
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
        let now_ms = u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX);
        let allowed = self.next_shrink_allowed_ms.load(Ordering::Relaxed);
        if now_ms < allowed
            || self
                .next_shrink_allowed_ms
                .compare_exchange(
                    allowed,
                    now_ms.saturating_add(SHRINK_COOLDOWN_MS),
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
                .is_err()
        {
            return;
        }
        let mut current = self.limit.load(Ordering::Relaxed);
        while current > 1 {
            let next = current / 2;
            match self
                .limit
                .compare_exchange(current, next, Ordering::AcqRel, Ordering::Relaxed)
            {
                Ok(_) => {
                    let diff = current - next;
                    let removed = self.forget_available(diff);
                    if removed < diff {
                        self.pending_shrink
                            .fetch_add(diff - removed, Ordering::AcqRel);
                    }
                    return;
                }
                Err(observed) => current = observed,
            }
        }
    }

    fn consume_pending_shrink(&self) -> bool {
        let mut current = self.pending_shrink.load(Ordering::Relaxed);
        while current > 0 {
            match self.pending_shrink.compare_exchange(
                current,
                current - 1,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(observed) => current = observed,
            }
        }
        false
    }

    fn forget_available(&self, target: usize) -> usize {
        let mut forgotten = 0;
        while forgotten < target {
            match self.semaphore.try_acquire() {
                Ok(permit) => {
                    permit.forget();
                    forgotten += 1;
                }
                Err(_) => return forgotten,
            }
        }
        forgotten
    }
}

/// Bounds how many hedged (speculative duplicate) requests run at once.
/// Tokens return when the hedge completes, win or lose.
pub(crate) struct HedgeBudget {
    tokens: AtomicUsize,
    cap: usize,
}

pub(crate) struct HedgeToken<'a> {
    budget: &'a HedgeBudget,
}

impl Drop for HedgeToken<'_> {
    fn drop(&mut self) {
        self.budget.refund();
    }
}

impl HedgeBudget {
    pub(crate) fn new(tokens: usize) -> HedgeBudget {
        HedgeBudget {
            tokens: AtomicUsize::new(tokens),
            cap: tokens,
        }
    }

    pub(crate) fn try_take(&self) -> Option<HedgeToken<'_>> {
        let mut current = self.tokens.load(Ordering::Relaxed);
        while current > 0 {
            match self.tokens.compare_exchange(
                current,
                current - 1,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Some(HedgeToken { budget: self }),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hedge_token_drop_refunds_capacity() {
        let budget = HedgeBudget::new(3);
        let token = budget.try_take().unwrap();

        assert_eq!(budget.tokens.load(Ordering::Relaxed), 2);

        drop(token);

        assert_eq!(budget.tokens.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn hedge_token_drop_restores_exhausted_capacity() {
        let budget = HedgeBudget::new(2);
        let first = budget.try_take().unwrap();
        let second = budget.try_take().unwrap();

        assert!(budget.try_take().is_none());
        assert_eq!(budget.tokens.load(Ordering::Relaxed), 0);

        drop(first);
        drop(second);

        assert_eq!(budget.tokens.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn throttle_with_checked_out_permits_applies_pending_shrink_on_acquire() {
        let limiter = AimdLimiter::new(4, 4);
        let mut permits = Vec::new();
        for _ in 0..4 {
            permits.push(limiter.acquire().await.unwrap());
        }
        assert_eq!(limiter.semaphore.available_permits(), 0);

        limiter.on_throttle();

        assert_eq!(limiter.limit.load(Ordering::Relaxed), 2);
        assert_eq!(limiter.pending_shrink.load(Ordering::Relaxed), 2);

        drop(permits);
        let permit = limiter.acquire().await.unwrap();

        assert_eq!(limiter.pending_shrink.load(Ordering::Relaxed), 0);

        drop(permit);

        assert_eq!(limiter.semaphore.available_permits(), 2);
    }

    #[tokio::test]
    async fn throttle_burst_shrinks_once_within_cooldown() {
        let limiter = AimdLimiter::new(8, 8);

        limiter.on_throttle();
        limiter.on_throttle();
        limiter.on_throttle();

        assert_eq!(limiter.limit.load(Ordering::Relaxed), 4);
    }
}
