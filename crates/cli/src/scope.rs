//! Search scoping: prune candidate objects by key and by timestamps
//! embedded in keys, before anything is fetched.

use anyhow::{Context, Result};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::LazyLock;
use time::{Date, Duration, Month, OffsetDateTime, PrimitiveDateTime, Time};

pub(crate) struct Scope {
    key_prefix: Option<String>,
    key_regex: Option<regex::Regex>,
    globs: Option<crate::globs::GlobFilter>,
    since: Option<OffsetDateTime>,
    until: Option<OffsetDateTime>,
    undated: AtomicUsize,
}

impl Scope {
    pub(crate) fn from_args(
        key_prefix: Option<String>,
        key_regex: Option<String>,
        since: Option<String>,
        until: Option<String>,
        globs: Option<crate::globs::GlobFilter>,
    ) -> Result<Option<Scope>> {
        if key_prefix.is_none()
            && key_regex.is_none()
            && since.is_none()
            && until.is_none()
            && globs.is_none()
        {
            return Ok(None);
        }
        let now = OffsetDateTime::now_utc();
        let since = since
            .map(|s| parse_instant(&s, now, Bound::Start).context("invalid --since"))
            .transpose()?;
        let until = until
            .map(|s| parse_instant(&s, now, Bound::End).context("invalid --until"))
            .transpose()?;
        anyhow::ensure!(
            !matches!((since, until), (Some(start), Some(end)) if start >= end),
            "--since must be earlier than --until"
        );
        Ok(Some(Scope {
            key_prefix,
            key_regex: key_regex
                .map(|pattern| regex::Regex::new(&pattern).context("invalid --key-regex"))
                .transpose()?,
            globs,
            since,
            until,
            undated: AtomicUsize::new(0),
        }))
    }

    /// Object-level filter: a key passes when it satisfies regex/globs and
    /// its embedded time RANGE overlaps [since, until]. The key prefix is
    /// NOT checked here — `KeyScope::admits` owns it, via `key_prefix()`.
    /// Keys with no recognizable timestamp are kept (and counted) — object
    /// filtering must never silently hide data.
    pub(crate) fn matches(&self, key: &str) -> bool {
        if let Some(re) = &self.key_regex {
            if !re.is_match(key) {
                return false;
            }
        }
        if let Some(globs) = &self.globs {
            if !globs.admits(key) {
                return false;
            }
        }
        if self.since.is_none() && self.until.is_none() {
            return true;
        }
        let Some((start, end)) = extract_key_time(key) else {
            self.undated.fetch_add(1, Ordering::Relaxed);
            return true;
        };
        if let Some(since) = self.since {
            if end <= since {
                return false;
            }
        }
        if let Some(until) = self.until {
            if start >= until {
                return false;
            }
        }
        true
    }

    pub(crate) fn key_prefix(&self) -> Option<&str> {
        self.key_prefix.as_deref()
    }

    /// One stderr note after filtering, so a time window over undated keys
    /// is never silently meaningless.
    pub(crate) fn report(&self) {
        let undated = self.undated.load(Ordering::Relaxed);
        if undated > 0 {
            eprintln!(
                "note: {undated} candidate keys had no recognizable timestamp and were included"
            );
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Bound {
    Start,
    End,
}

/// `--since`/`--until` accept absolute `YYYY-MM-DD[THH:MM[:SS]][Z]` or
/// relative `30m`/`6h`/`2d`/`1w` (ago, UTC). A bare date used as an `--until`
/// bound means the END of that day — `--until 2026-06-09` includes June 9.
fn parse_instant(input: &str, now: OffsetDateTime, bound: Bound) -> Result<OffsetDateTime> {
    let input = input.trim();
    if let Some((value, unit)) = input
        .char_indices()
        .last()
        .map(|(i, c)| (&input[..i], c))
        .filter(|(value, _)| !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit()))
    {
        let value: i64 = value.parse()?;
        let multiplier = match unit {
            's' => 1,
            'm' => 60,
            'h' => 60 * 60,
            'd' => 24 * 60 * 60,
            'w' => 7 * 24 * 60 * 60,
            other => anyhow::bail!("unknown relative-time unit `{other}` (use s/m/h/d/w)"),
        };
        let seconds = value
            .checked_mul(multiplier)
            .context("relative time out of supported range")?;
        return now
            .checked_sub(Duration::seconds(seconds))
            .context("relative time out of supported range");
    }
    let input = input.strip_suffix('Z').unwrap_or(input);
    let (date_part, time_part) = match input.split_once('T') {
        Some((date, time)) => (date, Some(time)),
        None => (input, None),
    };
    let mut date_fields = date_part.split('-');
    let date = build_date(
        date_fields.next().context("missing year")?.parse()?,
        date_fields.next().context("missing month")?.parse()?,
        date_fields.next().context("missing day")?.parse()?,
    )?;
    anyhow::ensure!(date_fields.next().is_none(), "malformed date `{date_part}`");
    let (time, grain) = match time_part {
        None => (Time::MIDNIGHT, Duration::days(1)),
        Some(time_part) => {
            let mut fields = time_part.split(':');
            let hour = fields.next().context("missing hour")?.parse()?;
            let minute = fields.next().context("missing minute")?.parse()?;
            let (second, grain) = match fields.next() {
                Some(second) => (second.parse()?, Duration::seconds(1)),
                None => (0, Duration::minutes(1)),
            };
            anyhow::ensure!(fields.next().is_none(), "malformed time `{time_part}`");
            (Time::from_hms(hour, minute, second)?, grain)
        }
    };
    let instant = PrimitiveDateTime::new(date, time).assume_utc();
    // An End bound is inclusive at the granularity it was given: `--until
    // 2026-06-09` covers all of June 9, `--until 14:30` covers the 14:30
    // minute. Internally it becomes an exclusive end instant.
    Ok(match bound {
        Bound::Start => instant,
        Bound::End => instant
            .checked_add(grain)
            .context("instant out of supported range")?,
    })
}

fn build_date(year: i32, month: u8, day: u8) -> Result<Date> {
    Ok(Date::from_calendar_date(
        year,
        Month::try_from(month)?,
        day,
    )?)
}

#[derive(Clone, Copy)]
enum Granularity {
    Minute,
    Day,
    FromCaptures,
}

/// Years outside this window are treated as not-a-timestamp (version strings,
/// request ids, ports), which keeps the key undated-and-included rather than
/// silently excluded by a bogus far-past/far-future date.
const YEAR_RANGE: std::ops::RangeInclusive<i64> = 1990..=2099;

/// Log producers deliver up to this long after the events they contain
/// (`CloudTrail` ~15 min, ALB 5 min); every extracted range is widened
/// backwards by it so `--until` edges never lose the delivery-lagged object.
const DELIVERY_LAG: Duration = Duration::minutes(15);

/// Timestamp shapes that real S3 log deliveries embed in keys. Digit guards
/// keep stamps inside longer digit runs from parsing as dates. Covers:
/// ALB/`CloudTrail`/VPC filename stamps (`20260609T2300Z`), `CloudFront`
/// legacy / S3 access-log dashed stamps (`2026-06-09-23[-59-59]`), hive
/// partitions (`year=2026/month=06/day=09[/hour=23]`), `dt=`/`date=`
/// partitions, and plain `2026/06/09` path segments (whose trailing segment
/// may be a shard id, not an hour — so they always count as a full day).
static KEY_PATTERNS: &[(&str, Granularity)] = &[
    (
        r"(?:^|[^0-9])(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})Z",
        Granularity::Minute,
    ),
    (
        r"(?:^|[^0-9])(\d{4})-(\d{2})-(\d{2})-(\d{2})(?:-(\d{2})-(\d{2}))?(?:[^0-9]|$)",
        Granularity::FromCaptures,
    ),
    (
        r"year=(\d{4})/month=(\d{1,2})/day=(\d{1,2})(?:/hour=(\d{1,2}))?",
        Granularity::FromCaptures,
    ),
    (r"(?:dt|date)=(\d{4})-(\d{2})-(\d{2})", Granularity::Day),
    (
        r"(?:^|/)(\d{4})/(\d{1,2})/(\d{1,2})(?:/|$)",
        Granularity::Day,
    ),
];

static COMPILED_PATTERNS: LazyLock<Vec<regex::Regex>> = LazyLock::new(|| {
    KEY_PATTERNS
        .iter()
        .map(|(pattern, _)| regex::Regex::new(pattern).expect("key pattern regexes are static"))
        .collect()
});

type TimeRange = (OffsetDateTime, OffsetDateTime);

fn hull(ranges: &[TimeRange]) -> Option<TimeRange> {
    let lo = ranges.iter().map(|&(lo, _)| lo).min()?;
    let hi = ranges.iter().map(|&(_, hi)| hi).max()?;
    Some((lo, hi))
}

/// The time range a key covers, derived from EVERY plausible timestamp in it.
/// Sub-day stamps (delivery filenames) win when they agree with the coarse
/// path date — that keeps minute precision for ALB/`CloudTrail` keys. When
/// stamps genuinely conflict (a stale date elsewhere in the key), the ranges
/// are unioned, so a wrong stamp can only widen the search, never lose
/// matches. The result is widened backwards by the delivery lag.
fn extract_key_time(key: &str) -> Option<TimeRange> {
    let mut day_ranges: Vec<TimeRange> = Vec::new();
    let mut fine_ranges: Vec<TimeRange> = Vec::new();
    for (regex, (_, granularity)) in COMPILED_PATTERNS.iter().zip(KEY_PATTERNS) {
        for captures in regex.captures_iter(key) {
            let field =
                |i: usize| -> Option<i64> { captures.get(i).and_then(|m| m.as_str().parse().ok()) };
            let (Some(year), Some(month), Some(day)) = (field(1), field(2), field(3)) else {
                continue;
            };
            if !YEAR_RANGE.contains(&year) {
                continue;
            }
            let Ok(date) = build_date(year as i32, month as u8, day as u8) else {
                continue;
            };
            let hour = field(4);
            let minute = field(5);
            let second = field(6);
            let Ok(time) = Time::from_hms(
                hour.unwrap_or(0) as u8,
                minute.unwrap_or(0) as u8,
                second.unwrap_or(0) as u8,
            ) else {
                continue;
            };
            let span = match granularity {
                Granularity::Minute => Duration::minutes(1),
                Granularity::Day => Duration::days(1),
                Granularity::FromCaptures => {
                    if second.is_some() {
                        Duration::seconds(1)
                    } else if minute.is_some() {
                        Duration::minutes(1)
                    } else if hour.is_some() {
                        Duration::hours(1)
                    } else {
                        Duration::days(1)
                    }
                }
            };
            let start = PrimitiveDateTime::new(date, time).assume_utc();
            let Some(end) = start.checked_add(span) else {
                continue;
            };
            if span >= Duration::days(1) {
                day_ranges.push((start, end));
            } else {
                fine_ranges.push((start, end));
            }
        }
    }
    let (start, end) = match (hull(&fine_ranges), hull(&day_ranges)) {
        (Some(fine), None) => fine,
        (None, Some(day)) => day,
        (None, None) => return None,
        (Some(fine), Some(day)) => {
            let agrees = day_ranges
                .iter()
                .any(|&(lo, hi)| lo <= fine.0 && fine.1 <= hi);
            if agrees {
                fine
            } else {
                (fine.0.min(day.0), fine.1.max(day.1))
            }
        }
    };
    Some((start.checked_sub(DELIVERY_LAG).unwrap_or(start), end))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn utc(y: i32, mo: u8, d: u8, h: u8, mi: u8, s: u8) -> OffsetDateTime {
        PrimitiveDateTime::new(
            Date::from_calendar_date(y, Month::try_from(mo).unwrap(), d).unwrap(),
            Time::from_hms(h, mi, s).unwrap(),
        )
        .assume_utc()
    }

    #[test]
    fn alb_keys_keep_minute_precision_with_delivery_lag() {
        let key = "AWSLogs/123/elasticloadbalancing/us-east-1/2026/06/09/123_elasticloadbalancing_us-east-1_app.web.abc_20260609T2300Z_10.0.1.5_x4.log.gz";
        let (start, end) = extract_key_time(key).unwrap();
        // The filename stamp agrees with the path date, so the fine range
        // wins — widened backwards by the delivery lag.
        assert_eq!(start, utc(2026, 6, 9, 22, 45, 0));
        assert_eq!(end, utc(2026, 6, 9, 23, 1, 0));
    }

    #[test]
    fn conflicting_stamps_union_instead_of_trusting_one() {
        // A stale dashed stamp in the filename must not narrow the range
        // away from the canonical path date.
        let key = "logs/2026/06/09/export-2025-01-01-00.gz";
        let (start, end) = extract_key_time(key).unwrap();
        assert_eq!(start, utc(2024, 12, 31, 23, 45, 0));
        assert_eq!(end, utc(2026, 6, 10, 0, 0, 0));
    }

    #[test]
    fn extracts_plain_date_paths_as_full_days() {
        let (start, end) =
            extract_key_time("AWSLogs/1/vpcflowlogs/eu-west-1/2026/06/09/file").unwrap();
        assert_eq!(start, utc(2026, 6, 8, 23, 45, 0));
        assert_eq!(end, utc(2026, 6, 10, 0, 0, 0));

        // A trailing numeric segment may be a shard id, not an hour — the
        // range stays day-wide.
        let (start, end) = extract_key_time("firehose/2026/06/09/14/batch-1").unwrap();
        assert_eq!(start, utc(2026, 6, 8, 23, 45, 0));
        assert_eq!(end, utc(2026, 6, 10, 0, 0, 0));
    }

    #[test]
    fn extracts_hive_and_date_partitions() {
        let (start, end) =
            extract_key_time("logs/year=2026/month=6/day=9/hour=7/part-0.gz").unwrap();
        assert_eq!(start, utc(2026, 6, 9, 6, 45, 0));
        assert_eq!(end, utc(2026, 6, 9, 8, 0, 0));

        let (start, _) = extract_key_time("vector/date=2026-06-09/1759990000-uuid.log.gz").unwrap();
        assert_eq!(start, utc(2026, 6, 8, 23, 45, 0));
    }

    #[test]
    fn extracts_dashed_stamps_hourly_and_to_the_second() {
        let (start, end) = extract_key_time("cf/E123ABC.2026-06-09-15.a1b2c3.gz").unwrap();
        assert_eq!(start, utc(2026, 6, 9, 14, 45, 0));
        assert_eq!(end, utc(2026, 6, 9, 16, 0, 0));

        let (start, end) = extract_key_time("access/2026-06-09-15-42-07-DEADBEEF").unwrap();
        assert_eq!(start, utc(2026, 6, 9, 15, 27, 7));
        assert_eq!(end, utc(2026, 6, 9, 15, 42, 8));
    }

    #[test]
    fn undated_keys_have_no_time() {
        assert!(extract_key_time("plain/file.txt").is_none());
        assert!(extract_key_time("v1.2.3/build-4567").is_none());
        // Stamps inside longer digit runs are not dates.
        assert!(extract_key_time("run-12026-06-09-15").is_none());
        // Implausible years are not dates (and must not panic).
        assert!(extract_key_time("ids/9999-12-31-23/x").is_none());
        assert!(extract_key_time("ids/0426-01-02-03/x").is_none());
    }

    #[test]
    fn time_window_overlap_includes_day_spanning_objects() {
        let scope = Scope::from_args(None, None, Some("2026-06-09T14:00".into()), None, None)
            .unwrap()
            .unwrap();
        // Day-granular key overlaps a 14:00 cutoff — must be included.
        assert!(scope.matches("AWSLogs/1/CloudTrail/r/2026/06/09/x.json.gz"));
        // The day before ends at midnight — excluded.
        assert!(!scope.matches("AWSLogs/1/CloudTrail/r/2026/06/08/x.json.gz"));
        // Undated keys are kept and counted.
        assert!(scope.matches("no-date-here.log"));
        assert_eq!(scope.undated.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn until_excludes_later_objects_but_keeps_delivery_lagged_ones() {
        let scope = Scope::from_args(None, None, None, Some("2026-06-08".into()), None)
            .unwrap()
            .unwrap();
        // The next day's key may hold delivery-lagged events from June 8.
        assert!(scope.matches("logs/2026/06/09/x.gz"));
        assert!(scope.matches("logs/2026/06/08/x.gz"));
        // Two days later is out of any lag window.
        assert!(!scope.matches("logs/2026/06/10/x.gz"));
    }

    #[test]
    fn year_9999_until_errors_instead_of_panicking() {
        assert!(Scope::from_args(None, None, None, Some("9999-12-31".into()), None).is_err());
    }

    #[test]
    fn rejects_empty_or_reversed_time_windows() {
        assert!(Scope::from_args(
            None,
            None,
            Some("2026-06-10".into()),
            Some("2026-06-09".into()),
            None,
        )
        .is_err());
        assert!(Scope::from_args(
            None,
            None,
            Some("2026-06-10T00:00:00Z".into()),
            Some("2026-06-09".into()),
            None,
        )
        .is_err());
    }

    #[test]
    fn extreme_relative_times_error_instead_of_panicking() {
        let now = utc(2026, 6, 10, 12, 0, 0);
        assert!(parse_instant("9223372036854775807w", now, Bound::Start).is_err());
        assert!(parse_instant("9223372036854775807s", now, Bound::Start).is_err());
    }

    #[test]
    fn prefix_is_exposed_not_enforced_and_regex_filters() {
        let scope = Scope::from_args(
            Some("prod/".into()),
            Some(r"\.gz$".into()),
            None,
            None,
            None,
        )
        .unwrap()
        .unwrap();
        assert_eq!(scope.key_prefix(), Some("prod/"));
        assert!(scope.matches("prod/a.gz"));
        // prefix enforcement lives in KeyScope::admits, not here
        assert!(scope.matches("dev/a.gz"));
        assert!(!scope.matches("prod/a.txt"));
    }

    #[test]
    fn parses_absolute_and_relative_instants() {
        let now = utc(2026, 6, 10, 12, 0, 0);
        assert_eq!(
            parse_instant("2026-06-09", now, Bound::Start).unwrap(),
            utc(2026, 6, 9, 0, 0, 0)
        );
        assert_eq!(
            parse_instant("2026-06-09T14:30", now, Bound::Start).unwrap(),
            utc(2026, 6, 9, 14, 30, 0)
        );
        assert_eq!(
            parse_instant("2026-06-09T14:30:15Z", now, Bound::Start).unwrap(),
            utc(2026, 6, 9, 14, 30, 15)
        );
        assert_eq!(
            parse_instant("6h", now, Bound::Start).unwrap(),
            utc(2026, 6, 10, 6, 0, 0)
        );
        assert_eq!(
            parse_instant("2d", now, Bound::Start).unwrap(),
            utc(2026, 6, 8, 12, 0, 0)
        );
        assert!(parse_instant("6x", now, Bound::Start).is_err());
        assert!(parse_instant("not-a-date", now, Bound::Start).is_err());
    }
}
