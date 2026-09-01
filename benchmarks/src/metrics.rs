//! Integer-nanosecond latency aggregation and wall-clock throughput metrics.

use std::error::Error;
use std::fmt;
use std::time::Duration;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MetricsError {
    EmptyLatencySamples,
    LatencyTotalOverflow,
    ZeroElapsedTime,
    ZeroCompletedOperations,
    SampleCountMismatch { completed_ops: u64, samples: usize },
    NonFiniteMetric,
}

impl fmt::Display for MetricsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid benchmark metrics: {self:?}")
    }
}

impl Error for MetricsError {}

#[derive(Clone, Debug, PartialEq)]
pub struct LatencySummary {
    sample_count: usize,
    total_nanos: u128,
    mean_nanos: f64,
    p50_nanos: u64,
    p95_nanos: u64,
    p99_nanos: u64,
}

impl LatencySummary {
    pub fn from_nanos(samples: &[u64]) -> Result<Self, MetricsError> {
        if samples.is_empty() {
            return Err(MetricsError::EmptyLatencySamples);
        }
        let total_nanos = samples.iter().try_fold(0_u128, |total, sample| {
            total
                .checked_add(u128::from(*sample))
                .ok_or(MetricsError::LatencyTotalOverflow)
        })?;
        let mut ordered = samples.to_vec();
        ordered.sort_unstable();
        let mean_nanos = total_nanos as f64 / samples.len() as f64;
        require_finite(&[mean_nanos])?;
        Ok(Self {
            sample_count: samples.len(),
            total_nanos,
            mean_nanos,
            // Fixed nearest-rank: rank = ceil(p * N), using a one-based rank;
            // the selected zero-based index is rank - 1. Thus N=1 always
            // selects its sole sample and P50 of N=2 selects the first sample.
            p50_nanos: nearest_rank(&ordered, 50),
            p95_nanos: nearest_rank(&ordered, 95),
            p99_nanos: nearest_rank(&ordered, 99),
        })
    }

    pub const fn sample_count(&self) -> usize {
        self.sample_count
    }

    pub const fn total_nanos(&self) -> u128 {
        self.total_nanos
    }

    pub fn mean_nanos(&self) -> f64 {
        self.mean_nanos
    }

    pub fn mean_us(&self) -> f64 {
        self.mean_nanos / 1_000.0
    }

    pub const fn p50_nanos(&self) -> u64 {
        self.p50_nanos
    }

    pub const fn p95_nanos(&self) -> u64 {
        self.p95_nanos
    }

    pub const fn p99_nanos(&self) -> u64 {
        self.p99_nanos
    }

    pub fn p50_us(&self) -> f64 {
        self.p50_nanos as f64 / 1_000.0
    }

    pub fn p95_us(&self) -> f64 {
        self.p95_nanos as f64 / 1_000.0
    }

    pub fn p99_us(&self) -> f64 {
        self.p99_nanos as f64 / 1_000.0
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct RunMetrics {
    elapsed: Duration,
    ops_per_second: f64,
    records_per_second: Option<f64>,
    latency: LatencySummary,
}

impl RunMetrics {
    pub const fn elapsed(&self) -> Duration {
        self.elapsed
    }

    pub fn elapsed_seconds(&self) -> f64 {
        self.elapsed.as_secs_f64()
    }

    pub fn ops_per_second(&self) -> f64 {
        self.ops_per_second
    }

    pub fn records_per_second(&self) -> Option<f64> {
        self.records_per_second
    }

    pub const fn latency(&self) -> &LatencySummary {
        &self.latency
    }
}

pub fn calculate_run_metrics(
    elapsed: Duration,
    completed_ops: u64,
    completed_records: u64,
    report_records_per_second: bool,
    latency_nanos: &[u64],
) -> Result<RunMetrics, MetricsError> {
    if elapsed.is_zero() {
        return Err(MetricsError::ZeroElapsedTime);
    }
    if completed_ops == 0 {
        return Err(MetricsError::ZeroCompletedOperations);
    }
    if usize::try_from(completed_ops).ok() != Some(latency_nanos.len()) {
        return Err(MetricsError::SampleCountMismatch {
            completed_ops,
            samples: latency_nanos.len(),
        });
    }
    let elapsed_seconds = elapsed.as_secs_f64();
    let ops_per_second = completed_ops as f64 / elapsed_seconds;
    let records_per_second =
        report_records_per_second.then_some(completed_records as f64 / elapsed_seconds);
    require_finite(&[
        elapsed_seconds,
        ops_per_second,
        records_per_second.unwrap_or(0.0),
    ])?;
    Ok(RunMetrics {
        elapsed,
        ops_per_second,
        records_per_second,
        latency: LatencySummary::from_nanos(latency_nanos)?,
    })
}

fn nearest_rank(ordered: &[u64], percentile: u128) -> u64 {
    let count = ordered.len() as u128;
    let rank = percentile
        .checked_mul(count)
        .expect("sample count fits nearest-rank arithmetic")
        .div_ceil(100);
    ordered[usize::try_from(rank - 1).expect("nearest-rank index fits usize")]
}

fn require_finite(values: &[f64]) -> Result<(), MetricsError> {
    if values.iter().all(|value| value.is_finite()) {
        Ok(())
    } else {
        Err(MetricsError::NonFiniteMetric)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nan_and_infinity_are_rejected_by_the_metric_guard() {
        assert_eq!(
            require_finite(&[f64::NAN]),
            Err(MetricsError::NonFiniteMetric)
        );
        assert_eq!(
            require_finite(&[f64::INFINITY]),
            Err(MetricsError::NonFiniteMetric)
        );
        assert_eq!(
            require_finite(&[f64::NEG_INFINITY]),
            Err(MetricsError::NonFiniteMetric)
        );
    }
}
