use std::time::Duration;

use kv_bench::{LatencySummary, MetricsError, calculate_run_metrics};

#[test]
fn one_and_two_sample_nearest_rank_and_fractional_microseconds_are_exact() {
    let one = LatencySummary::from_nanos(&[1_501]).unwrap();
    assert_eq!(one.sample_count(), 1);
    assert_eq!(one.total_nanos(), 1_501);
    assert_eq!(one.mean_us(), 1.501);
    assert_eq!(one.p50_us(), 1.501);
    assert_eq!(one.p95_us(), 1.501);
    assert_eq!(one.p99_us(), 1.501);

    let two = LatencySummary::from_nanos(&[2_001, 1_000]).unwrap();
    assert_eq!(two.sample_count(), 2);
    assert_eq!(two.mean_us(), 1.5005);
    assert_eq!(two.p50_nanos(), 1_000);
    assert_eq!(two.p95_nanos(), 2_001);
    assert_eq!(two.p99_nanos(), 2_001);
}

#[test]
fn hundred_sample_nearest_rank_golden_uses_merged_samples() {
    let samples: Vec<_> = (1..=100).rev().collect();
    let summary = LatencySummary::from_nanos(&samples).unwrap();
    assert_eq!(summary.sample_count(), 100);
    assert_eq!(summary.total_nanos(), 5_050);
    assert_eq!(summary.mean_nanos(), 50.5);
    assert_eq!(summary.p50_nanos(), 50);
    assert_eq!(summary.p95_nanos(), 95);
    assert_eq!(summary.p99_nanos(), 99);
}

#[test]
fn zero_or_inconsistent_inputs_never_form_valid_metrics() {
    assert_eq!(
        LatencySummary::from_nanos(&[]),
        Err(MetricsError::EmptyLatencySamples)
    );
    assert_eq!(
        calculate_run_metrics(Duration::ZERO, 1, 1, false, &[1]),
        Err(MetricsError::ZeroElapsedTime)
    );
    assert_eq!(
        calculate_run_metrics(Duration::from_nanos(1), 0, 0, false, &[]),
        Err(MetricsError::ZeroCompletedOperations)
    );
    assert_eq!(
        calculate_run_metrics(Duration::from_secs(1), 2, 2, false, &[1]),
        Err(MetricsError::SampleCountMismatch {
            completed_ops: 2,
            samples: 1,
        })
    );
}

#[test]
fn throughput_and_optional_records_rate_follow_wall_clock_formula() {
    let elapsed = Duration::from_millis(250);
    let metrics = calculate_run_metrics(elapsed, 100, 10_000, true, &vec![1_250; 100])
        .expect("valid metrics");
    assert_eq!(metrics.elapsed(), elapsed);
    assert_eq!(metrics.elapsed_seconds(), 0.25);
    assert_eq!(metrics.ops_per_second(), 400.0);
    assert_eq!(metrics.records_per_second(), Some(40_000.0));
    assert_eq!(metrics.latency().mean_us(), 1.25);
    assert!(metrics.ops_per_second().is_finite());
    assert!(metrics.records_per_second().unwrap().is_finite());

    let no_auxiliary = calculate_run_metrics(elapsed, 2, 2, false, &[1_001, 2_001]).unwrap();
    assert_eq!(no_auxiliary.records_per_second(), None);
    assert!(no_auxiliary.ops_per_second().is_finite());
}
