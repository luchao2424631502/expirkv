use std::collections::HashSet;

use kv_bench::{
    BenchConfig, SplitMix64, Trace, TraceError, Workload, derive_trace_seed,
    deterministic_permutation,
};

#[test]
fn splitmix64_and_unbiased_bounded_sampling_have_golden_outputs() {
    let mut random = SplitMix64::new(0);
    assert_eq!(
        (0..5).map(|_| random.next_u64()).collect::<Vec<_>>(),
        [
            0xe220_a839_7b1d_cdaf,
            0x6e78_9e6a_a1b9_65f4,
            0x06c4_5d18_8009_454f,
            0xf88b_b8a8_724c_81ec,
            0x1b39_896a_51a8_749b,
        ]
    );

    let mut bounded = SplitMix64::new(123);
    assert_eq!(
        (0..12)
            .map(|_| bounded.uniform_below(10))
            .collect::<Vec<_>>(),
        [5, 8, 0, 1, 2, 6, 4, 7, 1, 9, 5, 0]
    );
    assert!((0..1_000).all(|_| bounded.uniform_below(7) < 7));
    assert!((0..32).all(|_| bounded.uniform_below(1) == 0));
}

#[test]
fn seed_derivation_and_fisher_yates_permutations_are_frozen() {
    let base_seed = BenchConfig::formal().seed();
    assert_eq!(
        derive_trace_seed(base_seed, Workload::RandomGet, 0),
        0x4699_7cef_fe5b_8643
    );
    assert_eq!(
        derive_trace_seed(base_seed, Workload::RangeScan, 0),
        0x1402_c544_10d4_5bd1
    );
    let put_seed = derive_trace_seed(base_seed, Workload::SinglePut, 0);
    let delete_seed = derive_trace_seed(base_seed, Workload::SingleDelete, 0);
    assert_eq!(put_seed, 0xf4e5_d049_236b_3ea4);
    assert_eq!(delete_seed, 0x9b93_512f_4999_217e);
    assert_eq!(
        put_seed,
        derive_trace_seed(base_seed, Workload::BatchPut, 0)
    );
    assert_eq!(
        delete_seed,
        derive_trace_seed(base_seed, Workload::BatchDelete, 0)
    );
    assert_eq!(
        deterministic_permutation(10, put_seed),
        [9, 8, 2, 5, 4, 3, 7, 6, 0, 1]
    );
    assert_eq!(
        deterministic_permutation(10, delete_seed),
        [6, 1, 8, 2, 7, 4, 0, 3, 9, 5]
    );
}

#[test]
fn all_six_small_traces_have_frozen_golden_content() {
    let config = BenchConfig::test_only(10, 3, 2, 8, 6);
    let expected = [
        (Workload::RandomGet, vec![7, 8, 4, 7, 6, 5, 6, 2], 1),
        (Workload::RangeScan, vec![0, 0, 5, 7, 0, 6], 1),
        (Workload::SinglePut, vec![9, 8, 2, 5, 4, 3, 7, 6, 0, 1], 1),
        (Workload::BatchPut, vec![9, 8, 2, 5, 4, 3, 7, 6, 0, 1], 2),
        (
            Workload::SingleDelete,
            vec![6, 1, 8, 2, 7, 4, 0, 3, 9, 5],
            1,
        ),
        (Workload::BatchDelete, vec![6, 1, 8, 2, 7, 4, 0, 3, 9, 5], 2),
    ];

    for (workload, logical_ids, request_width) in expected {
        let trace = Trace::generate(&config, workload, 0).expect("small trace must generate");
        assert_eq!(trace.workload(), workload);
        assert_eq!(trace.repetition(), 0);
        assert_eq!(trace.request_width(), request_width);
        assert_eq!(trace.logical_ids(), logical_ids);
        assert_eq!(
            trace.request_count(),
            workload.operation_count(&config) as usize
        );
    }
}

#[test]
fn read_traces_are_in_range_and_intentionally_allow_repetition() {
    let config = BenchConfig::test_only(20, 4, 5, 64, 64);
    let point = Trace::generate(&config, Workload::RandomGet, 0).unwrap();
    assert!(point.logical_ids().iter().all(|id| *id < 20));
    assert!(HashSet::<_>::from_iter(point.logical_ids()).len() < point.logical_ids().len());

    let range = Trace::generate(&config, Workload::RangeScan, 0).unwrap();
    let max_start = config.record_count() - config.range_length();
    assert!(range.logical_ids().iter().all(|start| *start <= max_start));
    assert!(HashSet::<_>::from_iter(range.logical_ids()).len() < range.logical_ids().len());
}

#[test]
fn batch_traces_only_group_the_corresponding_single_write_order() {
    let config = BenchConfig::test_only(20, 4, 5, 16, 12);
    for (single, batch) in [
        (Workload::SinglePut, Workload::BatchPut),
        (Workload::SingleDelete, Workload::BatchDelete),
    ] {
        let single = Trace::generate(&config, single, 2).unwrap();
        let batch = Trace::generate(&config, batch, 2).unwrap();
        assert_eq!(batch.logical_ids(), single.logical_ids());
        assert_eq!(batch.request_width(), 5);
        assert_eq!(batch.request_count(), 4);
        assert!(batch.requests().all(|request| request.len() == 5));
        let unique: HashSet<_> = batch.logical_ids().iter().copied().collect();
        assert_eq!(unique.len(), 20);
        assert_eq!(unique, HashSet::from_iter(0..20));
    }
}

#[test]
fn every_workload_partitions_without_changing_the_global_trace() {
    let config = BenchConfig::test_only(20, 4, 5, 64, 32);
    for workload in Workload::ALL {
        let trace = Trace::generate(&config, workload, 1).unwrap();
        for thread_count in [1, 10, 100, 1_000] {
            let partitions = trace.partition(thread_count).unwrap();
            assert_eq!(partitions.len(), thread_count);
            assert_eq!(
                partitions
                    .iter()
                    .map(|part| part.request_count())
                    .sum::<usize>(),
                trace.request_count()
            );
            assert_eq!(
                partitions
                    .iter()
                    .flat_map(|part| part.logical_ids().iter().copied())
                    .collect::<Vec<_>>(),
                trace.logical_ids()
            );

            let base = trace.request_count() / thread_count;
            let remainder = trace.request_count() % thread_count;
            let mut next_start = 0;
            for (index, partition) in partitions.iter().enumerate() {
                assert_eq!(partition.thread_index(), index);
                assert_eq!(partition.request_start(), next_start);
                assert_eq!(partition.request_width(), trace.request_width());
                assert_eq!(
                    partition.request_count(),
                    base + usize::from(index < remainder)
                );
                assert!(
                    partition
                        .requests()
                        .all(|request| request.len() == trace.request_width())
                );
                next_start += partition.request_count();
            }
        }
    }
}

#[test]
fn repetitions_are_deterministic_distinct_and_range_checked() {
    let config = BenchConfig::test_only(20, 4, 5, 64, 32);
    for workload in Workload::ALL {
        let first = Trace::generate(&config, workload, 0).unwrap();
        let first_again = Trace::generate(&config, workload, 0).unwrap();
        let second = Trace::generate(&config, workload, 1).unwrap();
        assert_eq!(first, first_again);
        assert_ne!(first.seed(), second.seed());
        assert_ne!(first.logical_ids(), second.logical_ids());
    }
    assert_eq!(
        Trace::generate(&config, Workload::RandomGet, 5),
        Err(TraceError::RepetitionOutOfRange {
            repetition: 5,
            repetitions: 5,
        })
    );
    let trace = Trace::generate(&config, Workload::RandomGet, 0).unwrap();
    assert_eq!(trace.partition(0), Err(TraceError::ZeroThreads));
}
