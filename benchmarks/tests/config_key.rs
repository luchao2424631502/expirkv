use kv_bench::{
    BenchConfig, BenchMode, KEY_LENGTH, KeyCodecError, Workload, decode_key, encode_key,
    fixed_value,
};

#[test]
fn formal_configuration_is_fully_frozen() {
    let config = BenchConfig::formal();
    assert_eq!(config.mode(), BenchMode::Formal);
    assert!(config.is_formal());
    assert_eq!(config.record_count(), 10_000_000);
    assert_eq!(config.key_length(), 16);
    assert_eq!(config.value_length(), 1_024);
    assert_eq!(config.range_length(), 100);
    assert_eq!(config.batch_size(), 100);
    assert!(!config.sync_writes());
    assert!(!config.compression_enabled());
    assert_eq!(config.seed(), 20_260_720);
    assert_eq!(config.repetitions(), 5);
    assert_eq!(config.thread_counts(), &[1, 10, 100, 1_000]);
    assert_eq!(config.random_get_operations(), 10_000_000);
    assert_eq!(config.range_scan_operations(), 1_000_000);
    assert_eq!(config.write_buffer_size(), 4 * 1_024 * 1_024);
    assert_eq!(config.block_cache_size(), 8 * 1_024 * 1_024);
    assert_eq!(config.block_size(), 4 * 1_024);
    assert_eq!(config.block_restart_interval(), 16);
    assert_eq!(config.max_open_files(), 1_000);
    assert_eq!(config.max_table_file_size(), 2 * 1_024 * 1_024);

    assert_eq!(Workload::RandomGet.operation_count(&config), 10_000_000);
    assert_eq!(Workload::RangeScan.operation_count(&config), 1_000_000);
    assert_eq!(Workload::SinglePut.operation_count(&config), 10_000_000);
    assert_eq!(Workload::BatchPut.operation_count(&config), 100_000);
    assert_eq!(Workload::SingleDelete.operation_count(&config), 10_000_000);
    assert_eq!(Workload::BatchDelete.operation_count(&config), 100_000);
}

#[test]
fn small_configuration_is_unambiguously_non_formal() {
    let config = BenchConfig::test_only(20, 4, 5, 64, 32);
    assert_eq!(config.mode(), BenchMode::Smoke);
    assert!(!config.is_formal());
    assert_eq!(config.record_count(), 20);
    assert_eq!(config.range_length(), 4);
    assert_eq!(config.batch_size(), 5);
    assert_eq!(config.random_get_operations(), 64);
    assert_eq!(config.range_scan_operations(), 32);
    assert_eq!(config.seed(), BenchConfig::formal().seed());
    assert_eq!(config.value_length(), BenchConfig::formal().value_length());
}

#[test]
fn key_codec_is_exact_big_endian_and_order_preserving() {
    let config = BenchConfig::formal();
    let cases = [0, 1, 255, 256, config.record_count() - 1];
    let encoded: Vec<_> = cases
        .into_iter()
        .map(|id| encode_key(&config, id).expect("valid id must encode"))
        .collect();

    for (id, key) in cases.into_iter().zip(&encoded) {
        assert_eq!(key.len(), KEY_LENGTH);
        assert_eq!(&key[..8], &[0; 8]);
        assert_eq!(&key[8..], &id.to_be_bytes());
        assert_eq!(decode_key(&config, key), Ok(id));
    }
    assert!(encoded.windows(2).all(|pair| pair[0] < pair[1]));
    assert_eq!(encoded[2][14..], [0, 255]);
    assert_eq!(encoded[3][14..], [1, 0]);
}

#[test]
fn key_codec_rejects_wrong_namespace_length_and_range() {
    let config = BenchConfig::test_only(10, 3, 2, 8, 6);
    assert_eq!(
        encode_key(&config, 10),
        Err(KeyCodecError::IdOutOfRange {
            id: 10,
            record_count: 10,
        })
    );
    assert_eq!(
        decode_key(&config, &[0; 15]),
        Err(KeyCodecError::WrongLength { actual: 15 })
    );

    let mut foreign_namespace = [0_u8; KEY_LENGTH];
    foreign_namespace[7] = 1;
    assert_eq!(
        decode_key(&config, &foreign_namespace),
        Err(KeyCodecError::NonZeroNamespace)
    );

    let mut out_of_range = [0_u8; KEY_LENGTH];
    out_of_range[8..].copy_from_slice(&10_u64.to_be_bytes());
    assert_eq!(
        decode_key(&config, &out_of_range),
        Err(KeyCodecError::IdOutOfRange {
            id: 10,
            record_count: 10,
        })
    );
}

#[test]
fn fixed_value_has_stable_bytes_and_digest() {
    let config = BenchConfig::formal();
    let first = fixed_value(&config);
    let second = fixed_value(&config);
    assert_eq!(first, second);
    assert_eq!(first.len(), 1_024);
    assert_eq!(
        &first[..16],
        &[
            165, 136, 18, 7, 172, 221, 178, 38, 39, 115, 164, 225, 201, 62, 30, 231
        ]
    );
    assert_eq!(
        &first[first.len() - 16..],
        &[
            49, 37, 44, 227, 216, 23, 205, 195, 47, 123, 240, 165, 145, 35, 251, 60
        ]
    );
    assert_eq!(fnv1a64(&first), 0x9de1_9d21_dc49_8012);
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
    })
}
