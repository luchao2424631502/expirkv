#![allow(dead_code)]

#[path = "../src/error.rs"]
mod error;

pub(crate) use error::{
    Operation, ProtocolStage, Result, RetryAdvice, StorageError, StorageErrorKind, WriteOutcome,
};

#[path = "../src/vlog/format.rs"]
pub(crate) mod vlog_format;

mod vlog {
    pub(crate) use crate::vlog_format as format;
}

#[path = "../src/commit/descriptor.rs"]
mod descriptor;

#[path = "../src/format.rs"]
mod root_format;

use crc32c::{crc32c, crc32c_append};
use descriptor::*;
use root_format::*;
use vlog::format::*;

fn assert_error_kind<T: std::fmt::Debug>(result: Result<T>, expected: StorageErrorKind) {
    let error = result.unwrap_err();
    assert_eq!(error.kind, expected);
    assert!(error.message.is_empty());
    assert!(error.source.is_none());
}

fn sample_uuid() -> [u8; 16] {
    [
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
        0x10,
    ]
}

fn sample_pointer(file_id: u32, record_offset: u32) -> ValuePointer {
    pointer_for_key(file_id, record_offset, 3, 5)
}

fn pointer_for_key(file_id: u32, record_offset: u32, key_len: u32, value_len: u16) -> ValuePointer {
    ValuePointer {
        format_version: 0,
        file_id,
        record_offset,
        record_len: 55 + key_len + u32::from(value_len),
        value_len,
    }
}

fn sample_meta(distinct_key_count: u64) -> TxMeta {
    TxMeta {
        commit_seq: 2,
        tx_uuid: TxUuid(sample_uuid()),
        prev_seq: 1,
        vlog_begin: VLogPos {
            file_id: 3,
            offset: 64,
        },
        vlog_end: VLogPos {
            file_id: 3,
            offset: 200,
        },
        logical_op_count: 3,
        distinct_key_count,
        envelope_crc32c: 0x1122_3344,
        descriptor_crc32c: 0x5566_7788,
    }
}

fn sample_descriptor() -> TransactionDescriptor {
    TransactionDescriptor {
        meta: sample_meta(2),
        mutations: vec![
            TxMutation {
                user_key: vec![0x00, 0xff],
                before_state: ValueState::Absent,
                after_state: ValueState::Present(pointer_for_key(7, 64, 2, 5)),
            },
            TxMutation {
                user_key: b"second".to_vec(),
                before_state: ValueState::Present(pointer_for_key(8, 128, 6, 5)),
                after_state: ValueState::Absent,
            },
        ],
    }
}

fn mutation_refs(encoded: &EncodedDescriptor) -> Vec<(&[u8], &[u8])> {
    encoded
        .mutations
        .iter()
        .map(|mutation| (mutation.key.as_slice(), mutation.value.as_slice()))
        .collect()
}

fn recompute_descriptor_crc(
    meta_value: &mut [u8; TX_META_ENCODED_LEN],
    mutations: &[EncodedMutation],
) {
    let mut crc = crc32c(b"RKDESC0");
    crc = crc32c_append(crc, &meta_value[0..82]);
    for mutation in mutations {
        let value_len = u32::try_from(mutation.value.len()).unwrap();
        crc = crc32c_append(crc, &mutation.key);
        crc = crc32c_append(crc, &value_len.to_le_bytes());
        crc = crc32c_append(crc, &mutation.value);
    }
    meta_value[82..86].copy_from_slice(&crc.to_le_bytes());
}

fn rewrite_crc(bytes: &mut [u8], covered_len: usize, crc_offset: usize) {
    let checksum = crc32c(&bytes[..covered_len]);
    bytes[crc_offset..crc_offset + 4].copy_from_slice(&checksum.to_le_bytes());
}

#[test]
fn format_v0_golden_round_trip_and_strict_validation() {
    let metadata = FormatMetadataV0::new(sample_uuid()).unwrap();
    let encoded = metadata.encode().unwrap();
    let mut expected = Vec::new();
    expected.extend_from_slice(b"RUSTKV00");
    expected.extend_from_slice(&0_u32.to_le_bytes());
    expected.extend_from_slice(&sample_uuid());
    expected.extend_from_slice(&65_536_u32.to_le_bytes());
    expected.extend_from_slice(&60_000_u32.to_le_bytes());
    assert_eq!(encoded.len(), 36);
    assert_eq!(encoded.as_slice(), expected);
    assert_eq!(FormatMetadataV0::decode(&encoded).unwrap(), metadata);

    assert_error_kind(
        FormatMetadataV0::decode(&encoded[..35]),
        StorageErrorKind::Corruption,
    );
    let mut too_long = encoded.to_vec();
    too_long.push(0);
    assert_error_kind(
        FormatMetadataV0::decode(&too_long),
        StorageErrorKind::Corruption,
    );

    let mut damaged = encoded;
    damaged[0] ^= 1;
    assert_error_kind(
        FormatMetadataV0::decode(&damaged),
        StorageErrorKind::Corruption,
    );
    let mut damaged = encoded;
    damaged[8..12].copy_from_slice(&1_u32.to_le_bytes());
    assert_error_kind(
        FormatMetadataV0::decode(&damaged),
        StorageErrorKind::IncompatibleFormat,
    );
    let mut damaged = encoded;
    damaged[12..28].fill(0);
    assert_error_kind(
        FormatMetadataV0::decode(&damaged),
        StorageErrorKind::Corruption,
    );
    let mut damaged = encoded;
    damaged[28..32].copy_from_slice(&4096_u32.to_le_bytes());
    assert_error_kind(
        FormatMetadataV0::decode(&damaged),
        StorageErrorKind::Corruption,
    );
    let mut damaged = encoded;
    damaged[32..36].copy_from_slice(&60_001_u32.to_le_bytes());
    assert_error_kind(
        FormatMetadataV0::decode(&damaged),
        StorageErrorKind::Corruption,
    );
    assert_error_kind(
        FormatMetadataV0::new([0; 16]),
        StorageErrorKind::InvalidArgument,
    );
}

#[test]
fn value_pointer_v0_golden_layout_and_bounds() {
    let pointer = sample_pointer(42, 64);
    let encoded = pointer.encode().unwrap();
    assert_eq!(
        encoded,
        [
            0x00, 0x00, 0x2a, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00, 0x3f, 0x00, 0x00, 0x00,
            0x05, 0x00,
        ]
    );
    assert_eq!(ValuePointer::decode(&encoded).unwrap(), pointer);

    let layout = pointer.validate_file_bounds(127).unwrap();
    assert_eq!(layout.key_len, 3);
    assert_eq!(layout.value_relative_offset, 54);
    assert_eq!(layout.value_start, 118);
    assert_eq!(layout.value_end, 123);
    assert_eq!(layout.record_end, 127);

    let minimum = ValuePointer {
        format_version: 0,
        file_id: 0,
        record_offset: 64,
        record_len: 56,
        value_len: 0,
    };
    assert_eq!(minimum.layout().unwrap().key_len, 1);
    minimum.validate_file_bounds(120).unwrap();

    let maximum = ValuePointer {
        format_version: 0,
        file_id: 999_999,
        record_offset: 64,
        record_len: 60_055,
        value_len: 59_999,
    };
    assert_eq!(maximum.layout().unwrap().key_len, 1);
    maximum.validate_file_bounds(60_119).unwrap();

    let at_file_end = ValuePointer {
        record_offset: u32::try_from((1_u64 << 32) - 60_055).unwrap(),
        ..maximum
    };
    assert_eq!(
        at_file_end
            .validate_file_bounds(1_u64 << 32)
            .unwrap()
            .record_end,
        1_u64 << 32
    );
}

#[test]
fn value_pointer_rejects_malformed_and_unsafe_locations_before_io() {
    let encoded = sample_pointer(1, 64).encode().unwrap();
    assert_error_kind(
        ValuePointer::decode(&encoded[..15]),
        StorageErrorKind::Corruption,
    );
    let mut too_long = encoded.to_vec();
    too_long.push(0);
    assert_error_kind(
        ValuePointer::decode(&too_long),
        StorageErrorKind::Corruption,
    );

    let mut damaged = encoded;
    damaged[0..2].copy_from_slice(&1_u16.to_le_bytes());
    assert_error_kind(
        ValuePointer::decode(&damaged),
        StorageErrorKind::IncompatibleFormat,
    );
    let mut damaged = encoded;
    damaged[2..6].copy_from_slice(&1_000_000_u32.to_le_bytes());
    assert_error_kind(ValuePointer::decode(&damaged), StorageErrorKind::Corruption);
    for record_len in [55_u32, 60_056] {
        let mut damaged = encoded;
        damaged[10..14].copy_from_slice(&record_len.to_le_bytes());
        assert_error_kind(ValuePointer::decode(&damaged), StorageErrorKind::Corruption);
    }
    let mut damaged = encoded;
    damaged[10..14].copy_from_slice(&60_055_u32.to_le_bytes());
    damaged[14..16].copy_from_slice(&60_000_u16.to_le_bytes());
    assert_error_kind(ValuePointer::decode(&damaged), StorageErrorKind::Corruption);

    for pointer in [
        ValuePointer {
            record_offset: 0,
            ..sample_pointer(1, 64)
        },
        ValuePointer {
            record_offset: 65_536,
            ..sample_pointer(1, 64)
        },
        ValuePointer {
            record_offset: 65_535,
            ..sample_pointer(1, 64)
        },
        ValuePointer {
            record_offset: u32::MAX,
            ..sample_pointer(1, 64)
        },
    ] {
        assert_error_kind(
            pointer.validate_file_bounds(1_u64 << 32),
            StorageErrorKind::Corruption,
        );
    }
    assert_error_kind(
        sample_pointer(1, 64).validate_file_bounds(126),
        StorageErrorKind::Corruption,
    );
    assert_error_kind(
        sample_pointer(1, 64).validate_file_bounds((1_u64 << 32) + 1),
        StorageErrorKind::Corruption,
    );
}

#[test]
fn transaction_keys_are_big_endian_ordered_and_strict() {
    assert_eq!(DATABASE_IDENTITY_KEY, b"database_identity");
    assert_eq!(HEAD_SEQ_KEY, b"head_seq");
    assert_eq!(DURABLE_FRONTIER_KEY, b"durable_frontier");
    assert_eq!(RECOVERY_STATE_KEY, b"recovery_state");

    let meta_1 = encode_tx_meta_key(1).unwrap();
    let mutation_1_0 = encode_tx_mutation_key(1, 0).unwrap();
    let mutation_1_1 = encode_tx_mutation_key(1, 1).unwrap();
    let meta_2 = encode_tx_meta_key(2).unwrap();

    assert_eq!(meta_1, [b'T', b'X', 0, 0, 0, 0, 0, 0, 0, 1, 0]);
    assert_eq!(
        mutation_1_1,
        [
            b'T', b'X', 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 1
        ]
    );
    assert!(meta_1.as_slice() < mutation_1_0.as_slice());
    assert!(mutation_1_0 < mutation_1_1);
    assert!(mutation_1_1.as_slice() < meta_2.as_slice());
    assert_eq!(decode_tx_meta_key(&meta_1).unwrap(), 1);
    assert_eq!(decode_tx_mutation_key(&mutation_1_1).unwrap(), (1, 1));

    assert_error_kind(encode_tx_meta_key(0), StorageErrorKind::InvalidArgument);
    assert_error_kind(
        encode_tx_mutation_key(0, 0),
        StorageErrorKind::InvalidArgument,
    );
    assert_error_kind(
        decode_tx_meta_key(&meta_1[..10]),
        StorageErrorKind::Corruption,
    );
    let mut meta_key_too_long = meta_1.to_vec();
    meta_key_too_long.push(0);
    assert_error_kind(
        decode_tx_meta_key(&meta_key_too_long),
        StorageErrorKind::Corruption,
    );
    assert_error_kind(
        decode_tx_mutation_key(&mutation_1_0[..18]),
        StorageErrorKind::Corruption,
    );
    let mut mutation_key_too_long = mutation_1_0.to_vec();
    mutation_key_too_long.push(0);
    assert_error_kind(
        decode_tx_mutation_key(&mutation_key_too_long),
        StorageErrorKind::Corruption,
    );
    let mut damaged = meta_1;
    damaged[0] = b'Z';
    assert_error_kind(decode_tx_meta_key(&damaged), StorageErrorKind::Corruption);
    let mut damaged = mutation_1_0;
    damaged[10] = 2;
    assert_error_kind(
        decode_tx_mutation_key(&damaged),
        StorageErrorKind::Corruption,
    );
}

#[test]
fn vlog_position_is_exact_little_endian_and_accepts_exclusive_4gib_end() {
    let position = VLogPos {
        file_id: 999_999,
        offset: 1_u64 << 32,
    };
    let encoded = position.encode().unwrap();
    assert_eq!(
        encoded,
        [
            0x3f, 0x42, 0x0f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00
        ]
    );
    assert_eq!(VLogPos::decode(&encoded).unwrap(), position);
    assert_error_kind(
        VLogPos::decode(&encoded[..11]),
        StorageErrorKind::Corruption,
    );
    let mut too_long = encoded.to_vec();
    too_long.push(0);
    assert_error_kind(VLogPos::decode(&too_long), StorageErrorKind::Corruption);
    assert_error_kind(
        VLogPos {
            file_id: 1_000_000,
            offset: 0,
        }
        .encode(),
        StorageErrorKind::InvalidArgument,
    );
    assert_error_kind(
        VLogPos {
            file_id: 0,
            offset: (1_u64 << 32) + 1,
        }
        .encode(),
        StorageErrorKind::InvalidArgument,
    );
}

#[test]
fn tx_meta_v0_golden_round_trip_and_relations() {
    let meta = sample_meta(2);
    let encoded = encode_tx_meta(&meta).unwrap();
    let mut expected = Vec::new();
    expected.extend_from_slice(b"RKTM");
    expected.extend_from_slice(&0_u16.to_le_bytes());
    expected.extend_from_slice(&2_u64.to_le_bytes());
    expected.extend_from_slice(&sample_uuid());
    expected.extend_from_slice(&1_u64.to_le_bytes());
    expected.extend_from_slice(&3_u32.to_le_bytes());
    expected.extend_from_slice(&64_u64.to_le_bytes());
    expected.extend_from_slice(&3_u32.to_le_bytes());
    expected.extend_from_slice(&200_u64.to_le_bytes());
    expected.extend_from_slice(&3_u64.to_le_bytes());
    expected.extend_from_slice(&2_u64.to_le_bytes());
    expected.extend_from_slice(&0x1122_3344_u32.to_le_bytes());
    expected.extend_from_slice(&0x5566_7788_u32.to_le_bytes());
    assert_eq!(encoded.len(), 86);
    assert_eq!(encoded.as_slice(), expected);
    assert_eq!(decode_tx_meta(&encoded).unwrap(), meta);

    assert_error_kind(decode_tx_meta(&encoded[..85]), StorageErrorKind::Corruption);
    let mut too_long = encoded.to_vec();
    too_long.push(0);
    assert_error_kind(decode_tx_meta(&too_long), StorageErrorKind::Corruption);
    let mut damaged = encoded;
    damaged[0] ^= 1;
    assert_error_kind(decode_tx_meta(&damaged), StorageErrorKind::Corruption);
    let mut damaged = encoded;
    damaged[4..6].copy_from_slice(&1_u16.to_le_bytes());
    assert_error_kind(
        decode_tx_meta(&damaged),
        StorageErrorKind::IncompatibleFormat,
    );
    let mut damaged = encoded;
    damaged[14..30].fill(0);
    assert_error_kind(decode_tx_meta(&damaged), StorageErrorKind::Corruption);

    for invalid in [
        TxMeta {
            commit_seq: 0,
            ..meta.clone()
        },
        TxMeta {
            prev_seq: 0,
            ..meta.clone()
        },
        TxMeta {
            tx_uuid: TxUuid([0; 16]),
            ..meta.clone()
        },
        TxMeta {
            vlog_end: meta.vlog_begin,
            ..meta.clone()
        },
        TxMeta {
            logical_op_count: 0,
            ..meta.clone()
        },
        TxMeta {
            distinct_key_count: 4,
            ..meta.clone()
        },
    ] {
        assert_error_kind(encode_tx_meta(&invalid), StorageErrorKind::InvalidArgument);
    }
}

#[test]
fn tx_mutation_covers_all_state_pairs_and_rejects_trailing_bytes() {
    let pointer = pointer_for_key(7, 64, 2, 5);
    for (before_state, after_state) in [
        (ValueState::Absent, ValueState::Absent),
        (ValueState::Absent, ValueState::Present(pointer)),
        (ValueState::Present(pointer), ValueState::Present(pointer)),
        (ValueState::Present(pointer), ValueState::Absent),
    ] {
        let mutation = TxMutation {
            user_key: vec![0x00, 0xff],
            before_state,
            after_state,
        };
        let encoded = encode_tx_mutation(&mutation).unwrap();
        assert_eq!(decode_tx_mutation(&encoded).unwrap(), mutation);
    }

    let absent = TxMutation {
        user_key: vec![0x00, 0xff],
        before_state: ValueState::Absent,
        after_state: ValueState::Absent,
    };
    assert_eq!(
        encode_tx_mutation(&absent).unwrap(),
        [2, 0, 0x00, 0xff, 0, 0]
    );
    let present = TxMutation {
        user_key: vec![0x00, 0xff],
        before_state: ValueState::Absent,
        after_state: ValueState::Present(pointer),
    };
    let mut present_golden = vec![2, 0, 0x00, 0xff, 0, 1];
    present_golden.extend_from_slice(&pointer.encode().unwrap());
    assert_eq!(encode_tx_mutation(&present).unwrap(), present_golden);

    let mut trailing = encode_tx_mutation(&absent).unwrap();
    trailing.push(0);
    assert_error_kind(decode_tx_mutation(&trailing), StorageErrorKind::Corruption);
    assert_error_kind(
        decode_tx_mutation(&[0, 0, 0, 0]),
        StorageErrorKind::Corruption,
    );
    assert_error_kind(
        decode_tx_mutation(&[1, 0, b'k', 2, 0]),
        StorageErrorKind::Corruption,
    );
    assert_error_kind(
        decode_tx_mutation(&[0xff, 0xff]),
        StorageErrorKind::Corruption,
    );
    assert_error_kind(
        encode_tx_mutation(&TxMutation {
            user_key: Vec::new(),
            before_state: ValueState::Absent,
            after_state: ValueState::Absent,
        }),
        StorageErrorKind::InvalidArgument,
    );
    assert_error_kind(
        encode_tx_mutation(&TxMutation {
            user_key: vec![0x00, 0xff],
            before_state: ValueState::Absent,
            after_state: ValueState::Present(sample_pointer(7, 64)),
        }),
        StorageErrorKind::InvalidArgument,
    );
    let mut mismatched_pointer_key_len = encode_tx_mutation(&present).unwrap();
    mismatched_pointer_key_len[16..20].copy_from_slice(&63_u32.to_le_bytes());
    assert_error_kind(
        decode_tx_mutation(&mismatched_pointer_key_len),
        StorageErrorKind::Corruption,
    );
    let mut unknown_pointer_version = encode_tx_mutation(&present).unwrap();
    unknown_pointer_version[6..8].copy_from_slice(&1_u16.to_le_bytes());
    assert_error_kind(
        decode_tx_mutation(&unknown_pointer_version),
        StorageErrorKind::IncompatibleFormat,
    );
}

#[test]
fn descriptor_crc_and_whole_descriptor_validation_are_strict() {
    let descriptor = sample_descriptor();
    let encoded = encode_descriptor(&descriptor).unwrap();
    let refs = mutation_refs(&encoded);
    let decoded = decode_descriptor(&encoded.meta_key, &encoded.meta_value, &refs).unwrap();
    assert_eq!(decoded.mutations, descriptor.mutations);
    assert_eq!(decoded.meta.commit_seq, descriptor.meta.commit_seq);
    assert_ne!(
        decoded.meta.descriptor_crc32c,
        descriptor.meta.descriptor_crc32c
    );

    let stored_crc = u32::from_le_bytes(encoded.meta_value[82..86].try_into().unwrap());
    let mut expected_crc = crc32c(b"RKDESC0");
    expected_crc = crc32c_append(expected_crc, &encoded.meta_value[0..82]);
    for mutation in &encoded.mutations {
        expected_crc = crc32c_append(expected_crc, &mutation.key);
        expected_crc = crc32c_append(
            expected_crc,
            &u32::try_from(mutation.value.len()).unwrap().to_le_bytes(),
        );
        expected_crc = crc32c_append(expected_crc, &mutation.value);
    }
    assert_eq!(stored_crc, expected_crc);
    assert_eq!(stored_crc, 0x1ba6_3062);

    assert_error_kind(
        decode_descriptor(
            &encoded.meta_key,
            &encoded.meta_value,
            &refs[..refs.len() - 1],
        ),
        StorageErrorKind::Corruption,
    );
    let wrong_meta_key = encode_tx_meta_key(1).unwrap();
    assert_error_kind(
        decode_descriptor(&wrong_meta_key, &encoded.meta_value, &refs),
        StorageErrorKind::Corruption,
    );
    let mut extra = refs.clone();
    extra.push(refs[0]);
    assert_error_kind(
        decode_descriptor(&encoded.meta_key, &encoded.meta_value, &extra),
        StorageErrorKind::Corruption,
    );

    let duplicate_ordinal = vec![refs[0], refs[0]];
    assert_error_kind(
        decode_descriptor(&encoded.meta_key, &encoded.meta_value, &duplicate_ordinal),
        StorageErrorKind::Corruption,
    );

    let mut missing_ordinal_key = encoded.mutations[1].key;
    missing_ordinal_key[11..19].copy_from_slice(&2_u64.to_be_bytes());
    let missing_ordinal = vec![
        refs[0],
        (
            missing_ordinal_key.as_slice(),
            encoded.mutations[1].value.as_slice(),
        ),
    ];
    assert_error_kind(
        decode_descriptor(&encoded.meta_key, &encoded.meta_value, &missing_ordinal),
        StorageErrorKind::Corruption,
    );

    let mut tampered = encoded.clone();
    tampered.mutations[0].value[2] ^= 1;
    let tampered_refs = mutation_refs(&tampered);
    assert_error_kind(
        decode_descriptor(&tampered.meta_key, &tampered.meta_value, &tampered_refs),
        StorageErrorKind::Corruption,
    );

    let mut duplicate_key = encoded.clone();
    duplicate_key.mutations[1].value = encode_tx_mutation(&TxMutation {
        user_key: descriptor.mutations[0].user_key.clone(),
        before_state: ValueState::Present(pointer_for_key(8, 128, 2, 5)),
        after_state: descriptor.mutations[1].after_state,
    })
    .unwrap();
    recompute_descriptor_crc(&mut duplicate_key.meta_value, &duplicate_key.mutations);
    let duplicate_key_refs = mutation_refs(&duplicate_key);
    assert_error_kind(
        decode_descriptor(
            &duplicate_key.meta_key,
            &duplicate_key.meta_value,
            &duplicate_key_refs,
        ),
        StorageErrorKind::Corruption,
    );

    let mut invalid_descriptor = sample_descriptor();
    invalid_descriptor.mutations[1].user_key = invalid_descriptor.mutations[0].user_key.clone();
    assert_error_kind(
        encode_descriptor(&invalid_descriptor),
        StorageErrorKind::InvalidArgument,
    );
}

#[test]
fn database_identity_v0_golden_crc_versions_and_cross_identity() {
    let identity = DatabaseIdentity {
        identity_format_version: 0,
        database_format_version: 0,
        database_uuid: sample_uuid(),
        keyspace_layout_version: 0,
    };
    let encoded = identity.encode().unwrap();
    let mut prefix = Vec::new();
    prefix.extend_from_slice(b"RKDI");
    prefix.extend_from_slice(&0_u16.to_le_bytes());
    prefix.extend_from_slice(&0_u32.to_le_bytes());
    prefix.extend_from_slice(&sample_uuid());
    prefix.extend_from_slice(&0_u16.to_le_bytes());
    assert_eq!(&encoded[0..28], prefix);
    assert_eq!(
        u32::from_le_bytes(encoded[28..32].try_into().unwrap()),
        crc32c(&prefix)
    );
    assert_eq!(
        u32::from_le_bytes(encoded[28..32].try_into().unwrap()),
        0x82bb_ca73
    );
    assert_eq!(DatabaseIdentity::decode(&encoded).unwrap(), identity);
    identity.validate_against(0, sample_uuid()).unwrap();

    assert_error_kind(
        DatabaseIdentity::decode(&encoded[..31]),
        StorageErrorKind::Corruption,
    );
    let mut too_long = encoded.to_vec();
    too_long.push(0);
    assert_error_kind(
        DatabaseIdentity::decode(&too_long),
        StorageErrorKind::Corruption,
    );
    let mut damaged = encoded;
    damaged[0] ^= 1;
    assert_error_kind(
        DatabaseIdentity::decode(&damaged),
        StorageErrorKind::Corruption,
    );
    let mut damaged = encoded;
    damaged[10] ^= 1;
    assert_error_kind(
        DatabaseIdentity::decode(&damaged),
        StorageErrorKind::Corruption,
    );
    let mut unknown_identity_version = encoded;
    unknown_identity_version[4..6].copy_from_slice(&1_u16.to_le_bytes());
    rewrite_crc(&mut unknown_identity_version, 28, 28);
    assert_error_kind(
        DatabaseIdentity::decode(&unknown_identity_version),
        StorageErrorKind::IncompatibleFormat,
    );
    let mut unknown_layout = encoded;
    unknown_layout[26..28].copy_from_slice(&1_u16.to_le_bytes());
    rewrite_crc(&mut unknown_layout, 28, 28);
    assert_error_kind(
        DatabaseIdentity::decode(&unknown_layout),
        StorageErrorKind::IncompatibleFormat,
    );
    let mut zero_uuid = encoded;
    zero_uuid[10..26].fill(0);
    rewrite_crc(&mut zero_uuid, 28, 28);
    assert_error_kind(
        DatabaseIdentity::decode(&zero_uuid),
        StorageErrorKind::Corruption,
    );
    assert_error_kind(
        identity.validate_against(1, sample_uuid()),
        StorageErrorKind::InvalidLayout,
    );
    assert_error_kind(
        identity.validate_against(0, [9; 16]),
        StorageErrorKind::InvalidLayout,
    );
}

#[test]
fn head_seq_is_exact_little_endian_and_capacity_checked() {
    assert_eq!(
        encode_head_seq(0x0102_0304_0506_0708),
        [8, 7, 6, 5, 4, 3, 2, 1]
    );
    assert_eq!(
        decode_head_seq(&[8, 7, 6, 5, 4, 3, 2, 1]).unwrap(),
        0x0102_0304_0506_0708
    );
    assert_error_kind(decode_head_seq(&[0; 7]), StorageErrorKind::Corruption);
    assert_error_kind(decode_head_seq(&[0; 9]), StorageErrorKind::Corruption);
    assert_eq!(next_commit_seq(0).unwrap(), 1);
    assert_error_kind(
        next_commit_seq(u64::MAX),
        StorageErrorKind::CapacityExceeded,
    );
}

#[test]
fn durable_frontier_v0_golden_crc_and_relations() {
    let empty = DurableFrontier {
        durable_seq: 0,
        durable_vlog_end: DurableVLogEnd::Empty,
    };
    let empty_encoded = empty.encode().unwrap();
    assert_eq!(&empty_encoded[0..4], b"RKDF");
    assert_eq!(&empty_encoded[4..6], &[0, 0]);
    assert_eq!(&empty_encoded[6..14], &[0; 8]);
    assert_eq!(empty_encoded[14], 0);
    assert_eq!(&empty_encoded[15..27], &[0; 12]);
    assert_eq!(
        u32::from_le_bytes(empty_encoded[27..31].try_into().unwrap()),
        crc32c(&empty_encoded[0..27])
    );
    assert_eq!(
        u32::from_le_bytes(empty_encoded[27..31].try_into().unwrap()),
        0xad40_0390
    );
    assert_eq!(DurableFrontier::decode(&empty_encoded).unwrap(), empty);

    let frontier = DurableFrontier {
        durable_seq: 5,
        durable_vlog_end: DurableVLogEnd::Position(VLogPos {
            file_id: 9,
            offset: 1_u64 << 32,
        }),
    };
    let encoded = frontier.encode().unwrap();
    assert_eq!(&encoded[0..4], b"RKDF");
    assert_eq!(&encoded[6..14], &5_u64.to_le_bytes());
    assert_eq!(encoded[14], 1);
    assert_eq!(&encoded[15..19], &9_u32.to_le_bytes());
    assert_eq!(&encoded[19..27], &(1_u64 << 32).to_le_bytes());
    assert_eq!(
        u32::from_le_bytes(encoded[27..31].try_into().unwrap()),
        0xc6e7_1855
    );
    assert_eq!(DurableFrontier::decode(&encoded).unwrap(), frontier);
    frontier.validate_against_head(5).unwrap();

    assert_error_kind(
        DurableFrontier::decode(&encoded[..30]),
        StorageErrorKind::Corruption,
    );
    let mut too_long = encoded.to_vec();
    too_long.push(0);
    assert_error_kind(
        DurableFrontier::decode(&too_long),
        StorageErrorKind::Corruption,
    );
    let mut damaged = encoded;
    damaged[27] ^= 1;
    assert_error_kind(
        DurableFrontier::decode(&damaged),
        StorageErrorKind::Corruption,
    );
    let mut damaged_magic = encoded;
    damaged_magic[0] ^= 1;
    assert_error_kind(
        DurableFrontier::decode(&damaged_magic),
        StorageErrorKind::Corruption,
    );
    let mut unknown_version = encoded;
    unknown_version[4..6].copy_from_slice(&1_u16.to_le_bytes());
    rewrite_crc(&mut unknown_version, 27, 27);
    assert_error_kind(
        DurableFrontier::decode(&unknown_version),
        StorageErrorKind::IncompatibleFormat,
    );
    let mut unknown_tag = encoded;
    unknown_tag[14] = 2;
    rewrite_crc(&mut unknown_tag, 27, 27);
    assert_error_kind(
        DurableFrontier::decode(&unknown_tag),
        StorageErrorKind::Corruption,
    );
    let mut nonzero_empty = empty_encoded;
    nonzero_empty[15] = 1;
    rewrite_crc(&mut nonzero_empty, 27, 27);
    assert_error_kind(
        DurableFrontier::decode(&nonzero_empty),
        StorageErrorKind::Corruption,
    );
    assert_error_kind(
        DurableFrontier {
            durable_seq: 0,
            durable_vlog_end: frontier.durable_vlog_end,
        }
        .encode(),
        StorageErrorKind::InvalidArgument,
    );
    assert_error_kind(
        frontier.validate_against_head(4),
        StorageErrorKind::Corruption,
    );
}

#[test]
fn recovery_state_v0_golden_crc_and_phase_invariants() {
    let state = RecoveryState {
        phase: RecoveryPhase::Undo,
        original_head: 10,
        target_seq: 5,
        target_vlog_end: DurableVLogEnd::Position(VLogPos {
            file_id: 7,
            offset: 4096,
        }),
        next_undo_seq: 10,
        trim_required: true,
    };
    let encoded = state.encode().unwrap();
    assert_eq!(&encoded[0..4], b"RKRS");
    assert_eq!(&encoded[4..6], &[0, 0]);
    assert_eq!(encoded[6], 1);
    assert_eq!(&encoded[7..15], &10_u64.to_le_bytes());
    assert_eq!(&encoded[15..23], &5_u64.to_le_bytes());
    assert_eq!(encoded[23], 1);
    assert_eq!(&encoded[24..28], &7_u32.to_le_bytes());
    assert_eq!(&encoded[28..36], &4096_u64.to_le_bytes());
    assert_eq!(&encoded[36..44], &10_u64.to_le_bytes());
    assert_eq!(encoded[44], 1);
    assert_eq!(
        u32::from_le_bytes(encoded[45..49].try_into().unwrap()),
        crc32c(&encoded[0..45])
    );
    assert_eq!(
        u32::from_le_bytes(encoded[45..49].try_into().unwrap()),
        0x6f7f_61f2
    );
    assert_eq!(RecoveryState::decode(&encoded).unwrap(), state);

    for valid in [
        RecoveryState {
            phase: RecoveryPhase::Trim,
            next_undo_seq: 5,
            ..state
        },
        RecoveryState {
            phase: RecoveryPhase::Finalize,
            next_undo_seq: 5,
            trim_required: false,
            ..state
        },
        RecoveryState {
            phase: RecoveryPhase::Undo,
            original_head: 0,
            target_seq: 0,
            target_vlog_end: DurableVLogEnd::Empty,
            next_undo_seq: 0,
            trim_required: true,
        },
    ] {
        let encoded = valid.encode().unwrap();
        assert_eq!(RecoveryState::decode(&encoded).unwrap(), valid);
    }

    assert_error_kind(
        RecoveryState::decode(&encoded[..48]),
        StorageErrorKind::Corruption,
    );
    let mut too_long = encoded.to_vec();
    too_long.push(0);
    assert_error_kind(
        RecoveryState::decode(&too_long),
        StorageErrorKind::Corruption,
    );
    let mut damaged = encoded;
    damaged[45] ^= 1;
    assert_error_kind(
        RecoveryState::decode(&damaged),
        StorageErrorKind::Corruption,
    );
    let mut damaged_magic = encoded;
    damaged_magic[0] ^= 1;
    assert_error_kind(
        RecoveryState::decode(&damaged_magic),
        StorageErrorKind::Corruption,
    );
    let mut unknown_version = encoded;
    unknown_version[4..6].copy_from_slice(&1_u16.to_le_bytes());
    rewrite_crc(&mut unknown_version, 45, 45);
    assert_error_kind(
        RecoveryState::decode(&unknown_version),
        StorageErrorKind::IncompatibleFormat,
    );
    for (offset, value) in [(6, 0), (6, 4), (44, 2)] {
        let mut damaged = encoded;
        damaged[offset] = value;
        rewrite_crc(&mut damaged, 45, 45);
        assert_error_kind(
            RecoveryState::decode(&damaged),
            StorageErrorKind::Corruption,
        );
    }
    let mut unknown_end_tag = encoded;
    unknown_end_tag[23] = 2;
    rewrite_crc(&mut unknown_end_tag, 45, 45);
    assert_error_kind(
        RecoveryState::decode(&unknown_end_tag),
        StorageErrorKind::Corruption,
    );

    for invalid in [
        RecoveryState {
            target_seq: 11,
            ..state
        },
        RecoveryState {
            next_undo_seq: 11,
            ..state
        },
        RecoveryState {
            target_seq: 0,
            ..state
        },
        RecoveryState {
            phase: RecoveryPhase::Trim,
            next_undo_seq: 5,
            trim_required: false,
            ..state
        },
        RecoveryState {
            phase: RecoveryPhase::Finalize,
            next_undo_seq: 5,
            trim_required: true,
            ..state
        },
    ] {
        assert_error_kind(invalid.encode(), StorageErrorKind::InvalidArgument);
    }
}

#[test]
fn malformed_metadata_lengths_never_panic_or_allocate_from_untrusted_counts() {
    for len in 0..100 {
        let bytes = vec![0xff; len];
        let _ = FormatMetadataV0::decode(&bytes);
        let _ = ValuePointer::decode(&bytes);
        let _ = decode_tx_meta_key(&bytes);
        let _ = decode_tx_mutation_key(&bytes);
        let _ = decode_tx_meta(&bytes);
        let _ = decode_tx_mutation(&bytes);
        let _ = DatabaseIdentity::decode(&bytes);
        let _ = decode_head_seq(&bytes);
        let _ = DurableFrontier::decode(&bytes);
        let _ = RecoveryState::decode(&bytes);
    }

    assert_error_kind(
        decode_tx_mutation(&[0xff, 0xff, 0, 1, 2, 3]),
        StorageErrorKind::Corruption,
    );
}
