#![allow(dead_code)]

#[path = "../src/error.rs"]
mod error;

pub(crate) use error::{
    Operation, ProtocolStage, Result, RetryAdvice, StorageError, StorageErrorKind, WriteOutcome,
};

#[path = "../src/vlog/format.rs"]
mod vlog_format;

use crc32c::{crc32c, crc32c_append};
use vlog_format::*;

fn tx_uuid() -> [u8; 16] {
    [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
}

fn database_uuid() -> [u8; 16] {
    [16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
}

fn assert_kind<T: std::fmt::Debug>(result: Result<T>, expected: StorageErrorKind) {
    assert_eq!(result.unwrap_err().kind, expected);
}

fn write_header_crc(bytes: &mut [u8]) {
    let checksum = crc32c(&bytes[0..35]);
    bytes[35..39].copy_from_slice(&checksum.to_le_bytes());
}

fn write_standard_crc(bytes: &mut [u8]) {
    let crc_offset = bytes.len() - 4;
    let checksum = crc32c(&bytes[0..crc_offset]);
    bytes[crc_offset..].copy_from_slice(&checksum.to_le_bytes());
}

fn write_footer_crc(bytes: &mut [u8]) {
    let checksum = crc32c_append(crc32c(&bytes[0..99]), &bytes[103..111]);
    bytes[99..103].copy_from_slice(&checksum.to_le_bytes());
}

fn scan(envelope: &PreparedEnvelope, geometry: VLogGeometry) -> Result<ScannedEnvelope> {
    scan_prepared_envelope(
        &envelope.chunks,
        geometry,
        database_uuid(),
        envelope.vlog_begin,
        envelope.vlog_end,
        Some(envelope.envelope_crc32c),
    )
}

fn single_delete_envelope_at(
    geometry: VLogGeometry,
    vlog_begin: VLogPosition,
    commit_seq: u64,
) -> PreparedEnvelope {
    let begin = TxBegin {
        commit_seq,
        tx_uuid: tx_uuid(),
        vlog_begin,
        logical_op_count: 1,
        distinct_key_count: 1,
    };
    let delete = DeleteRecordRef {
        commit_seq,
        tx_uuid: tx_uuid(),
        op_index: 0,
        key: b"x",
    };
    let delete_start = VLogPosition {
        file_id: vlog_begin.file_id,
        offset: vlog_begin.offset + u64::from(TX_BEGIN_ENCODED_LEN),
    };
    let footer_start = VLogPosition {
        file_id: vlog_begin.file_id,
        offset: delete_start.offset + u64::from(MIN_DELETE_RECORD_LEN),
    };
    let vlog_end = VLogPosition {
        file_id: vlog_begin.file_id,
        offset: footer_start.offset + u64::from(TX_PREPARED_END_ENCODED_LEN),
    };
    assert!(vlog_end.offset <= geometry.max_file_size);

    let mut footer = TxPreparedEnd {
        commit_seq,
        tx_uuid: tx_uuid(),
        vlog_begin,
        vlog_end,
        logical_op_count: 1,
        distinct_key_count: 1,
        kv_record_count: 0,
        delete_record_count: 1,
        envelope_crc32c: 0,
    };
    let mut envelope_crc = EnvelopeCrc32c::new();
    envelope_crc.update_tx_begin(begin).unwrap();
    envelope_crc.update_delete_record(delete).unwrap();
    footer.envelope_crc32c = envelope_crc.finish_with_footer(footer).unwrap();

    PreparedEnvelope {
        commit_seq,
        tx_uuid: tx_uuid(),
        vlog_begin,
        vlog_end,
        envelope_crc32c: footer.envelope_crc32c,
        chunks: vec![
            PhysicalChunk {
                position: vlog_begin,
                bytes: encode_tx_begin(begin).unwrap(),
            },
            PhysicalChunk {
                position: delete_start,
                bytes: encode_delete_record(delete).unwrap(),
            },
            PhysicalChunk {
                position: footer_start,
                bytes: encode_tx_prepared_end(footer).unwrap(),
            },
        ],
        value_pointers: vec![None],
    }
}

fn record_chunk_index(
    envelope: &PreparedEnvelope,
    record_type: RecordType,
    ordinal: usize,
) -> usize {
    envelope
        .chunks
        .iter()
        .enumerate()
        .filter_map(|(index, chunk)| {
            RecordHeader::decode(&chunk.bytes)
                .ok()
                .filter(|header| header.record_type == record_type)
                .map(|_| index)
        })
        .nth(ordinal)
        .unwrap()
}

#[test]
fn maximum_kv_record_fits_first_and_later_pages() {
    assert_eq!(VLOG_PAGE_SIZE, 65_536);
    assert_eq!(MAX_VLOG_FILE_SIZE, 1_u64 << 32);
    let value = vec![0xff; 59_999];
    let encoded = encode_kv_record(KvRecordRef {
        commit_seq: 1,
        tx_uuid: tx_uuid(),
        op_index: 0,
        key: &[0],
        value: &value,
    })
    .unwrap();
    assert_eq!(encoded.len(), MAX_KV_RECORD_LEN as usize);

    let mut first_page = LayoutPlanner::empty(VLogGeometry::PRODUCTION).unwrap();
    let placement = first_page.plan_record(MAX_KV_RECORD_LEN).unwrap();
    assert_eq!(placement.record_start.offset, 64);
    assert_eq!(placement.preludes.len(), 2);
    assert_eq!(first_page.position().offset, 60_119);
    assert_eq!(65_536 - first_page.position().offset, 5_417);
    assert!(decode_record_at(&encoded, placement.record_start, VLogGeometry::PRODUCTION).is_ok());

    let mut later_page = LayoutPlanner::from_position(
        VLogGeometry::PRODUCTION,
        VLogPosition {
            file_id: 0,
            offset: 65_536,
        },
    )
    .unwrap();
    let placement = later_page.plan_record(MAX_KV_RECORD_LEN).unwrap();
    assert_eq!(placement.record_start.offset, 65_536 + 16);
    assert_eq!(placement.preludes.len(), 1);
    assert_eq!(131_072 - later_page.position().offset, 5_465);
    assert!(decode_record_at(&encoded, placement.record_start, VLogGeometry::PRODUCTION).is_ok());
}

#[test]
fn page_tail_remainders_1_42_and_43_follow_the_six_step_rule() {
    for (tail_after_record, expected_page_end) in [(1_u64, 72_u32), (42, 113)] {
        let start = 65_536 - u64::from(TX_BEGIN_ENCODED_LEN) - tail_after_record;
        let mut planner = LayoutPlanner::from_position(
            VLogGeometry::PRODUCTION,
            VLogPosition {
                file_id: 0,
                offset: start,
            },
        )
        .unwrap();
        let placement = planner.plan_record(TX_BEGIN_ENCODED_LEN).unwrap();
        assert_eq!(
            placement.preludes[0],
            LayoutPrelude::PageEnd {
                position: VLogPosition {
                    file_id: 0,
                    offset: start,
                },
                encoded_len: expected_page_end,
            }
        );
        assert_eq!(
            placement.preludes[1],
            LayoutPrelude::PageHeader {
                position: VLogPosition {
                    file_id: 0,
                    offset: 65_536,
                },
                page_no: 1,
            }
        );
        assert_eq!(placement.record_start.offset, 65_536 + 16);
    }

    let start = 65_536 - u64::from(TX_BEGIN_ENCODED_LEN) - 43;
    let mut planner = LayoutPlanner::from_position(
        VLogGeometry::PRODUCTION,
        VLogPosition {
            file_id: 0,
            offset: start,
        },
    )
    .unwrap();
    let placement = planner.plan_record(TX_BEGIN_ENCODED_LEN).unwrap();
    assert!(placement.preludes.is_empty());
    assert_eq!(65_536 - planner.position().offset, 43);
}

#[test]
fn scanner_enforces_the_same_page_tail_invariant_as_the_planner() {
    let geometry = VLogGeometry::test_only(512, 2_048, 2).unwrap();
    let envelope_len =
        u64::from(TX_BEGIN_ENCODED_LEN + MIN_DELETE_RECORD_LEN + TX_PREPARED_END_ENCODED_LEN);

    for (commit_seq, remaining, accepted) in [(20, 1_u64, false), (21, 42, false), (22, 43, true)] {
        let vlog_end_offset = geometry.page_size - remaining;
        let envelope = single_delete_envelope_at(
            geometry,
            VLogPosition {
                file_id: 0,
                offset: vlog_end_offset - envelope_len,
            },
            commit_seq,
        );
        let result = scan(&envelope, geometry);
        if accepted {
            assert!(result.is_ok(), "remaining={remaining}: {result:?}");
        } else {
            assert_kind(result, StorageErrorKind::Corruption);
        }
    }
}

#[test]
fn records_can_end_exactly_at_page_and_4gib_exclusive_boundaries() {
    let page_start = 65_536 - u64::from(TX_BEGIN_ENCODED_LEN);
    let mut page_planner = LayoutPlanner::from_position(
        VLogGeometry::PRODUCTION,
        VLogPosition {
            file_id: 8,
            offset: page_start,
        },
    )
    .unwrap();
    let placement = page_planner.plan_record(TX_BEGIN_ENCODED_LEN).unwrap();
    assert!(placement.preludes.is_empty());
    assert_eq!(page_planner.position().offset, 65_536);

    let file_start = MAX_VLOG_FILE_SIZE - u64::from(TX_PREPARED_END_ENCODED_LEN);
    let mut file_planner = LayoutPlanner::from_position(
        VLogGeometry::PRODUCTION,
        VLogPosition {
            file_id: 8,
            offset: file_start,
        },
    )
    .unwrap();
    let placement = file_planner
        .plan_record(TX_PREPARED_END_ENCODED_LEN)
        .unwrap();
    assert!(placement.preludes.is_empty());
    assert_eq!(file_planner.position().offset, MAX_VLOG_FILE_SIZE);

    let next = file_planner.plan_record(TX_BEGIN_ENCODED_LEN).unwrap();
    assert_eq!(next.record_start.file_id, 9);
    assert_eq!(next.record_start.offset, 64);
    assert_eq!(next.preludes.len(), 2);
}

#[test]
fn file_id_999999_exhaustion_is_capacity_error_and_does_not_advance() {
    let start = VLogPosition {
        file_id: 999_999,
        offset: MAX_VLOG_FILE_SIZE - 43,
    };
    let mut planner = LayoutPlanner::from_position(VLogGeometry::PRODUCTION, start).unwrap();
    assert_kind(
        planner.plan_record(TX_BEGIN_ENCODED_LEN),
        StorageErrorKind::CapacityExceeded,
    );
    assert_eq!(planner.position(), start);
}

#[test]
fn prepared_envelope_crosses_page_and_scans_to_exact_footer_end() {
    let geometry = VLogGeometry::test_only(256, 1_024, 4).unwrap();
    let mut planner = LayoutPlanner::empty(geometry).unwrap();
    let operations = [LogicalOperationRef::Put {
        key: b"k",
        value: b"v",
    }];
    let envelope =
        prepare_envelope(&mut planner, database_uuid(), 1, tx_uuid(), &operations).unwrap();

    assert_eq!(
        envelope.vlog_begin,
        VLogPosition {
            file_id: 0,
            offset: 0
        }
    );
    assert_eq!(planner.position(), envelope.vlog_end);
    assert!(envelope.chunks.iter().any(|chunk| {
        RecordHeader::decode(&chunk.bytes)
            .is_ok_and(|header| header.record_type == RecordType::PageEnd)
    }));
    let scanned = scan(&envelope, geometry).unwrap();
    assert_eq!(scanned.logical_op_count, 1);
    assert_eq!(scanned.distinct_key_count, 1);
    assert_eq!(scanned.kv_record_count, 1);
    assert_eq!(scanned.delete_record_count, 0);
    assert_eq!(scanned.vlog_end, envelope.vlog_end);

    let footer_index = record_chunk_index(&envelope, RecordType::TxPreparedEnd, 0);
    let footer_chunk = &envelope.chunks[footer_index];
    let located = locate_footer_from_end(
        footer_chunk.position,
        &footer_chunk.bytes,
        envelope.vlog_end,
        geometry,
    )
    .unwrap();
    assert_eq!(located.record_start, footer_chunk.position);
    assert_eq!(located.footer.envelope_crc32c, envelope.envelope_crc32c);
}

#[test]
fn prepared_envelope_crosses_files_with_headers_inside_vlog_interval() {
    let geometry = VLogGeometry::test_only(256, 512, 3).unwrap();
    let mut planner = LayoutPlanner::empty(geometry).unwrap();
    let value = [0x5a; 40];
    let operations = [
        LogicalOperationRef::Put {
            key: b"k0",
            value: &value,
        },
        LogicalOperationRef::Put {
            key: b"k1",
            value: &value,
        },
        LogicalOperationRef::Put {
            key: b"k2",
            value: &value,
        },
        LogicalOperationRef::Put {
            key: b"k3",
            value: &value,
        },
    ];
    let envelope =
        prepare_envelope(&mut planner, database_uuid(), 2, tx_uuid(), &operations).unwrap();
    assert!(envelope.vlog_end.file_id >= 1);
    assert!(envelope.chunks.iter().any(|chunk| {
        chunk.position.file_id == 1
            && chunk.position.offset == 0
            && PageHeader::decode(&chunk.bytes).is_ok()
    }));
    assert!(envelope.chunks.iter().any(|chunk| {
        chunk.position.file_id == 1
            && chunk.position.offset == 16
            && VLogFileHeader::decode(&chunk.bytes).is_ok()
    }));
    let scanned = scan(&envelope, geometry).unwrap();
    assert_eq!(scanned.logical_op_count, 4);
    assert_eq!(scanned.distinct_key_count, 4);
    assert_eq!(envelope.value_pointers.len(), 4);
    assert!(envelope.value_pointers.iter().all(Option::is_some));
}

#[test]
fn envelope_starting_at_file_limit_rolls_and_scans_from_the_next_file() {
    let geometry = VLogGeometry::test_only(256, 512, 3).unwrap();
    let vlog_begin = VLogPosition {
        file_id: 0,
        offset: geometry.max_file_size,
    };
    let mut planner = LayoutPlanner::from_position(geometry, vlog_begin).unwrap();
    let operations = [LogicalOperationRef::Delete { key: b"gone" }];

    let envelope =
        prepare_envelope(&mut planner, database_uuid(), 8, tx_uuid(), &operations).unwrap();

    assert_eq!(envelope.vlog_begin, vlog_begin);
    assert_eq!(
        envelope.chunks.first().unwrap().position,
        VLogPosition {
            file_id: 1,
            offset: 0,
        }
    );
    assert!(PageHeader::decode(&envelope.chunks[0].bytes).is_ok());
    assert_eq!(scan(&envelope, geometry).unwrap().vlog_begin, vlog_begin);
}

#[test]
fn vlog_begin_includes_page_end_and_new_page_headers() {
    let geometry = VLogGeometry::test_only(256, 1_024, 2).unwrap();
    let start = VLogPosition {
        file_id: 0,
        offset: 256 - 80,
    };
    let mut planner = LayoutPlanner::from_position(geometry, start).unwrap();
    let operations = [LogicalOperationRef::Delete { key: b"gone" }];
    let envelope =
        prepare_envelope(&mut planner, database_uuid(), 3, tx_uuid(), &operations).unwrap();
    assert_eq!(envelope.vlog_begin, start);
    assert_eq!(envelope.chunks[0].position, start);
    assert_eq!(
        RecordHeader::decode(&envelope.chunks[0].bytes)
            .unwrap()
            .record_type,
        RecordType::PageEnd
    );
    assert_eq!(envelope.chunks[1].position.offset, 256);
    scan(&envelope, geometry).unwrap();
}

#[test]
fn scanner_rejects_missing_duplicate_and_out_of_order_op_index() {
    let geometry = VLogGeometry::test_only(512, 2_048, 2).unwrap();
    let operations = [
        LogicalOperationRef::Put {
            key: b"a",
            value: b"1",
        },
        LogicalOperationRef::Delete { key: b"b" },
    ];
    let mut planner = LayoutPlanner::empty(geometry).unwrap();
    let envelope =
        prepare_envelope(&mut planner, database_uuid(), 4, tx_uuid(), &operations).unwrap();

    let first_op = record_chunk_index(&envelope, RecordType::KvRecord, 0);
    let second_op = record_chunk_index(&envelope, RecordType::DeleteRecord, 0);

    let mut missing = envelope.clone();
    missing.chunks.remove(first_op);
    assert_kind(scan(&missing, geometry), StorageErrorKind::Corruption);

    let mut duplicate = envelope.clone();
    duplicate.chunks[second_op].bytes[39..47].copy_from_slice(&0_u64.to_le_bytes());
    write_standard_crc(&mut duplicate.chunks[second_op].bytes);
    assert_kind(scan(&duplicate, geometry), StorageErrorKind::Corruption);

    let mut out_of_order = envelope;
    out_of_order.chunks[first_op].bytes[39..47].copy_from_slice(&1_u64.to_le_bytes());
    write_standard_crc(&mut out_of_order.chunks[first_op].bytes);
    assert_kind(scan(&out_of_order, geometry), StorageErrorKind::Corruption);
}

#[test]
fn scanner_rejects_begin_footer_counts_identity_and_envelope_crc_mismatch() {
    let geometry = VLogGeometry::test_only(512, 2_048, 2).unwrap();
    let operations = [
        LogicalOperationRef::Put {
            key: b"a",
            value: b"1",
        },
        LogicalOperationRef::Delete { key: b"b" },
    ];
    let mut planner = LayoutPlanner::empty(geometry).unwrap();
    let envelope =
        prepare_envelope(&mut planner, database_uuid(), 5, tx_uuid(), &operations).unwrap();
    let begin_index = record_chunk_index(&envelope, RecordType::TxBegin, 0);
    let kv_index = record_chunk_index(&envelope, RecordType::KvRecord, 0);
    let footer_index = record_chunk_index(&envelope, RecordType::TxPreparedEnd, 0);

    let mut begin_count = envelope.clone();
    begin_count.chunks[begin_index].bytes[51..59].copy_from_slice(&3_u64.to_le_bytes());
    write_standard_crc(&mut begin_count.chunks[begin_index].bytes);
    assert_kind(scan(&begin_count, geometry), StorageErrorKind::Corruption);

    let mut footer_counts = envelope.clone();
    footer_counts.chunks[footer_index].bytes[79..87].copy_from_slice(&0_u64.to_le_bytes());
    footer_counts.chunks[footer_index].bytes[87..95].copy_from_slice(&2_u64.to_le_bytes());
    write_footer_crc(&mut footer_counts.chunks[footer_index].bytes);
    assert_kind(scan(&footer_counts, geometry), StorageErrorKind::Corruption);

    let mut wrong_identity = envelope.clone();
    wrong_identity.chunks[kv_index].bytes[19] ^= 1;
    write_header_crc(&mut wrong_identity.chunks[kv_index].bytes);
    write_standard_crc(&mut wrong_identity.chunks[kv_index].bytes);
    assert_kind(
        scan(&wrong_identity, geometry),
        StorageErrorKind::Corruption,
    );

    let mut logical_tamper = envelope;
    logical_tamper.chunks[kv_index].bytes[51] ^= 1;
    write_standard_crc(&mut logical_tamper.chunks[kv_index].bytes);
    assert_kind(
        scan(&logical_tamper, geometry),
        StorageErrorKind::Corruption,
    );
}

#[test]
fn scanner_rejects_page_end_and_file_header_corruption() {
    let geometry = VLogGeometry::test_only(256, 512, 3).unwrap();
    let value = [0x33; 40];
    let operations = [
        LogicalOperationRef::Put {
            key: b"k0",
            value: &value,
        },
        LogicalOperationRef::Put {
            key: b"k1",
            value: &value,
        },
        LogicalOperationRef::Put {
            key: b"k2",
            value: &value,
        },
    ];
    let mut planner = LayoutPlanner::empty(geometry).unwrap();
    let envelope =
        prepare_envelope(&mut planner, database_uuid(), 6, tx_uuid(), &operations).unwrap();
    let page_end_index = record_chunk_index(&envelope, RecordType::PageEnd, 0);
    let file_header_index = envelope
        .chunks
        .iter()
        .position(|chunk| chunk.position.file_id == 1 && chunk.position.offset == 16)
        .unwrap();

    let mut nonzero_padding = envelope.clone();
    nonzero_padding.chunks[page_end_index].bytes[39] = 1;
    write_standard_crc(&mut nonzero_padding.chunks[page_end_index].bytes);
    assert_kind(
        scan(&nonzero_padding, geometry),
        StorageErrorKind::Corruption,
    );

    let mut page_end_identity = envelope.clone();
    page_end_identity.chunks[page_end_index].bytes[19] ^= 1;
    write_header_crc(&mut page_end_identity.chunks[page_end_index].bytes);
    write_standard_crc(&mut page_end_identity.chunks[page_end_index].bytes);
    assert_kind(
        scan(&page_end_identity, geometry),
        StorageErrorKind::Corruption,
    );

    let mut file_identity = envelope;
    file_identity.chunks[file_header_index].bytes[12] ^= 1;
    let checksum = crc32c(&file_identity.chunks[file_header_index].bytes[0..44]);
    file_identity.chunks[file_header_index].bytes[44..48].copy_from_slice(&checksum.to_le_bytes());
    assert_kind(scan(&file_identity, geometry), StorageErrorKind::Corruption);
}

#[test]
fn scanner_explicitly_rejects_bare_zero_truncated_and_consecutive_page_end() {
    let geometry = VLogGeometry::test_only(256, 1_024, 3).unwrap();
    let value = [0x44; 40];
    let operations = [
        LogicalOperationRef::Put {
            key: b"k0",
            value: &value,
        },
        LogicalOperationRef::Put {
            key: b"k1",
            value: &value,
        },
    ];
    let mut planner = LayoutPlanner::empty(geometry).unwrap();
    let envelope =
        prepare_envelope(&mut planner, database_uuid(), 9, tx_uuid(), &operations).unwrap();
    let page_end_index = record_chunk_index(&envelope, RecordType::PageEnd, 0);

    let mut bare_zero = envelope.clone();
    bare_zero.chunks[page_end_index].bytes.fill(0);
    assert_kind(scan(&bare_zero, geometry), StorageErrorKind::Corruption);

    let mut truncated = envelope.clone();
    truncated.chunks[page_end_index].bytes.pop();
    assert_kind(scan(&truncated, geometry), StorageErrorKind::Corruption);

    let mut consecutive = envelope;
    let next_record_index = consecutive
        .chunks
        .iter()
        .enumerate()
        .skip(page_end_index + 1)
        .find_map(|(index, chunk)| RecordHeader::decode(&chunk.bytes).ok().map(|_| index))
        .unwrap();
    let second_page_end_position = consecutive.chunks[next_record_index].position;
    consecutive.chunks[next_record_index].bytes =
        encode_page_end(9, tx_uuid(), second_page_end_position, geometry).unwrap();
    assert_kind(scan(&consecutive, geometry), StorageErrorKind::Corruption);
}

#[test]
fn reverse_footer_and_forward_scan_reject_wrong_exact_endpoint() {
    let geometry = VLogGeometry::test_only(512, 2_048, 2).unwrap();
    let operations = [LogicalOperationRef::Delete { key: b"x" }];
    let mut planner = LayoutPlanner::empty(geometry).unwrap();
    let envelope =
        prepare_envelope(&mut planner, database_uuid(), 7, tx_uuid(), &operations).unwrap();
    let footer_index = record_chunk_index(&envelope, RecordType::TxPreparedEnd, 0);
    let footer_chunk = &envelope.chunks[footer_index];

    let wrong_end = VLogPosition {
        file_id: envelope.vlog_end.file_id,
        offset: envelope.vlog_end.offset - 1,
    };
    assert_kind(
        locate_footer_from_end(
            footer_chunk.position,
            &footer_chunk.bytes,
            wrong_end,
            geometry,
        ),
        StorageErrorKind::Corruption,
    );
    assert_kind(
        scan_prepared_envelope(
            &envelope.chunks,
            geometry,
            database_uuid(),
            envelope.vlog_begin,
            wrong_end,
            Some(envelope.envelope_crc32c),
        ),
        StorageErrorKind::Corruption,
    );

    let mut bad_trailer = footer_chunk.bytes.clone();
    bad_trailer[110] ^= 1;
    assert_kind(
        locate_footer_from_end(
            footer_chunk.position,
            &bad_trailer,
            envelope.vlog_end,
            geometry,
        ),
        StorageErrorKind::Corruption,
    );
}
