//! Systematic write-position coverage for the vlog layout planner and envelope builder.
//!
//! Every scenario is expressed in geometry-relative coordinates (P = page_size,
//! F = max_file_size, M = max_file_id) and executed against three small, fast
//! geometries:
//!   * (page 4096, file 16384, max_file_id 4)
//!   * (page 512,  file 2048,  max_file_id 2)
//!   * (page 1024, file 8192,  max_file_id 3)
//!
//! `ensure_record_area` (private) is observed through the `preludes` that
//! `plan_record` emits; `append_placement` (private) is observed through the
//! chunk stream `prepare_envelope` produces, verified by contiguity and a full
//! `scan_prepared_envelope` round-trip.

#![allow(dead_code)]

#[path = "../src/error.rs"]
mod error;

pub(crate) use error::{
    Operation, ProtocolStage, Result, RetryAdvice, StorageError, StorageErrorKind, WriteOutcome,
};

#[path = "../src/vlog/format.rs"]
mod vlog_format;

use vlog_format::*;

// ---------- helpers ----------

const GEOMETRIES: [(u64, u64, u32); 3] = [(4096, 16_384, 4), (512, 2_048, 2), (1_024, 8_192, 3)];

fn tx_uuid() -> [u8; 16] {
    [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
}

fn database_uuid() -> [u8; 16] {
    [16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
}

fn pos(file_id: u32, offset: u64) -> VLogPosition {
    VLogPosition { file_id, offset }
}

fn assert_kind<T: std::fmt::Debug>(result: Result<T>, expected: StorageErrorKind) {
    assert_eq!(result.unwrap_err().kind, expected);
}

fn for_each_geometry<F: Fn(VLogGeometry)>(f: F) {
    for (page_size, max_file_size, max_file_id) in GEOMETRIES {
        let geometry = VLogGeometry::test_only(page_size, max_file_size, max_file_id)
            .unwrap_or_else(|_| {
                panic!("invalid geometry {page_size}/{max_file_size}/{max_file_id}")
            });
        f(geometry);
    }
}

/// Mirrors `next_chunk_position` in format.rs: an end exactly at `max_file_size`
/// rolls to the next file; any other end is its own start.
fn next_chunk_pos(previous_end: VLogPosition, geometry: VLogGeometry) -> VLogPosition {
    if previous_end.offset == geometry.max_file_size {
        VLogPosition {
            file_id: previous_end.file_id + 1,
            offset: 0,
        }
    } else {
        previous_end
    }
}

fn chunk_end(chunk: &PhysicalChunk) -> VLogPosition {
    VLogPosition {
        file_id: chunk.position.file_id,
        offset: chunk.position.offset + chunk.bytes.len() as u64,
    }
}

/// Every chunk must start where the previous one ends (rolling files at the
/// file boundary). The first chunk follows `next_chunk_pos(vlog_begin)`, exactly
/// like `scan_prepared_envelope` computes `first_chunk_position`. Returns the end
/// of the last chunk.
fn assert_contiguous_chunks(envelope: &PreparedEnvelope, geometry: VLogGeometry) -> VLogPosition {
    assert!(!envelope.chunks.is_empty(), "envelope has no chunks");
    assert_eq!(
        envelope.chunks[0].position,
        next_chunk_pos(envelope.vlog_begin, geometry),
        "first chunk does not follow vlog_begin"
    );
    for window in envelope.chunks.windows(2) {
        let expected = next_chunk_pos(chunk_end(&window[0]), geometry);
        assert_eq!(window[1].position, expected, "chunks are not contiguous");
    }
    chunk_end(envelope.chunks.last().unwrap())
}

fn scan_ok(envelope: &PreparedEnvelope, geometry: VLogGeometry) -> ScannedEnvelope {
    scan_prepared_envelope(
        &envelope.chunks,
        geometry,
        database_uuid(),
        envelope.vlog_begin,
        envelope.vlog_end,
        Some(envelope.envelope_crc32c),
    )
    .unwrap()
}

/// Universal invariants every prepared envelope must satisfy; `expected` is
/// (logical_op_count, kv_record_count, delete_record_count, distinct_key_count).
fn assert_envelope_invariants(
    envelope: &PreparedEnvelope,
    geometry: VLogGeometry,
    expected: (u64, u64, u64, u64),
) {
    let end = assert_contiguous_chunks(envelope, geometry);
    assert_eq!(end, envelope.vlog_end, "chunk stream end != vlog_end");
    let scanned = scan_ok(envelope, geometry);
    assert_eq!(scanned.logical_op_count, expected.0);
    assert_eq!(scanned.kv_record_count, expected.1);
    assert_eq!(scanned.delete_record_count, expected.2);
    assert_eq!(scanned.distinct_key_count, expected.3);
    assert_eq!(scanned.vlog_begin, envelope.vlog_begin);
    assert_eq!(scanned.vlog_end, envelope.vlog_end);
    assert_eq!(scanned.envelope_crc32c, envelope.envelope_crc32c);
}

fn record_type(bytes: &[u8]) -> Option<RecordType> {
    RecordHeader::decode(bytes)
        .ok()
        .map(|header| header.record_type)
}

fn record_positions(envelope: &PreparedEnvelope, which: RecordType) -> Vec<VLogPosition> {
    envelope
        .chunks
        .iter()
        .filter(|chunk| record_type(&chunk.bytes) == Some(which))
        .map(|chunk| chunk.position)
        .collect()
}

#[allow(clippy::result_large_err)] // PreparedEnvelope carries Vec fields; fine for a test helper
fn prepare(
    planner: &mut LayoutPlanner,
    commit_seq: u64,
    operations: &[LogicalOperationRef<'_>],
) -> Result<PreparedEnvelope> {
    prepare_envelope(planner, database_uuid(), commit_seq, tx_uuid(), operations)
}

fn delete_op<'a>(key: &'a [u8]) -> LogicalOperationRef<'a> {
    LogicalOperationRef::Delete { key }
}

fn put_op<'a>(key: &'a [u8], value: &'a [u8]) -> LogicalOperationRef<'a> {
    LogicalOperationRef::Put { key, value }
}

// ====================================================================
// Test 1: plan_record produces the correct RecordPlacement at every
// write position; the preludes directly expose ensure_record_area.
// ====================================================================

/// A1: fresh file (offset 0) — ensure_record_area must emit PageHeader+FileHeader.
#[test]
fn fresh_file_emits_page_and_file_headers() {
    for_each_geometry(|g| {
        let mut planner = LayoutPlanner::empty(g).unwrap();
        let placement = planner.plan_record(TX_BEGIN_ENCODED_LEN).unwrap();
        assert_eq!(
            placement.preludes,
            vec![
                LayoutPrelude::PageHeader {
                    position: pos(0, 0),
                    page_no: 0,
                },
                LayoutPrelude::FileHeader {
                    position: pos(0, PAGE_HEADER_ENCODED_LEN as u64),
                },
            ]
        );
        assert_eq!(placement.record_start, pos(0, FIRST_PAGE_RECORD_AREA_START));
        assert_eq!(placement.encoded_len, TX_BEGIN_ENCODED_LEN);
        assert_eq!(
            planner.position(),
            pos(
                0,
                FIRST_PAGE_RECORD_AREA_START + u64::from(TX_BEGIN_ENCODED_LEN)
            )
        );
    });
}

/// A2: first-page record-area start (offset 64) — no preludes.
#[test]
fn first_page_record_area_start_has_no_preludes() {
    for_each_geometry(|g| {
        let mut planner =
            LayoutPlanner::from_position(g, pos(0, FIRST_PAGE_RECORD_AREA_START)).unwrap();
        let placement = planner.plan_record(TX_BEGIN_ENCODED_LEN).unwrap();
        assert!(placement.preludes.is_empty());
        assert_eq!(placement.record_start, pos(0, FIRST_PAGE_RECORD_AREA_START));
        assert_eq!(
            planner.position(),
            pos(
                0,
                FIRST_PAGE_RECORD_AREA_START + u64::from(TX_BEGIN_ENCODED_LEN)
            )
        );
    });
}

/// A3: first-page interior — no preludes.
#[test]
fn first_page_interior_has_no_preludes() {
    for_each_geometry(|g| {
        let mut planner = LayoutPlanner::from_position(g, pos(0, 164)).unwrap();
        let placement = planner.plan_record(100).unwrap();
        assert!(placement.preludes.is_empty());
        assert_eq!(placement.record_start, pos(0, 164));
        assert_eq!(planner.position(), pos(0, 264));
    });
}

/// A4: page boundary (offset P) — PageHeader for page 1, record lands at P+16.
#[test]
fn page_boundary_emits_page_header() {
    for_each_geometry(|g| {
        let p = g.page_size;
        let mut planner = LayoutPlanner::from_position(g, pos(0, p)).unwrap();
        let placement = planner.plan_record(TX_BEGIN_ENCODED_LEN).unwrap();
        assert_eq!(
            placement.preludes,
            vec![LayoutPrelude::PageHeader {
                position: pos(0, p),
                page_no: 1,
            }]
        );
        assert_eq!(
            placement.record_start,
            pos(0, p + OTHER_PAGE_RECORD_AREA_OFFSET)
        );
        assert_eq!(
            planner.position(),
            pos(
                0,
                p + OTHER_PAGE_RECORD_AREA_OFFSET + u64::from(TX_BEGIN_ENCODED_LEN)
            )
        );
    });
}

/// A5: later-page record-area start (offset P+16) — no preludes.
#[test]
fn later_page_record_area_start_has_no_preludes() {
    for_each_geometry(|g| {
        let p = g.page_size;
        let mut planner =
            LayoutPlanner::from_position(g, pos(0, p + OTHER_PAGE_RECORD_AREA_OFFSET)).unwrap();
        let placement = planner.plan_record(TX_BEGIN_ENCODED_LEN).unwrap();
        assert!(placement.preludes.is_empty());
        assert_eq!(
            placement.record_start,
            pos(0, p + OTHER_PAGE_RECORD_AREA_OFFSET)
        );
    });
}

/// A6: file boundary (offset F, which is also a page boundary) — roll to file 1
/// and emit PageHeader+FileHeader.
#[test]
fn file_boundary_rolls_and_emits_page_and_file_headers() {
    for_each_geometry(|g| {
        let f = g.max_file_size;
        let mut planner = LayoutPlanner::from_position(g, pos(0, f)).unwrap();
        let placement = planner.plan_record(TX_BEGIN_ENCODED_LEN).unwrap();
        assert_eq!(
            placement.preludes,
            vec![
                LayoutPrelude::PageHeader {
                    position: pos(1, 0),
                    page_no: 0,
                },
                LayoutPrelude::FileHeader {
                    position: pos(1, PAGE_HEADER_ENCODED_LEN as u64),
                },
            ]
        );
        assert_eq!(placement.record_start, pos(1, FIRST_PAGE_RECORD_AREA_START));
        assert_eq!(
            planner.position(),
            pos(
                1,
                FIRST_PAGE_RECORD_AREA_START + u64::from(TX_BEGIN_ENCODED_LEN)
            )
        );
    });
}

/// A7: file boundary at the last allowed file id — capacity error, position
/// unchanged.
#[test]
fn file_boundary_at_max_file_id_is_capacity_exceeded() {
    for_each_geometry(|g| {
        let (f, m) = (g.max_file_size, g.max_file_id);
        let start = pos(m, f);
        let mut planner = LayoutPlanner::from_position(g, start).unwrap();
        assert_kind(
            planner.plan_record(TX_BEGIN_ENCODED_LEN),
            StorageErrorKind::CapacityExceeded,
        );
        assert_eq!(planner.position(), start);
    });
}

/// B1: a record ending exactly at the page boundary leaves no preludes, then the
/// next record continues on the next page.
#[test]
fn record_ending_at_page_boundary_then_next_record_gets_page_header() {
    for_each_geometry(|g| {
        let p = g.page_size;
        let mut planner = LayoutPlanner::from_position(g, pos(0, p / 2)).unwrap();
        let placement = planner.plan_record((p / 2) as u32).unwrap();
        assert!(placement.preludes.is_empty());
        assert_eq!(placement.record_start, pos(0, p / 2));
        assert_eq!(planner.position(), pos(0, p));

        let next = planner.plan_record(TX_BEGIN_ENCODED_LEN).unwrap();
        assert_eq!(
            next.preludes,
            vec![LayoutPrelude::PageHeader {
                position: pos(0, p),
                page_no: 1,
            }]
        );
        assert_eq!(next.record_start, pos(0, p + OTHER_PAGE_RECORD_AREA_OFFSET));
    });
}

/// B2: a record crossing the page boundary gets a PageEnd then a PageHeader.
#[test]
fn record_crossing_page_boundary_gets_page_end_then_page_header() {
    for_each_geometry(|g| {
        let p = g.page_size;
        let record_len = (p / 2 + 100) as u32;
        let mut planner = LayoutPlanner::from_position(g, pos(0, p / 2)).unwrap();
        let placement = planner.plan_record(record_len).unwrap();
        assert_eq!(
            placement.preludes,
            vec![
                LayoutPrelude::PageEnd {
                    position: pos(0, p / 2),
                    encoded_len: (p / 2) as u32,
                },
                LayoutPrelude::PageHeader {
                    position: pos(0, p),
                    page_no: 1,
                },
            ]
        );
        assert_eq!(
            placement.record_start,
            pos(0, p + OTHER_PAGE_RECORD_AREA_OFFSET)
        );
        assert_eq!(
            planner.position(),
            pos(0, p + OTHER_PAGE_RECORD_AREA_OFFSET + u64::from(record_len))
        );
    });
}

/// B3: a trailing remainder of 1..42 bytes forces a PageEnd before the record.
#[test]
fn tail_remainder_1_to_42_forces_page_end() {
    for_each_geometry(|g| {
        let p = g.page_size;
        let start = p - u64::from(TX_BEGIN_ENCODED_LEN) - 1;
        let mut planner = LayoutPlanner::from_position(g, pos(0, start)).unwrap();
        let placement = planner.plan_record(TX_BEGIN_ENCODED_LEN).unwrap();
        assert_eq!(
            placement.preludes,
            vec![
                LayoutPrelude::PageEnd {
                    position: pos(0, start),
                    encoded_len: u64::from(TX_BEGIN_ENCODED_LEN) as u32 + 1,
                },
                LayoutPrelude::PageHeader {
                    position: pos(0, p),
                    page_no: 1,
                },
            ]
        );
        assert_eq!(
            placement.record_start,
            pos(0, p + OTHER_PAGE_RECORD_AREA_OFFSET)
        );
    });
}

/// B4: a trailing remainder of exactly 43 bytes does NOT force a PageEnd.
#[test]
fn tail_remainder_43_does_not_force_page_end() {
    for_each_geometry(|g| {
        let p = g.page_size;
        let start = p - u64::from(TX_BEGIN_ENCODED_LEN) - u64::from(PAGE_END_MIN_SIZE);
        let mut planner = LayoutPlanner::from_position(g, pos(0, start)).unwrap();
        let placement = planner.plan_record(TX_BEGIN_ENCODED_LEN).unwrap();
        assert!(placement.preludes.is_empty());
        assert_eq!(placement.record_start, pos(0, start));
        assert_eq!(planner.position(), pos(0, p - u64::from(PAGE_END_MIN_SIZE)));
    });
}

/// B5: a record ending exactly at the file boundary, then the next record rolls.
#[test]
fn record_ending_at_file_boundary_then_next_record_rolls() {
    for_each_geometry(|g| {
        let f = g.max_file_size;
        let mut planner =
            LayoutPlanner::from_position(g, pos(0, f - u64::from(TX_BEGIN_ENCODED_LEN))).unwrap();
        let placement = planner.plan_record(TX_BEGIN_ENCODED_LEN).unwrap();
        assert!(placement.preludes.is_empty());
        assert_eq!(
            placement.record_start,
            pos(0, f - u64::from(TX_BEGIN_ENCODED_LEN))
        );
        assert_eq!(planner.position(), pos(0, f));

        let next = planner.plan_record(TX_BEGIN_ENCODED_LEN).unwrap();
        assert_eq!(
            next.preludes,
            vec![
                LayoutPrelude::PageHeader {
                    position: pos(1, 0),
                    page_no: 0,
                },
                LayoutPrelude::FileHeader {
                    position: pos(1, PAGE_HEADER_ENCODED_LEN as u64),
                },
            ]
        );
        assert_eq!(next.record_start, pos(1, FIRST_PAGE_RECORD_AREA_START));
    });
}

/// B6: an unusable page tail (fewer than 43 bytes left) is rejected with
/// InvalidLayout and leaves the planner untouched. Only reachable through
/// `from_position`; the planner itself never produces this state.
#[test]
fn unusable_page_tail_is_invalid_layout() {
    for_each_geometry(|g| {
        let p = g.page_size;
        let start = pos(0, p - 10);
        let mut planner = LayoutPlanner::from_position(g, start).unwrap();
        assert_kind(
            planner.plan_record(TX_BEGIN_ENCODED_LEN),
            StorageErrorKind::InvalidLayout,
        );
        assert_eq!(planner.position(), start);
    });
}

/// C1: the `after != 0 && after < 43` guard — a record that would land 4 bytes
/// short of the page end after a reposition is rejected. `plan_record(P-20)`
/// from a fresh file always leaves after == 4.
#[test]
fn record_leaving_4_byte_tail_after_reposition_is_invalid_layout() {
    for_each_geometry(|g| {
        let p = g.page_size;
        let mut planner = LayoutPlanner::empty(g).unwrap();
        assert_kind(
            planner.plan_record((p - 20) as u32),
            StorageErrorKind::InvalidLayout,
        );
        assert_eq!(planner.position(), pos(0, 0));
    });
}

/// C2: the Ok counterpart of C1 — `plan_record(P-16)` from a fresh file fills
/// the first later page exactly and emits all four preludes.
#[test]
fn fresh_file_exact_fill_emits_four_preludes() {
    for_each_geometry(|g| {
        let p = g.page_size;
        let mut planner = LayoutPlanner::empty(g).unwrap();
        let placement = planner.plan_record((p - 16) as u32).unwrap();
        assert_eq!(
            placement.preludes,
            vec![
                LayoutPrelude::PageHeader {
                    position: pos(0, 0),
                    page_no: 0,
                },
                LayoutPrelude::FileHeader {
                    position: pos(0, PAGE_HEADER_ENCODED_LEN as u64),
                },
                LayoutPrelude::PageEnd {
                    position: pos(0, FIRST_PAGE_RECORD_AREA_START),
                    encoded_len: (p - FIRST_PAGE_RECORD_AREA_START) as u32,
                },
                LayoutPrelude::PageHeader {
                    position: pos(0, p),
                    page_no: 1,
                },
            ]
        );
        assert_eq!(
            placement.record_start,
            pos(0, p + OTHER_PAGE_RECORD_AREA_OFFSET)
        );
        assert_eq!(planner.position(), pos(0, 2 * p));
    });
}

/// C3: a file roll followed by a page-end on the new file's first page — the
/// maximal four-prelude placement, ending with after == 43.
#[test]
fn file_roll_then_page_tail_emits_four_preludes() {
    for_each_geometry(|g| {
        let p = g.page_size;
        let f = g.max_file_size;
        let mut planner = LayoutPlanner::from_position(g, pos(0, f)).unwrap();
        let placement = planner.plan_record((p - 59) as u32).unwrap();
        assert_eq!(
            placement.preludes,
            vec![
                LayoutPrelude::PageHeader {
                    position: pos(1, 0),
                    page_no: 0,
                },
                LayoutPrelude::FileHeader {
                    position: pos(1, PAGE_HEADER_ENCODED_LEN as u64),
                },
                LayoutPrelude::PageEnd {
                    position: pos(1, FIRST_PAGE_RECORD_AREA_START),
                    encoded_len: (p - FIRST_PAGE_RECORD_AREA_START) as u32,
                },
                LayoutPrelude::PageHeader {
                    position: pos(1, p),
                    page_no: 1,
                },
            ]
        );
        assert_eq!(
            placement.record_start,
            pos(1, p + OTHER_PAGE_RECORD_AREA_OFFSET)
        );
        assert_eq!(planner.position(), pos(1, 2 * p - 43));
    });
}

/// C4: a page-end at the file tail combined with a file roll — three preludes.
#[test]
fn page_end_at_file_tail_rolls_with_three_preludes() {
    for_each_geometry(|g| {
        let f = g.max_file_size;
        let start = f - u64::from(TX_BEGIN_ENCODED_LEN) - 1;
        let mut planner = LayoutPlanner::from_position(g, pos(0, start)).unwrap();
        let placement = planner.plan_record(TX_BEGIN_ENCODED_LEN).unwrap();
        assert_eq!(
            placement.preludes,
            vec![
                LayoutPrelude::PageEnd {
                    position: pos(0, start),
                    encoded_len: u64::from(TX_BEGIN_ENCODED_LEN) as u32 + 1,
                },
                LayoutPrelude::PageHeader {
                    position: pos(1, 0),
                    page_no: 0,
                },
                LayoutPrelude::FileHeader {
                    position: pos(1, PAGE_HEADER_ENCODED_LEN as u64),
                },
            ]
        );
        assert_eq!(placement.record_start, pos(1, FIRST_PAGE_RECORD_AREA_START));
        assert_eq!(
            planner.position(),
            pos(
                1,
                FIRST_PAGE_RECORD_AREA_START + u64::from(TX_BEGIN_ENCODED_LEN)
            )
        );
    });
}

/// C5: the 111-byte TxPreparedEnd crossing the page and file boundaries.
#[test]
fn footer_length_record_crossing_page_and_file() {
    for_each_geometry(|g| {
        let (p, f) = (g.page_size, g.max_file_size);
        let footer_len = TX_PREPARED_END_ENCODED_LEN;

        // 111 at a page tail (remaining 100, remainder 29) → PageEnd + PageHeader.
        let mut a = LayoutPlanner::from_position(g, pos(0, p - 100)).unwrap();
        let placement = a.plan_record(footer_len).unwrap();
        assert_eq!(
            placement.preludes,
            vec![
                LayoutPrelude::PageEnd {
                    position: pos(0, p - 100),
                    encoded_len: 100,
                },
                LayoutPrelude::PageHeader {
                    position: pos(0, p),
                    page_no: 1,
                },
            ]
        );
        assert_eq!(
            placement.record_start,
            pos(0, p + OTHER_PAGE_RECORD_AREA_OFFSET)
        );

        // 111 with a trailing remainder of exactly 43 → no preludes.
        let mut b = LayoutPlanner::from_position(g, pos(0, p - 100 - 54)).unwrap();
        let placement = b.plan_record(footer_len).unwrap();
        assert!(placement.preludes.is_empty());
        assert_eq!(b.position(), pos(0, p - u64::from(PAGE_END_MIN_SIZE)));

        // 111 at the file tail → PageEnd + roll.
        let mut c = LayoutPlanner::from_position(g, pos(0, f - 100)).unwrap();
        let placement = c.plan_record(footer_len).unwrap();
        assert_eq!(
            placement.preludes,
            vec![
                LayoutPrelude::PageEnd {
                    position: pos(0, f - 100),
                    encoded_len: 100,
                },
                LayoutPrelude::PageHeader {
                    position: pos(1, 0),
                    page_no: 0,
                },
                LayoutPrelude::FileHeader {
                    position: pos(1, PAGE_HEADER_ENCODED_LEN as u64),
                },
            ]
        );
        assert_eq!(placement.record_start, pos(1, FIRST_PAGE_RECORD_AREA_START));

        // 111 ending exactly 43 bytes before the file boundary → no preludes.
        let mut d = LayoutPlanner::from_position(g, pos(0, f - 100 - 54)).unwrap();
        let placement = d.plan_record(footer_len).unwrap();
        assert!(placement.preludes.is_empty());
        assert_eq!(d.position(), pos(0, f - u64::from(PAGE_END_MIN_SIZE)));
    });
}

/// D1: `from_position` rejects record positions inside the page header area and
/// out-of-range file ids / offsets.
#[test]
fn from_position_rejects_header_area_and_out_of_range() {
    for_each_geometry(|g| {
        let (p, f, m) = (g.page_size, g.max_file_size, g.max_file_id);
        // Just below the first-page record-area minimum.
        assert_kind(
            LayoutPlanner::from_position(g, pos(0, FIRST_PAGE_RECORD_AREA_START - 1)),
            StorageErrorKind::InvalidArgument,
        );
        // Just below the later-page record-area minimum.
        assert_kind(
            LayoutPlanner::from_position(g, pos(0, p + OTHER_PAGE_RECORD_AREA_OFFSET - 1)),
            StorageErrorKind::InvalidArgument,
        );
        // file_id above the maximum.
        assert_kind(
            LayoutPlanner::from_position(g, pos(m + 1, 0)),
            StorageErrorKind::InvalidArgument,
        );
        // offset above the maximum.
        assert_kind(
            LayoutPlanner::from_position(g, pos(0, f + 1)),
            StorageErrorKind::InvalidArgument,
        );
    });
}

/// D2: `plan_record` rejects records smaller than the page-end minimum and
/// larger than a page.
#[test]
fn plan_record_rejects_out_of_range_lengths() {
    for_each_geometry(|g| {
        let p = g.page_size;
        let mut a = LayoutPlanner::empty(g).unwrap();
        assert_kind(
            a.plan_record(PAGE_END_MIN_SIZE - 1),
            StorageErrorKind::InvalidArgument,
        );
        let mut b = LayoutPlanner::empty(g).unwrap();
        assert_kind(
            b.plan_record(p as u32 + 1),
            StorageErrorKind::InvalidArgument,
        );
    });
}

/// D3: the last allowed file id is usable as long as we are not at its boundary.
#[test]
fn last_file_id_is_usable_before_its_boundary() {
    for_each_geometry(|g| {
        let m = g.max_file_id;
        let mut planner =
            LayoutPlanner::from_position(g, pos(m, FIRST_PAGE_RECORD_AREA_START)).unwrap();
        let placement = planner.plan_record(TX_BEGIN_ENCODED_LEN).unwrap();
        assert!(placement.preludes.is_empty());
        assert_eq!(placement.record_start, pos(m, FIRST_PAGE_RECORD_AREA_START));
    });
}

// ====================================================================
// Test 2: prepare_envelope produces the correct PreparedEnvelope at
// every write position; the chunk stream directly exercises
// append_placement.
// ====================================================================

/// A: envelope from a fresh file (planner at 0), ops [Put, Delete].
#[test]
fn envelope_from_fresh_file() {
    for_each_geometry(|g| {
        let mut planner = LayoutPlanner::empty(g).unwrap();
        let ops = [put_op(b"k", b"v"), delete_op(b"x")];
        let envelope = prepare(&mut planner, 1, &ops).unwrap();

        assert_eq!(envelope.vlog_begin, pos(0, 0));
        assert_eq!(planner.position(), envelope.vlog_end);
        assert_eq!(envelope.value_pointers, vec![Some(pointer()), None]);
        assert_eq!(
            record_positions(&envelope, RecordType::TxBegin),
            vec![pos(0, 64)]
        );
        assert_eq!(
            record_positions(&envelope, RecordType::KvRecord),
            vec![pos(0, 135)]
        );
        assert_eq!(
            record_positions(&envelope, RecordType::DeleteRecord),
            vec![pos(0, 192)]
        );
        assert_eq!(
            record_positions(&envelope, RecordType::TxPreparedEnd),
            vec![pos(0, 246)]
        );
        assert_eq!(envelope.vlog_end, pos(0, 357));
        assert_envelope_invariants(&envelope, g, (2, 1, 1, 2));
    });
}

/// B: envelope starting at a page boundary.
#[test]
fn envelope_from_page_boundary() {
    for_each_geometry(|g| {
        let p = g.page_size;
        let mut planner = LayoutPlanner::from_position(g, pos(0, p)).unwrap();
        let ops = [delete_op(b"x")];
        let envelope = prepare(&mut planner, 2, &ops).unwrap();

        assert_eq!(envelope.vlog_begin, pos(0, p));
        assert_eq!(planner.position(), envelope.vlog_end);
        assert_eq!(
            record_positions(&envelope, RecordType::TxBegin),
            vec![pos(0, p + OTHER_PAGE_RECORD_AREA_OFFSET)]
        );
        assert_eq!(envelope.vlog_end, pos(0, p + 252));
        assert_envelope_invariants(&envelope, g, (1, 0, 1, 1));
    });
}

/// C: envelope starting at the file boundary — the first chunks roll to file 1.
#[test]
fn envelope_from_file_boundary_rolls() {
    for_each_geometry(|g| {
        let f = g.max_file_size;
        let mut planner = LayoutPlanner::from_position(g, pos(0, f)).unwrap();
        let ops = [delete_op(b"x")];
        let envelope = prepare(&mut planner, 3, &ops).unwrap();

        assert_eq!(envelope.vlog_begin, pos(0, f));
        assert_eq!(planner.position(), envelope.vlog_end);
        assert_eq!(envelope.chunks[0].position, pos(1, 0));
        assert_eq!(
            envelope.chunks[1].position,
            pos(1, PAGE_HEADER_ENCODED_LEN as u64)
        );
        assert!(is_page_header(&envelope.chunks[0], g).is_some());
        assert!(is_file_header(&envelope.chunks[1]).is_some());
        assert_eq!(
            record_positions(&envelope, RecordType::TxBegin),
            vec![pos(1, FIRST_PAGE_RECORD_AREA_START)]
        );
        assert_eq!(envelope.vlog_end, pos(1, 300));
        assert_envelope_invariants(&envelope, g, (1, 0, 1, 1));
    });
}

/// D: envelope starting in the last 100 bytes of a page — the first chunk is a
/// PageEnd, then the page header, then the transaction records.
#[test]
fn envelope_from_page_tail_starts_with_page_end() {
    for_each_geometry(|g| {
        let p = g.page_size;
        let start = p - 100;
        let mut planner = LayoutPlanner::from_position(g, pos(0, start)).unwrap();
        let ops = [delete_op(b"x")];
        let envelope = prepare(&mut planner, 4, &ops).unwrap();

        assert_eq!(envelope.vlog_begin, pos(0, start));
        assert_eq!(planner.position(), envelope.vlog_end);
        assert_eq!(envelope.chunks[0].position, pos(0, start));
        assert_eq!(
            record_type(&envelope.chunks[0].bytes),
            Some(RecordType::PageEnd)
        );
        assert_eq!(envelope.chunks[0].bytes.len(), 100);
        assert_eq!(envelope.chunks[1].position, pos(0, p));
        assert!(is_page_header(&envelope.chunks[1], g).is_some());
        assert_eq!(
            record_positions(&envelope, RecordType::TxBegin),
            vec![pos(0, p + OTHER_PAGE_RECORD_AREA_OFFSET)]
        );
        assert_eq!(envelope.vlog_end, pos(0, p + 252));
        assert_envelope_invariants(&envelope, g, (1, 0, 1, 1));
    });
}

/// E: envelope starting strictly inside a later page — no header preludes.
#[test]
fn envelope_from_later_page_interior_has_no_headers() {
    for_each_geometry(|g| {
        let p = g.page_size;
        let start = p + 100;
        let mut planner = LayoutPlanner::from_position(g, pos(0, start)).unwrap();
        let ops = [delete_op(b"x")];
        let envelope = prepare(&mut planner, 5, &ops).unwrap();

        assert_eq!(envelope.vlog_begin, pos(0, start));
        assert_eq!(planner.position(), envelope.vlog_end);
        assert_eq!(envelope.chunks[0].position, pos(0, start));
        assert_eq!(
            record_type(&envelope.chunks[0].bytes),
            Some(RecordType::TxBegin)
        );
        assert_eq!(envelope.vlog_end, pos(0, p + 336));
        assert_envelope_invariants(&envelope, g, (1, 0, 1, 1));
    });
}

/// F: an envelope large enough to cross a file boundary naturally. Each Put uses
/// a value of P-72 so its encoded length (P-16) exactly fills a later page.
#[test]
fn envelope_crossing_files_naturally() {
    for_each_geometry(|g| {
        let (p, f) = (g.page_size, g.max_file_size);
        let pages_per_file = f / p;
        let n = pages_per_file + 1;
        let value_len = (p - 72) as usize;

        let keys: Vec<Vec<u8>> = (0..n).map(|i| vec![b'a' + i as u8]).collect();
        let values: Vec<Vec<u8>> = (0..n).map(|_| vec![0x5a; value_len]).collect();
        let ops: Vec<LogicalOperationRef<'_>> = keys
            .iter()
            .zip(values.iter())
            .map(|(key, value)| put_op(key, value))
            .collect();

        let mut planner = LayoutPlanner::empty(g).unwrap();
        let envelope = prepare(&mut planner, 6, &ops).unwrap();

        assert_eq!(envelope.vlog_begin, pos(0, 0));
        assert_eq!(planner.position(), envelope.vlog_end);
        assert!(
            envelope.vlog_end.file_id >= 1,
            "expected a file crossing, got {envelope:?}"
        );
        assert!(envelope.chunks.iter().any(|chunk| {
            chunk.position == pos(1, PAGE_HEADER_ENCODED_LEN as u64)
                && is_file_header(chunk).is_some()
        }));
        assert!(
            envelope
                .chunks
                .iter()
                .any(|chunk| record_type(&chunk.bytes) == Some(RecordType::PageEnd))
        );
        assert_eq!(envelope.value_pointers.len(), n as usize);
        assert!(envelope.value_pointers.iter().all(Option::is_some));
        assert_envelope_invariants(&envelope, g, (n, n, 0, n));
    });
}

/// G: capacity at the last file's boundary, with a positive control on the last
/// page of the last file.
#[test]
fn envelope_capacity_at_last_file_boundary() {
    for_each_geometry(|g| {
        let (p, f, m) = (g.page_size, g.max_file_size, g.max_file_id);

        // At the boundary of the last file the very first record cannot be placed.
        let start = pos(m, f);
        let mut planner = LayoutPlanner::from_position(g, start).unwrap();
        let ops = [delete_op(b"x")];
        assert_kind(
            prepare(&mut planner, 7, &ops),
            StorageErrorKind::CapacityExceeded,
        );
        assert_eq!(planner.position(), start);

        // Positive control: the record-area start of the last page of the last
        // file leaves room for the whole envelope.
        let last_page_start = p * (f / p - 1) + OTHER_PAGE_RECORD_AREA_OFFSET;
        let mut planner = LayoutPlanner::from_position(g, pos(m, last_page_start)).unwrap();
        let ops = [delete_op(b"x")];
        let envelope = prepare(&mut planner, 8, &ops).unwrap();
        assert_eq!(envelope.vlog_begin, pos(m, last_page_start));
        assert_eq!(envelope.vlog_end, pos(m, last_page_start + 236));
        assert_eq!(planner.position(), envelope.vlog_end);
        assert_envelope_invariants(&envelope, g, (1, 0, 1, 1));
    });
}

/// H: value pointers align with their record chunks for a mixed Put/Delete batch.
#[test]
fn envelope_value_pointers_align_with_record_chunks() {
    for_each_geometry(|g| {
        let mut planner = LayoutPlanner::empty(g).unwrap();
        let ops = [put_op(b"k", b"v"), delete_op(b"x"), put_op(b"k2", b"v2")];
        let envelope = prepare(&mut planner, 9, &ops).unwrap();

        let kv_positions = record_positions(&envelope, RecordType::KvRecord);
        assert_eq!(kv_positions.len(), 2);

        assert_eq!(envelope.value_pointers.len(), 3);
        assert!(matches!(
            envelope.value_pointers[0],
            Some(ValuePointer {
                file_id: 0,
                record_offset: 135,
                record_len: 57,
                value_len: 1,
                ..
            })
        ));
        assert!(envelope.value_pointers[1].is_none());
        assert!(matches!(
            envelope.value_pointers[2],
            Some(ValuePointer {
                file_id: 0,
                record_offset: 246,
                record_len: 59,
                value_len: 2,
                ..
            })
        ));

        // Each Some pointer must describe its own record chunk exactly.
        let mut some_pointers = envelope
            .value_pointers
            .iter()
            .filter_map(|pointer| pointer.as_ref());
        for kv_position in &kv_positions {
            let pointer = some_pointers.next().unwrap();
            assert_eq!(pointer.file_id, kv_position.file_id);
            assert_eq!(u64::from(pointer.record_offset), kv_position.offset);
        }

        assert_eq!(envelope.vlog_end, pos(0, 416));
        assert_envelope_invariants(&envelope, g, (3, 2, 1, 3));
    });
}

fn pointer() -> ValuePointer {
    ValuePointer {
        format_version: VALUE_POINTER_FORMAT_VERSION,
        file_id: 0,
        record_offset: 135,
        record_len: 57,
        value_len: 1,
    }
}

fn is_page_header(chunk: &PhysicalChunk, geometry: VLogGeometry) -> Option<PageHeader> {
    if chunk.bytes.len() != PAGE_HEADER_ENCODED_LEN {
        return None;
    }
    let header = PageHeader::decode(&chunk.bytes).ok()?;
    let expected_page_no = (chunk.position.offset / geometry.page_size) as u32;
    (header.file_id == chunk.position.file_id && header.page_no == expected_page_no)
        .then_some(header)
}

fn is_file_header(chunk: &PhysicalChunk) -> Option<VLogFileHeader> {
    if chunk.bytes.len() != FILE_HEADER_ENCODED_LEN {
        return None;
    }
    let header = VLogFileHeader::decode(&chunk.bytes).ok()?;
    (header.database_uuid == database_uuid() && header.file_id == chunk.position.file_id)
        .then_some(header)
}

// ====================================================================
// Test 2 (round 2): envelope starts / internal layouts the first pass
// did not cover — page-tail exact remainders, boundary-exact records,
// mid-envelope PageEnds, mid-way capacity exhaustion, and a precise
// multi-file chunk stream.
// ====================================================================

/// Compact classification of one physical chunk for exact-stream assertions.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum ChunkKind {
    PageHeader(u32),
    FileHeader,
    PageEnd(u32),
    Record(RecordType),
}

fn chunk_kind(chunk: &PhysicalChunk, geometry: VLogGeometry) -> ChunkKind {
    if chunk.bytes.len() == PAGE_HEADER_ENCODED_LEN
        && let Some(header) = PageHeader::decode(&chunk.bytes).ok()
    {
        let expected_page_no = (chunk.position.offset / geometry.page_size) as u32;
        assert_eq!(
            header.file_id, chunk.position.file_id,
            "decoded file_id mismatch"
        );
        assert_eq!(header.page_no, expected_page_no, "decoded page_no mismatch");
        return ChunkKind::PageHeader(header.page_no);
    }
    if chunk.bytes.len() == FILE_HEADER_ENCODED_LEN
        && let Some(header) = VLogFileHeader::decode(&chunk.bytes).ok()
    {
        assert_eq!(
            header.database_uuid,
            database_uuid(),
            "database UUID mismatch"
        );
        assert_eq!(
            header.file_id, chunk.position.file_id,
            "file header id mismatch"
        );
        return ChunkKind::FileHeader;
    }
    match record_type(&chunk.bytes) {
        Some(RecordType::PageEnd) => ChunkKind::PageEnd(chunk.bytes.len() as u32),
        Some(other) => ChunkKind::Record(other),
        None => panic!("unrecognized chunk at {:?}", chunk.position),
    }
}

/// N1: envelope from an arbitrary first-page interior position.
#[test]
fn envelope_from_first_page_interior_offset_100() {
    for_each_geometry(|g| {
        let mut planner = LayoutPlanner::from_position(g, pos(0, 100)).unwrap();
        let ops = [delete_op(b"x")];
        let envelope = prepare(&mut planner, 11, &ops).unwrap();

        assert_eq!(envelope.vlog_begin, pos(0, 100));
        assert_eq!(
            record_positions(&envelope, RecordType::TxBegin),
            vec![pos(0, 100)]
        );
        assert_eq!(
            record_positions(&envelope, RecordType::DeleteRecord),
            vec![pos(0, 171)]
        );
        assert_eq!(
            record_positions(&envelope, RecordType::TxPreparedEnd),
            vec![pos(0, 225)]
        );
        assert_eq!(envelope.vlog_end, pos(0, 336));
        assert_eq!(planner.position(), envelope.vlog_end);
        assert_envelope_invariants(&envelope, g, (1, 0, 1, 1));
    });
}

/// N2: envelope starting with exactly 43 / 42 / 1 bytes left in the page, on the
/// first page and on a later page. 43 is legal (a minimal PageEnd); 42 and 1 are
/// InvalidLayout.
#[test]
fn envelope_starting_at_exact_page_tail_remainders() {
    for_each_geometry(|g| {
        let p = g.page_size;
        let ops = [delete_op(b"x")];

        // First page, remaining exactly 43: PageEnd(43) then the records.
        let start = p - u64::from(PAGE_END_MIN_SIZE);
        let mut planner = LayoutPlanner::from_position(g, pos(0, start)).unwrap();
        let envelope = prepare(&mut planner, 12, &ops).unwrap();
        assert_eq!(envelope.vlog_begin, pos(0, start));
        assert_eq!(envelope.chunks[0].position, pos(0, start));
        assert_eq!(envelope.chunks[0].bytes.len(), PAGE_END_MIN_SIZE as usize);
        assert_eq!(
            record_type(&envelope.chunks[0].bytes),
            Some(RecordType::PageEnd)
        );
        assert!(is_page_header(&envelope.chunks[1], g).is_some());
        assert_eq!(
            record_positions(&envelope, RecordType::TxBegin),
            vec![pos(0, p + OTHER_PAGE_RECORD_AREA_OFFSET)]
        );
        assert_eq!(envelope.vlog_end, pos(0, p + 252));
        assert_envelope_invariants(&envelope, g, (1, 0, 1, 1));

        // Later page (page 2), remaining exactly 43: same shape.
        let start = 2 * p - u64::from(PAGE_END_MIN_SIZE);
        let mut planner = LayoutPlanner::from_position(g, pos(0, start)).unwrap();
        let envelope = prepare(&mut planner, 13, &ops).unwrap();
        assert_eq!(envelope.chunks[0].position, pos(0, start));
        assert_eq!(envelope.chunks[0].bytes.len(), PAGE_END_MIN_SIZE as usize);
        assert_eq!(
            record_positions(&envelope, RecordType::TxBegin),
            vec![pos(0, 2 * p + OTHER_PAGE_RECORD_AREA_OFFSET)]
        );
        assert_eq!(envelope.vlog_end, pos(0, 2 * p + 252));
        assert_envelope_invariants(&envelope, g, (1, 0, 1, 1));

        // Remaining 42 / 1 is an unusable tail: InvalidLayout, planner untouched.
        for bad in [p - 42, p - 1, 2 * p - 42, 2 * p - 1] {
            let start = pos(0, bad);
            let mut planner = LayoutPlanner::from_position(g, start).unwrap();
            assert_kind(
                prepare(&mut planner, 14, &ops),
                StorageErrorKind::InvalidLayout,
            );
            assert_eq!(planner.position(), start);
        }
    });
}

/// N3: BEGIN ends exactly at the page boundary — the next record gets a header.
#[test]
fn envelope_begin_ends_exactly_at_page_boundary() {
    for_each_geometry(|g| {
        let p = g.page_size;
        let start = p - u64::from(TX_BEGIN_ENCODED_LEN);
        let mut planner = LayoutPlanner::from_position(g, pos(0, start)).unwrap();
        let ops = [put_op(b"k", b"v")];
        let envelope = prepare(&mut planner, 15, &ops).unwrap();

        assert_eq!(envelope.vlog_begin, pos(0, start));
        assert_eq!(
            record_positions(&envelope, RecordType::TxBegin),
            vec![pos(0, start)]
        );
        assert_eq!(
            record_positions(&envelope, RecordType::KvRecord),
            vec![pos(0, p + OTHER_PAGE_RECORD_AREA_OFFSET)]
        );
        assert_eq!(envelope.vlog_end, pos(0, p + 184));
        assert_eq!(planner.position(), envelope.vlog_end);
        assert_envelope_invariants(&envelope, g, (1, 1, 0, 1));
    });
}

/// N4: BEGIN ends exactly at the file boundary — the next record rolls.
#[test]
fn envelope_begin_ends_exactly_at_file_boundary() {
    for_each_geometry(|g| {
        let f = g.max_file_size;
        let start = f - u64::from(TX_BEGIN_ENCODED_LEN);
        let mut planner = LayoutPlanner::from_position(g, pos(0, start)).unwrap();
        let ops = [put_op(b"k", b"v")];
        let envelope = prepare(&mut planner, 16, &ops).unwrap();

        assert_eq!(envelope.vlog_begin, pos(0, start));
        assert_eq!(
            record_positions(&envelope, RecordType::TxBegin),
            vec![pos(0, start)]
        );
        assert_eq!(
            record_positions(&envelope, RecordType::KvRecord),
            vec![pos(1, FIRST_PAGE_RECORD_AREA_START)]
        );
        assert_eq!(envelope.vlog_end, pos(1, 232));
        assert_eq!(planner.position(), envelope.vlog_end);
        assert_envelope_invariants(&envelope, g, (1, 1, 0, 1));
    });
}

/// N5: a PageEnd fired between two operations (after the Delete, before the
/// second Put).
#[test]
fn envelope_page_end_between_operations() {
    for_each_geometry(|g| {
        let p = g.page_size;
        let start = p - 228;
        let mut planner = LayoutPlanner::from_position(g, pos(0, start)).unwrap();
        let ops = [put_op(b"k", b"v"), delete_op(b"x"), put_op(b"l", b"w")];
        let envelope = prepare(&mut planner, 17, &ops).unwrap();

        assert_eq!(envelope.vlog_begin, pos(0, start));
        assert_eq!(
            record_positions(&envelope, RecordType::TxBegin),
            vec![pos(0, start)]
        );
        assert_eq!(
            record_positions(&envelope, RecordType::KvRecord),
            vec![
                pos(0, start + 71),
                pos(0, p + OTHER_PAGE_RECORD_AREA_OFFSET)
            ]
        );
        assert_eq!(
            record_positions(&envelope, RecordType::DeleteRecord),
            vec![pos(0, start + 128)]
        );
        // The single PageEnd sits between the Delete and the second Put.
        let page_end_index = envelope
            .chunks
            .iter()
            .position(|chunk| record_type(&chunk.bytes) == Some(RecordType::PageEnd))
            .unwrap();
        assert_eq!(envelope.chunks[page_end_index].position, pos(0, p - 46));
        assert_eq!(envelope.chunks[page_end_index].bytes.len(), 46);
        assert_eq!(envelope.vlog_end, pos(0, p + 184));
        assert_eq!(planner.position(), envelope.vlog_end);
        assert_envelope_invariants(&envelope, g, (3, 2, 1, 3));
    });
}

/// N6: a PageEnd fired right before the footer.
#[test]
fn envelope_page_end_before_footer() {
    for_each_geometry(|g| {
        let p = g.page_size;
        let start = p - 225;
        let mut planner = LayoutPlanner::from_position(g, pos(0, start)).unwrap();
        let ops = [delete_op(b"x")];
        let envelope = prepare(&mut planner, 18, &ops).unwrap();

        assert_eq!(envelope.vlog_begin, pos(0, start));
        assert_eq!(
            record_positions(&envelope, RecordType::TxBegin),
            vec![pos(0, start)]
        );
        assert_eq!(
            record_positions(&envelope, RecordType::DeleteRecord),
            vec![pos(0, start + 71)]
        );
        let page_end_index = envelope
            .chunks
            .iter()
            .position(|chunk| record_type(&chunk.bytes) == Some(RecordType::PageEnd))
            .unwrap();
        assert_eq!(envelope.chunks[page_end_index].position, pos(0, p - 100));
        assert_eq!(envelope.chunks[page_end_index].bytes.len(), 100);
        assert_eq!(
            record_positions(&envelope, RecordType::TxPreparedEnd),
            vec![pos(0, p + OTHER_PAGE_RECORD_AREA_OFFSET)]
        );
        assert_eq!(envelope.vlog_end, pos(0, p + 127));
        assert_eq!(planner.position(), envelope.vlog_end);
        assert_envelope_invariants(&envelope, g, (1, 0, 1, 1));
    });
}

/// N7: the footer ends exactly at the page boundary.
#[test]
fn envelope_footer_ends_exactly_at_page_boundary() {
    for_each_geometry(|g| {
        let p = g.page_size;
        let start = p - 236;
        let mut planner = LayoutPlanner::from_position(g, pos(0, start)).unwrap();
        let ops = [delete_op(b"x")];
        let envelope = prepare(&mut planner, 19, &ops).unwrap();

        assert_eq!(envelope.vlog_begin, pos(0, start));
        assert_eq!(
            record_positions(&envelope, RecordType::TxPreparedEnd),
            vec![pos(0, start + 125)]
        );
        assert_eq!(envelope.vlog_end, pos(0, p));
        assert!(
            envelope
                .chunks
                .iter()
                .all(|chunk| record_type(&chunk.bytes) != Some(RecordType::PageEnd))
        );
        assert_eq!(planner.position(), envelope.vlog_end);
        assert_envelope_invariants(&envelope, g, (1, 0, 1, 1));
    });
}

/// N8: the footer ends exactly at the file boundary.
#[test]
fn envelope_footer_ends_exactly_at_file_boundary() {
    for_each_geometry(|g| {
        let f = g.max_file_size;
        let start = f - 236;
        let mut planner = LayoutPlanner::from_position(g, pos(0, start)).unwrap();
        let ops = [delete_op(b"x")];
        let envelope = prepare(&mut planner, 20, &ops).unwrap();

        assert_eq!(envelope.vlog_begin, pos(0, start));
        assert_eq!(
            record_positions(&envelope, RecordType::TxPreparedEnd),
            vec![pos(0, start + 125)]
        );
        assert_eq!(envelope.vlog_end, pos(0, f));
        assert_eq!(planner.position(), envelope.vlog_end);
        assert_envelope_invariants(&envelope, g, (1, 0, 1, 1));
    });
}

/// N9: an envelope starting inside the last file exhausts capacity mid-way.
#[test]
fn envelope_exhausts_capacity_mid_way_in_last_file() {
    for_each_geometry(|g| {
        let (f, m) = (g.max_file_size, g.max_file_id);
        let start = pos(m, f - 339);
        let prefix_ops = [put_op(b"k", b"v"), put_op(b"l", b"w")];
        let mut prefix_planner = LayoutPlanner::from_position(g, start).unwrap();
        let prefix = prepare(&mut prefix_planner, 21, &prefix_ops).unwrap();
        assert_eq!(prefix.vlog_begin, start);
        assert_eq!(prefix.vlog_end, pos(m, f - u64::from(PAGE_END_MIN_SIZE)));
        assert_eq!(prefix_planner.position(), prefix.vlog_end);
        assert_envelope_invariants(&prefix, g, (2, 2, 0, 2));

        let mut planner = LayoutPlanner::from_position(g, start).unwrap();
        let value = [0xab; 144];
        let ops = [put_op(b"k", b"v"), put_op(b"l", b"w"), put_op(b"m", &value)];
        assert_kind(
            prepare(&mut planner, 21, &ops),
            StorageErrorKind::CapacityExceeded,
        );
        assert_eq!(planner.position(), start);
    });
}

/// N10: a precise multi-file envelope. Pinned to geometry (512, 2048, 2) with
/// eight 496-byte Puts (value 440, one-byte keys) that each exactly fill a later
/// page, crossing file 0 -> 1 -> 2. Asserts the full 28-chunk stream with exact
/// positions, page numbers and page-end lengths.
#[test]
fn envelope_crossing_three_files_with_exact_chunks() {
    let geometry = VLogGeometry::test_only(512, 2_048, 2).unwrap();
    let n = 8;
    let value_len = (geometry.page_size - 72) as usize;

    let keys: Vec<Vec<u8>> = (0..n).map(|i| vec![b'a' + i as u8]).collect();
    let values: Vec<Vec<u8>> = (0..n).map(|_| vec![0x5a; value_len]).collect();
    let ops: Vec<LogicalOperationRef<'_>> = keys
        .iter()
        .zip(values.iter())
        .map(|(key, value)| put_op(key, value))
        .collect();

    let mut planner = LayoutPlanner::empty(geometry).unwrap();
    let envelope = prepare(&mut planner, 22, &ops).unwrap();

    let expected: Vec<(VLogPosition, ChunkKind)> = vec![
        (pos(0, 0), ChunkKind::PageHeader(0)),
        (pos(0, 16), ChunkKind::FileHeader),
        (pos(0, 64), ChunkKind::Record(RecordType::TxBegin)),
        (pos(0, 135), ChunkKind::PageEnd(377)),
        (pos(0, 512), ChunkKind::PageHeader(1)),
        (pos(0, 528), ChunkKind::Record(RecordType::KvRecord)),
        (pos(0, 1024), ChunkKind::PageHeader(2)),
        (pos(0, 1040), ChunkKind::Record(RecordType::KvRecord)),
        (pos(0, 1536), ChunkKind::PageHeader(3)),
        (pos(0, 1552), ChunkKind::Record(RecordType::KvRecord)),
        (pos(1, 0), ChunkKind::PageHeader(0)),
        (pos(1, 16), ChunkKind::FileHeader),
        (pos(1, 64), ChunkKind::PageEnd(448)),
        (pos(1, 512), ChunkKind::PageHeader(1)),
        (pos(1, 528), ChunkKind::Record(RecordType::KvRecord)),
        (pos(1, 1024), ChunkKind::PageHeader(2)),
        (pos(1, 1040), ChunkKind::Record(RecordType::KvRecord)),
        (pos(1, 1536), ChunkKind::PageHeader(3)),
        (pos(1, 1552), ChunkKind::Record(RecordType::KvRecord)),
        (pos(2, 0), ChunkKind::PageHeader(0)),
        (pos(2, 16), ChunkKind::FileHeader),
        (pos(2, 64), ChunkKind::PageEnd(448)),
        (pos(2, 512), ChunkKind::PageHeader(1)),
        (pos(2, 528), ChunkKind::Record(RecordType::KvRecord)),
        (pos(2, 1024), ChunkKind::PageHeader(2)),
        (pos(2, 1040), ChunkKind::Record(RecordType::KvRecord)),
        (pos(2, 1536), ChunkKind::PageHeader(3)),
        (pos(2, 1552), ChunkKind::Record(RecordType::TxPreparedEnd)),
    ];
    let actual: Vec<(VLogPosition, ChunkKind)> = envelope
        .chunks
        .iter()
        .map(|chunk| (chunk.position, chunk_kind(chunk, geometry)))
        .collect();
    assert_eq!(actual, expected);

    let mut kv_index = 0_usize;
    let mut saw_begin = false;
    let mut saw_footer = false;
    for chunk in &envelope.chunks {
        let Some(record_type) = record_type(&chunk.bytes) else {
            continue;
        };
        let header = RecordHeader::decode(&chunk.bytes).unwrap();
        assert_eq!(header.commit_seq, 22);
        assert_eq!(header.tx_uuid, tx_uuid());
        match decode_record_at(&chunk.bytes, chunk.position, geometry).unwrap() {
            DecodedRecord::TxBegin(begin) => {
                assert!(!saw_begin);
                saw_begin = true;
                assert_eq!(record_type, RecordType::TxBegin);
                assert_eq!(begin.commit_seq, 22);
                assert_eq!(begin.tx_uuid, tx_uuid());
                assert_eq!(begin.vlog_begin, pos(0, 0));
                assert_eq!(begin.logical_op_count, 8);
                assert_eq!(begin.distinct_key_count, 8);
            }
            DecodedRecord::KvRecord(record) => {
                assert_eq!(record_type, RecordType::KvRecord);
                assert!(kv_index < n);
                assert_eq!(record.commit_seq, 22);
                assert_eq!(record.tx_uuid, tx_uuid());
                assert_eq!(record.op_index, kv_index as u64);
                assert_eq!(record.key, keys[kv_index].as_slice());
                assert_eq!(record.value, values[kv_index].as_slice());

                let pointer = envelope.value_pointers[kv_index].as_ref().unwrap();
                assert_eq!(pointer.format_version, VALUE_POINTER_FORMAT_VERSION);
                assert_eq!(pointer.file_id, chunk.position.file_id);
                assert_eq!(u64::from(pointer.record_offset), chunk.position.offset);
                assert_eq!(pointer.record_len as usize, chunk.bytes.len());
                assert_eq!(usize::from(pointer.value_len), values[kv_index].len());
                kv_index += 1;
            }
            DecodedRecord::TxPreparedEnd(footer) => {
                assert!(!saw_footer);
                saw_footer = true;
                assert_eq!(record_type, RecordType::TxPreparedEnd);
                assert_eq!(footer.commit_seq, 22);
                assert_eq!(footer.tx_uuid, tx_uuid());
                assert_eq!(footer.vlog_begin, pos(0, 0));
                assert_eq!(footer.vlog_end, pos(2, 1663));
                assert_eq!(footer.logical_op_count, 8);
                assert_eq!(footer.distinct_key_count, 8);
                assert_eq!(footer.kv_record_count, 8);
                assert_eq!(footer.delete_record_count, 0);
                assert_eq!(footer.envelope_crc32c, envelope.envelope_crc32c);
            }
            DecodedRecord::PageEnd => {
                assert_eq!(record_type, RecordType::PageEnd);
            }
            DecodedRecord::DeleteRecord(_) => panic!("unexpected DELETE_RECORD"),
        }
    }
    assert!(saw_begin);
    assert!(saw_footer);
    assert_eq!(kv_index, n);
    assert_eq!(envelope.value_pointers.len(), n);

    assert_eq!(envelope.vlog_begin, pos(0, 0));
    assert_eq!(envelope.vlog_end, pos(2, 1663));
    assert_eq!(planner.position(), envelope.vlog_end);
    assert_envelope_invariants(&envelope, geometry, (8, 8, 0, 8));
}
