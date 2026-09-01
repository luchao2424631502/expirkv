#ifndef KV_BENCH_LEVELDB_AGGREGATE_H_
#define KV_BENCH_LEVELDB_AGGREGATE_H_

#include <stddef.h>
#include <stdint.h>

#include "leveldb/c.h"

#ifdef __cplusplus
extern "C" {
#endif

enum {
  BENCH_LEVELDB_BATCH_PUT = 1,
  BENCH_LEVELDB_BATCH_DELETE = 2,
  BENCH_LEVELDB_SCAN_TIMED = 0,
  BENCH_LEVELDB_SCAN_FULL = 1,
};

typedef struct bench_leveldb_batch_item {
  uint8_t kind;
  const char* key;
  size_t key_length;
  const char* value;
  size_t value_length;
} bench_leveldb_batch_item;

typedef struct bench_leveldb_expected_record {
  const char* key;
  size_t key_length;
  const char* value;
  size_t value_length;
} bench_leveldb_expected_record;

typedef struct bench_leveldb_scan_result {
  size_t record_count;
  size_t value_bytes;
} bench_leveldb_scan_result;

void bench_leveldb_write_batch(leveldb_t* db,
                               const leveldb_writeoptions_t* options,
                               const bench_leveldb_batch_item* items,
                               size_t item_count, char** error);

void bench_leveldb_iterator_scan(
    leveldb_t* db, const leveldb_readoptions_t* options, const char* start,
    size_t start_length, size_t limit, uint8_t validation_mode,
    size_t expected_value_length,
    const bench_leveldb_expected_record* expected, size_t expected_count,
    bench_leveldb_scan_result* result, char** error);

#ifdef __cplusplus
}
#endif

#endif
