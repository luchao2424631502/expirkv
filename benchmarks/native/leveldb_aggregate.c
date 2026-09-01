#include "leveldb_aggregate.h"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

static void bench_set_error(char** error, const char* message, size_t length) {
  char* copy;
  if (error == NULL || *error != NULL) {
    return;
  }
  copy = (char*)malloc(length + 1);
  if (copy == NULL) {
    return;
  }
  memcpy(copy, message, length + 1);
  *error = copy;
}

static int bench_compare_bytes(const char* left, size_t left_length,
                               const char* right, size_t right_length) {
  size_t common = left_length < right_length ? left_length : right_length;
  int compared = common == 0 ? 0 : memcmp(left, right, common);
  if (compared != 0) {
    return compared;
  }
  if (left_length < right_length) {
    return -1;
  }
  if (left_length > right_length) {
    return 1;
  }
  return 0;
}

void bench_leveldb_write_batch(leveldb_t* db,
                               const leveldb_writeoptions_t* options,
                               const bench_leveldb_batch_item* items,
                               size_t item_count, char** error) {
  size_t index;
  leveldb_writebatch_t* batch = leveldb_writebatch_create();
  if (batch == NULL) {
    bench_set_error(error, "leveldb_writebatch_create returned null",
                    sizeof("leveldb_writebatch_create returned null") - 1);
    return;
  }
  for (index = 0; index < item_count; ++index) {
    const bench_leveldb_batch_item* item = &items[index];
    if (item->kind == BENCH_LEVELDB_BATCH_PUT) {
      leveldb_writebatch_put(batch, item->key, item->key_length, item->value,
                             item->value_length);
    } else if (item->kind == BENCH_LEVELDB_BATCH_DELETE) {
      leveldb_writebatch_delete(batch, item->key, item->key_length);
    } else {
      bench_set_error(error, "invalid benchmark LevelDB batch item kind",
                      sizeof("invalid benchmark LevelDB batch item kind") - 1);
      leveldb_writebatch_destroy(batch);
      return;
    }
  }
  leveldb_write(db, options, batch, error);
  leveldb_writebatch_destroy(batch);
}

void bench_leveldb_iterator_scan(
    leveldb_t* db, const leveldb_readoptions_t* options, const char* start,
    size_t start_length, size_t limit, uint8_t validation_mode,
    size_t expected_value_length,
    const bench_leveldb_expected_record* expected, size_t expected_count,
    bench_leveldb_scan_result* result, char** error) {
  leveldb_iterator_t* iterator;
  char* previous_key = NULL;
  size_t previous_capacity = 0;
  size_t previous_length = 0;

  result->record_count = 0;
  result->value_bytes = 0;
  if (validation_mode != BENCH_LEVELDB_SCAN_TIMED &&
      validation_mode != BENCH_LEVELDB_SCAN_FULL) {
    bench_set_error(error, "invalid benchmark LevelDB scan validation mode",
                    sizeof("invalid benchmark LevelDB scan validation mode") -
                        1);
    return;
  }

  iterator = leveldb_create_iterator(db, options);
  if (iterator == NULL) {
    bench_set_error(error, "leveldb_create_iterator returned null",
                    sizeof("leveldb_create_iterator returned null") - 1);
    return;
  }
  leveldb_iter_seek(iterator, start, start_length);
  while (result->record_count < limit && leveldb_iter_valid(iterator)) {
    size_t key_length = 0;
    size_t value_length = 0;
    const char* key = leveldb_iter_key(iterator, &key_length);
    const char* value = leveldb_iter_value(iterator, &value_length);
    size_t index = result->record_count;

    if (index == 0 &&
        bench_compare_bytes(key, key_length, start, start_length) < 0) {
      bench_set_error(error, "iterator returned a key below the seek target",
                      sizeof("iterator returned a key below the seek target") -
                          1);
      break;
    }
    if (index > 0 &&
        bench_compare_bytes(previous_key, previous_length, key, key_length) >=
            0) {
      bench_set_error(error, "iterator keys are not strictly increasing",
                      sizeof("iterator keys are not strictly increasing") - 1);
      break;
    }
    if (validation_mode == BENCH_LEVELDB_SCAN_TIMED) {
      if (value_length != expected_value_length) {
        bench_set_error(error, "iterator value length differs from expected",
                        sizeof("iterator value length differs from expected") -
                            1);
        break;
      }
    } else {
      const bench_leveldb_expected_record* record;
      if (index >= expected_count) {
        bench_set_error(error, "iterator returned an unexpected extra record",
                        sizeof("iterator returned an unexpected extra record") -
                            1);
        break;
      }
      record = &expected[index];
      if (bench_compare_bytes(key, key_length, record->key,
                              record->key_length) != 0) {
        bench_set_error(error, "iterator key differs from expected bytes",
                        sizeof("iterator key differs from expected bytes") - 1);
        break;
      }
      if (value_length != record->value_length ||
          (value_length > 0 &&
           memcmp(value, record->value, value_length) != 0)) {
        bench_set_error(error, "iterator value differs from expected bytes",
                        sizeof("iterator value differs from expected bytes") -
                            1);
        break;
      }
    }
    if (SIZE_MAX - result->value_bytes < value_length) {
      bench_set_error(error, "iterator value byte count overflowed",
                      sizeof("iterator value byte count overflowed") - 1);
      break;
    }
    if (key_length > previous_capacity) {
      char* resized = (char*)realloc(previous_key, key_length);
      if (resized == NULL && key_length != 0) {
        bench_set_error(error, "iterator previous-key allocation failed",
                        sizeof("iterator previous-key allocation failed") - 1);
        break;
      }
      previous_key = resized;
      previous_capacity = key_length;
    }
    if (key_length > 0) {
      memcpy(previous_key, key, key_length);
    }
    previous_length = key_length;
    result->value_bytes += value_length;
    result->record_count += 1;
    leveldb_iter_next(iterator);
  }

  if (*error == NULL) {
    leveldb_iter_get_error(iterator, error);
  }
  if (*error == NULL && validation_mode == BENCH_LEVELDB_SCAN_FULL &&
      result->record_count != expected_count) {
    bench_set_error(error, "iterator returned fewer records than expected",
                    sizeof("iterator returned fewer records than expected") -
                        1);
  }
  free(previous_key);
  leveldb_iter_destroy(iterator);
}
