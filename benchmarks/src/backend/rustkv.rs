//! Direct RustKV public-API backend.

use std::path::Path;

use rustkv::{Compression, Db, Options, ReadOptions, WriteBatch, WriteOptions};

use crate::BenchConfig;

use super::{
    BackendError, BackendKind, BackendOperation, BackendResult, BatchItem, BenchBackend, GetResult,
    ScanRequest, ScanResult, ScanValidation,
};

pub struct RustKvBackend {
    db: Db,
    read_options: ReadOptions<'static>,
    write_options: WriteOptions,
}

impl RustKvBackend {
    pub fn open(path: impl AsRef<Path>, config: &BenchConfig) -> BackendResult<Self> {
        let options = Options {
            create_if_missing: true,
            error_if_exists: false,
            write_buffer_size: config.write_buffer_size(),
            max_open_files: config.max_open_files(),
            block_cache_size: config.block_cache_size(),
            block_size: config.block_size(),
            block_restart_interval: config.block_restart_interval(),
            max_file_size: config.max_table_file_size(),
            compression: Compression::NoCompression,
            ..Options::default()
        };
        let db = Db::open(&options, path)
            .map_err(|error| rustkv_error(BackendOperation::Open, error))?;
        Ok(Self {
            db,
            read_options: ReadOptions::default(),
            write_options: WriteOptions {
                sync: config.sync_writes(),
            },
        })
    }
}

impl BenchBackend for RustKvBackend {
    fn get(&self, key: &[u8]) -> BackendResult<GetResult> {
        self.db
            .get(&self.read_options, key)
            .map(|value| match value {
                Some(value) => GetResult {
                    found: true,
                    value_length: value.len(),
                },
                None => GetResult {
                    found: false,
                    value_length: 0,
                },
            })
            .map_err(|error| rustkv_error(BackendOperation::Get, error))
    }

    fn put(&self, key: &[u8], value: &[u8]) -> BackendResult<()> {
        self.db
            .put(&self.write_options, key, value)
            .map_err(|error| rustkv_error(BackendOperation::Put, error))
    }

    fn delete(&self, key: &[u8]) -> BackendResult<()> {
        self.db
            .delete(&self.write_options, key)
            .map_err(|error| rustkv_error(BackendOperation::Delete, error))
    }

    fn write_batch(&self, items: &[BatchItem<'_>]) -> BackendResult<()> {
        let mut batch = WriteBatch::new();
        for item in items {
            match item {
                BatchItem::Put { key, value } => batch.put(key, value),
                BatchItem::Delete { key } => batch.delete(key),
            }
            .map_err(|error| rustkv_error(BackendOperation::WriteBatch, error))?;
        }
        self.db
            .write(&self.write_options, &batch)
            .map_err(|error| rustkv_error(BackendOperation::WriteBatch, error))
    }

    fn iterator_scan(&self, request: ScanRequest<'_>) -> BackendResult<ScanResult> {
        let mut iterator = self
            .db
            .iter(&self.read_options)
            .map_err(|error| rustkv_error(BackendOperation::IteratorScan, error))?;
        iterator.seek(request.start);

        let mut previous_key = Vec::new();
        let mut record_count = 0_usize;
        let mut value_bytes = 0_usize;
        while record_count < request.limit && iterator.valid() {
            let key = iterator
                .key()
                .ok_or_else(|| validation_error("valid RustKV iterator returned no key"))?;
            let value = iterator
                .value()
                .ok_or_else(|| validation_error("valid RustKV iterator returned no value"))?;
            if record_count == 0 && key < request.start {
                return Err(validation_error(
                    "iterator returned a key below the seek target",
                ));
            }
            if record_count > 0 && previous_key.as_slice() >= key {
                return Err(validation_error(
                    "iterator keys are not strictly increasing",
                ));
            }
            validate_record(request.validation, record_count, key, value)?;
            value_bytes = value_bytes
                .checked_add(value.len())
                .ok_or_else(|| validation_error("iterator value byte count overflowed"))?;
            previous_key.clear();
            previous_key.extend_from_slice(key);
            record_count += 1;
            iterator.next();
        }
        iterator
            .status()
            .map_err(|error| borrowed_rustkv_error(BackendOperation::IteratorScan, error))?;
        if let ScanValidation::Full { expected } = request.validation
            && record_count != expected.len()
        {
            return Err(validation_error(format!(
                "iterator returned {record_count} records, expected {}",
                expected.len()
            )));
        }
        Ok(ScanResult {
            record_count,
            value_bytes,
        })
    }
}

fn validate_record(
    validation: ScanValidation<'_>,
    index: usize,
    key: &[u8],
    value: &[u8],
) -> BackendResult<()> {
    match validation {
        ScanValidation::Timed {
            expected_value_length,
        } => {
            if value.len() != expected_value_length {
                return Err(validation_error(format!(
                    "iterator value length {} does not equal {expected_value_length}",
                    value.len()
                )));
            }
        }
        ScanValidation::Full { expected } => {
            let record = expected
                .get(index)
                .ok_or_else(|| validation_error("iterator returned an unexpected extra record"))?;
            if key != record.key {
                return Err(validation_error("iterator key differs from expected bytes"));
            }
            if value != record.value {
                return Err(validation_error(
                    "iterator value differs from expected bytes",
                ));
            }
        }
    }
    Ok(())
}

fn rustkv_error(operation: BackendOperation, error: rustkv::StorageError) -> BackendError {
    let source_text = if error.message.is_empty() {
        format!("{error:?}")
    } else {
        error.message
    };
    BackendError::new(BackendKind::RustKv, operation, source_text)
}

fn borrowed_rustkv_error(
    operation: BackendOperation,
    error: &rustkv::StorageError,
) -> BackendError {
    let source_text = if error.message.is_empty() {
        format!("{error:?}")
    } else {
        error.message.clone()
    };
    BackendError::new(BackendKind::RustKv, operation, source_text)
}

fn validation_error(message: impl Into<String>) -> BackendError {
    BackendError::new(BackendKind::RustKv, BackendOperation::IteratorScan, message)
}
