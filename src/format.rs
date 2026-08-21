//! Root FORMAT metadata.
#![allow(dead_code)] // Stage 2 codec; production consumers are wired in later stages.

use crate::{Operation, ProtocolStage, Result, RetryAdvice, StorageError, StorageErrorKind};

pub(crate) const FORMAT_MAGIC: [u8; 8] = *b"RUSTKV00";
pub(crate) const FORMAT_VERSION: u32 = 0;
pub(crate) const FORMAT_ENCODED_LEN: usize = 36;
pub(crate) const VLOG_PAGE_SIZE: u32 = 65_536;
pub(crate) const MAX_KEY_VALUE_SIZE: u32 = 60_000;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FormatMetadataV0 {
    pub(crate) magic: [u8; 8],
    pub(crate) format_version: u32,
    pub(crate) database_uuid: [u8; 16],
    pub(crate) page_size: u32,
    pub(crate) max_key_value_size: u32,
}

impl FormatMetadataV0 {
    pub(crate) fn new(database_uuid: [u8; 16]) -> Result<Self> {
        let metadata = Self {
            magic: FORMAT_MAGIC,
            format_version: FORMAT_VERSION,
            database_uuid,
            page_size: VLOG_PAGE_SIZE,
            max_key_value_size: MAX_KEY_VALUE_SIZE,
        };
        metadata.validate_for_encode()?;
        Ok(metadata)
    }

    pub(crate) fn encode(&self) -> Result<[u8; FORMAT_ENCODED_LEN]> {
        self.validate_for_encode()?;

        let mut encoded = [0_u8; FORMAT_ENCODED_LEN];
        encoded[0..8].copy_from_slice(&self.magic);
        encoded[8..12].copy_from_slice(&self.format_version.to_le_bytes());
        encoded[12..28].copy_from_slice(&self.database_uuid);
        encoded[28..32].copy_from_slice(&self.page_size.to_le_bytes());
        encoded[32..36].copy_from_slice(&self.max_key_value_size.to_le_bytes());
        Ok(encoded)
    }

    pub(crate) fn decode(encoded: &[u8]) -> Result<Self> {
        if encoded.len() != FORMAT_ENCODED_LEN {
            return Err(decode_error(StorageErrorKind::Corruption));
        }
        if encoded[0..8] != FORMAT_MAGIC {
            return Err(decode_error(StorageErrorKind::Corruption));
        }

        let format_version = u32::from_le_bytes(
            encoded[8..12]
                .try_into()
                .map_err(|_| decode_error(StorageErrorKind::Corruption))?,
        );
        if format_version != FORMAT_VERSION {
            return Err(decode_error(StorageErrorKind::IncompatibleFormat));
        }

        let database_uuid = encoded[12..28]
            .try_into()
            .map_err(|_| decode_error(StorageErrorKind::Corruption))?;
        let page_size = u32::from_le_bytes(
            encoded[28..32]
                .try_into()
                .map_err(|_| decode_error(StorageErrorKind::Corruption))?,
        );
        let max_key_value_size = u32::from_le_bytes(
            encoded[32..36]
                .try_into()
                .map_err(|_| decode_error(StorageErrorKind::Corruption))?,
        );
        if database_uuid == [0; 16]
            || page_size != VLOG_PAGE_SIZE
            || max_key_value_size != MAX_KEY_VALUE_SIZE
        {
            return Err(decode_error(StorageErrorKind::Corruption));
        }

        Ok(Self {
            magic: FORMAT_MAGIC,
            format_version,
            database_uuid,
            page_size,
            max_key_value_size,
        })
    }

    fn validate_for_encode(&self) -> Result<()> {
        if self.magic != FORMAT_MAGIC
            || self.format_version != FORMAT_VERSION
            || self.database_uuid == [0; 16]
            || self.page_size != VLOG_PAGE_SIZE
            || self.max_key_value_size != MAX_KEY_VALUE_SIZE
        {
            return Err(encode_error());
        }
        Ok(())
    }
}

fn encode_error() -> StorageError {
    StorageError::codec_error(
        StorageErrorKind::InvalidArgument,
        Operation::Open,
        ProtocolStage::Preflight,
        None,
        RetryAdvice::FixRequestAndRetrySameInstance,
    )
}

fn decode_error(kind: StorageErrorKind) -> StorageError {
    let retry_advice = if kind == StorageErrorKind::IncompatibleFormat {
        RetryAdvice::DoNotRetry
    } else {
        RetryAdvice::RestoreOrRepair
    };
    StorageError::codec_error(
        kind,
        Operation::Open,
        ProtocolStage::Recovery,
        None,
        retry_advice,
    )
}
