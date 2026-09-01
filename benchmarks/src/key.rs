//! Fixed key encoding and deterministic value bytes.

use crate::{BenchConfig, SplitMix64, mix64};

pub const KEY_LENGTH: usize = 16;
const NAMESPACE_LENGTH: usize = 8;
const VALUE_SEED_DOMAIN: u64 = 0x6b76_5f76_616c_7565;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KeyCodecError {
    WrongLength { actual: usize },
    NonZeroNamespace,
    IdOutOfRange { id: u64, record_count: u64 },
}

/// Encodes `[8 zero namespace bytes][8-byte big-endian logical id]`.
pub fn encode_key(config: &BenchConfig, id: u64) -> Result<[u8; KEY_LENGTH], KeyCodecError> {
    if id >= config.record_count() {
        return Err(KeyCodecError::IdOutOfRange {
            id,
            record_count: config.record_count(),
        });
    }
    let mut encoded = [0_u8; KEY_LENGTH];
    encoded[NAMESPACE_LENGTH..].copy_from_slice(&id.to_be_bytes());
    Ok(encoded)
}

/// Decodes and validates the fixed key representation.
pub fn decode_key(config: &BenchConfig, encoded: &[u8]) -> Result<u64, KeyCodecError> {
    if encoded.len() != KEY_LENGTH {
        return Err(KeyCodecError::WrongLength {
            actual: encoded.len(),
        });
    }
    if encoded[..NAMESPACE_LENGTH] != [0; NAMESPACE_LENGTH] {
        return Err(KeyCodecError::NonZeroNamespace);
    }
    let id = u64::from_be_bytes(
        encoded[NAMESPACE_LENGTH..]
            .try_into()
            .expect("key length was validated"),
    );
    if id >= config.record_count() {
        return Err(KeyCodecError::IdOutOfRange {
            id,
            record_count: config.record_count(),
        });
    }
    Ok(id)
}

/// Generates the one deterministic Value shared by every logical record.
pub fn fixed_value(config: &BenchConfig) -> Vec<u8> {
    let value_seed = mix64(config.seed().wrapping_add(VALUE_SEED_DOMAIN));
    let mut random = SplitMix64::new(value_seed);
    let mut value = Vec::with_capacity(config.value_length());
    while value.len() < config.value_length() {
        let bytes = random.next_u64().to_le_bytes();
        let remaining = config.value_length() - value.len();
        value.extend_from_slice(&bytes[..remaining.min(bytes.len())]);
    }
    value
}
