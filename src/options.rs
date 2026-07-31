use crate::{Error, Result};

const MIB: usize = 1024 * 1024;
const MIN_OPEN_FILES: usize = 10;
const MIN_BLOCK_SIZE: usize = 1024; // 最小1KB
const MAX_BLOCK_SIZE: usize = MIB; // 最大1MB
const MAX_BLOCK_RESTART_INTERVAL: usize = u8::MAX as usize;

/// Compression applied to index data blocks.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Compression {
    NoCompression,
    Lz4,
}

/// Database-wide configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Options {
    pub create_if_missing: bool,
    pub error_if_exists: bool,
    pub write_buffer_size: usize,
    pub max_open_files: usize,
    pub block_cache_size: usize,
    pub block_size: usize,
    pub block_restart_interval: usize,
    pub max_file_size: usize,
    pub compression: Compression,
}

impl Default for Options {
    fn default() -> Self {
        let options = Self {
            create_if_missing: false,
            error_if_exists: false,
            write_buffer_size: 4 * MIB,              // MemTable Size
            max_open_files: 1000,                    // Fd Limit
            block_cache_size: 8 * MIB,               // LRU cache Size
            block_size: 4 * 1024,                    // SSTable Block Size
            block_restart_interval: 16,              //
            max_file_size: 2 * MIB,                  // SSTable File Size
            compression: Compression::NoCompression, // 默认不开启SST压缩
        };
        debug_assert!(options.validate().is_ok());
        options
    }
}

impl Options {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.write_buffer_size == 0 {
            return Err(Error::InvalidArgument(
                "write_buffer_size must be greater than 0".into(),
            ));
        }

        if self.max_open_files < MIN_OPEN_FILES {
            return Err(Error::InvalidArgument(format!(
                "max_open_files must be at least {MIN_OPEN_FILES}"
            )));
        }

        if !(MIN_BLOCK_SIZE..=MAX_BLOCK_SIZE).contains(&self.block_size) {
            return Err(Error::InvalidArgument(format!(
                "block_size must be between {MIN_BLOCK_SIZE} and {MAX_BLOCK_SIZE} bytes"
            )));
        }

        if !(1..=MAX_BLOCK_RESTART_INTERVAL).contains(&self.block_restart_interval) {
            return Err(Error::InvalidArgument(format!(
                "block_restart_interval must be between 1 and {MAX_BLOCK_RESTART_INTERVAL}"
            )));
        }

        if self.max_file_size == 0 {
            return Err(Error::InvalidArgument(
                "max_file_size must be greater than 0".into(),
            ));
        }

        Ok(())
    }
}

/// Per-write configuration.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WriteOptions {
    pub sync: bool,
}

#[cfg(test)]
mod tests {
    use super::{
        Compression, MAX_BLOCK_RESTART_INTERVAL, MAX_BLOCK_SIZE, MIN_BLOCK_SIZE, MIN_OPEN_FILES,
        Options, WriteOptions,
    };
    use crate::Error;

    #[test]
    fn defaults_match_the_public_design() {
        let options = Options::default();

        assert!(!options.create_if_missing);
        assert!(!options.error_if_exists);
        assert_eq!(options.write_buffer_size, 4 * 1024 * 1024);
        assert_eq!(options.max_open_files, 1000);
        assert_eq!(options.block_cache_size, 8 * 1024 * 1024);
        assert_eq!(options.block_size, 4 * 1024);
        assert_eq!(options.block_restart_interval, 16);
        assert_eq!(options.max_file_size, 2 * 1024 * 1024);
        assert_eq!(options.compression, Compression::NoCompression);
        assert_eq!(WriteOptions::default(), WriteOptions { sync: false });
        assert!(options.validate().is_ok());
    }

    #[test]
    fn clone_is_an_independent_configuration_value() {
        let original = Options::default();
        let mut cloned = original.clone();
        cloned.create_if_missing = true;
        cloned.compression = Compression::Lz4;

        assert!(!original.create_if_missing);
        assert_eq!(original.compression, Compression::NoCompression);
        assert!(cloned.create_if_missing);
        assert_eq!(cloned.compression, Compression::Lz4);
    }

    #[test]
    fn block_cache_may_be_disabled() {
        let options = Options {
            block_cache_size: 0,
            ..Options::default()
        };

        assert!(options.validate().is_ok());
    }

    #[test]
    fn valid_option_boundaries_are_accepted() {
        for options in [
            Options {
                write_buffer_size: 1,
                ..Options::default()
            },
            Options {
                max_open_files: MIN_OPEN_FILES,
                ..Options::default()
            },
            Options {
                block_size: MIN_BLOCK_SIZE,
                ..Options::default()
            },
            Options {
                block_size: MAX_BLOCK_SIZE,
                ..Options::default()
            },
            Options {
                block_restart_interval: 1,
                ..Options::default()
            },
            Options {
                block_restart_interval: MAX_BLOCK_RESTART_INTERVAL,
                ..Options::default()
            },
            Options {
                max_file_size: 1,
                ..Options::default()
            },
        ] {
            assert!(options.validate().is_ok(), "{options:?}");
        }
    }

    #[test]
    fn invalid_option_boundaries_return_invalid_argument() {
        let cases = [
            Options {
                write_buffer_size: 0,
                ..Options::default()
            },
            Options {
                max_open_files: MIN_OPEN_FILES - 1,
                ..Options::default()
            },
            Options {
                block_size: MIN_BLOCK_SIZE - 1,
                ..Options::default()
            },
            Options {
                block_size: MAX_BLOCK_SIZE + 1,
                ..Options::default()
            },
            Options {
                block_restart_interval: 0,
                ..Options::default()
            },
            Options {
                block_restart_interval: MAX_BLOCK_RESTART_INTERVAL + 1,
                ..Options::default()
            },
            Options {
                max_file_size: 0,
                ..Options::default()
            },
        ];

        for options in cases {
            assert!(
                matches!(options.validate(), Err(Error::InvalidArgument(_))),
                "{options:?}"
            );
        }
    }
}
