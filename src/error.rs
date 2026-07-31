use std::{error, fmt, io};

/// Errors returned by RustKV.
#[derive(Debug)]
pub enum Error {
    InvalidArgument(String),
    NotFound,
    Io(io::Error),
    Corruption(String),
    Busy(String),
    Unsupported(String),
    CapacityExceeded(String),
    Durability { committed: bool, message: String },
    Background(String),
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidArgument(message) => write!(formatter, "invalid argument: {message}"),
            Self::NotFound => formatter.write_str("not found"),
            Self::Io(error) => write!(formatter, "I/O error: {error}"),
            Self::Corruption(message) => write!(formatter, "corruption: {message}"),
            Self::Busy(message) => write!(formatter, "database busy: {message}"),
            Self::Unsupported(message) => write!(formatter, "unsupported: {message}"),
            Self::CapacityExceeded(message) => {
                write!(formatter, "capacity exceeded: {message}")
            }
            Self::Durability { committed, message } => write!(
                formatter,
                "durability error (committed={committed}): {message}"
            ),
            Self::Background(message) => write!(formatter, "background error: {message}"),
        }
    }
}

impl error::Error for Error {
    // 自定义错误有源头就交出去
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

/// Result type used by RustKV APIs.
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::{Error, Result}; // 使用自定义的Error Result
    use std::{error::Error as _, io};

    #[test]
    fn display_includes_the_error_category_and_message() {
        let cases = [
            (
                Error::InvalidArgument("bad option".into()),
                "invalid argument: bad option",
            ),
            (Error::NotFound, "not found"),
            (
                Error::Corruption("bad checksum".into()),
                "corruption: bad checksum",
            ),
            (Error::Busy("locked".into()), "database busy: locked"),
            (Error::Unsupported("feature".into()), "unsupported: feature"),
            (
                Error::CapacityExceeded("file ids".into()),
                "capacity exceeded: file ids",
            ),
            (
                Error::Durability {
                    committed: true,
                    message: "sync failed".into(),
                },
                "durability error (committed=true): sync failed",
            ),
            (
                Error::Background("worker failed".into()),
                "background error: worker failed",
            ),
        ];

        for (error, expected) in cases {
            assert_eq!(error.to_string(), expected);
            assert!(error.source().is_none()); // std::io::Error source is not none
        }
    }

    #[test]
    fn io_conversion_preserves_the_source() {
        // 单独测试底层错误源
        let error = Error::from(io::Error::new(io::ErrorKind::PermissionDenied, "denied"));

        assert_eq!(error.to_string(), "I/O error: denied");
        assert_eq!(
            error
                .source()
                .and_then(|source| source.downcast_ref::<io::Error>())
                .map(io::Error::kind),
            Some(io::ErrorKind::PermissionDenied)
        );
    }

    #[test]
    fn result_alias_uses_rustkv_error() {
        fn fail() -> Result<()> {
            Err(Error::NotFound)
        }

        assert!(matches!(fail(), Err(Error::NotFound)));
    }
}
