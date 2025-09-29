use std::io::{Error as IoError, ErrorKind};

use serde::{Deserialize, Serialize, de::DeserializeOwned};

pub type IoResult<T> = Result<T, IoError>;

pub type RpcResult<T> = Result<T, RpcError>;

/// Errors that occur during requests
#[derive(Debug)]
pub enum Error<T> {
    RawMessage(String),
    Io(IoError),
    Custom(T),
}

impl<T> Error<T> {
    pub fn get_custom(&self) -> Option<&T> {
        let Self::Custom(value) = self else {
            return None;
        };

        Some(value)
    }
}

impl<T> From<IoError> for Error<T> {
    fn from(error: IoError) -> Self {
        Self::Io(error)
    }
}

impl<T: DeserializeOwned> From<RpcError> for Error<T> {
    fn from(error: RpcError) -> Self {
        match error {
            RpcError::RawMessage(e) => Self::RawMessage(e),
            RpcError::Io(e) => Self::Io(e.into()),
            RpcError::Custom(e) => serde_json::from_str::<T>(&e)
                .map(Self::Custom)
                .unwrap_or_else(|_| Self::RawMessage(e)),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct RpcIoError {
    kind: String,
    message: String,
}

impl From<IoError> for RpcIoError {
    fn from(error: IoError) -> Self {
        Self {
            kind: format!("{:?}", error.kind()),
            message: error.to_string(),
        }
    }
}

impl Into<IoError> for RpcIoError {
    fn into(self) -> IoError {
        IoError::new(
            match self.kind.as_str() {
                "AddrInUse" => ErrorKind::AddrInUse,
                "AddrNotAvailable" => ErrorKind::AddrNotAvailable,
                "AlreadyExists" => ErrorKind::AlreadyExists,
                "ArgumentListTooLong" => ErrorKind::ArgumentListTooLong,
                "BrokenPipe" => ErrorKind::BrokenPipe,
                "ConnectionAborted" => ErrorKind::ConnectionAborted,
                "ConnectionRefused" => ErrorKind::ConnectionRefused,
                "ConnectionReset" => ErrorKind::ConnectionReset,
                "CrossesDevices" => ErrorKind::CrossesDevices,
                "Deadlock" => ErrorKind::Deadlock,
                "DirectoryNotEmpty" => ErrorKind::DirectoryNotEmpty,
                "ExecutableFileBusy" => ErrorKind::ExecutableFileBusy,
                "FileTooLarge" => ErrorKind::FileTooLarge,
                "HostUnreachable" => ErrorKind::HostUnreachable,
                "Interrupted" => ErrorKind::Interrupted,
                "InvalidData" => ErrorKind::InvalidData,
                "InvalidInput" => ErrorKind::InvalidInput,
                "IsADirectory" => ErrorKind::IsADirectory,
                "network down" => ErrorKind::NetworkDown,
                "NetworkDown" => ErrorKind::NetworkUnreachable,
                "NotADirectory" => ErrorKind::NotADirectory,
                "NotConnected" => ErrorKind::NotConnected,
                "NotFound" => ErrorKind::NotFound,
                "NotSeekable" => ErrorKind::NotSeekable,
                "Other" => ErrorKind::Other,
                "OutOfMemory" => ErrorKind::OutOfMemory,
                "PermissionDenied" => ErrorKind::PermissionDenied,
                "QuotaExceeded" => ErrorKind::QuotaExceeded,
                "ReadOnlyFilesystem" => ErrorKind::ReadOnlyFilesystem,
                "ResourceBusy" => ErrorKind::ResourceBusy,
                "StaleNetworkFileHandle" => ErrorKind::StaleNetworkFileHandle,
                "StorageFull" => ErrorKind::StorageFull,
                "TimedOut" => ErrorKind::TimedOut,
                "TooManyLinks" => ErrorKind::TooManyLinks,
                "UnexpectedEof" => ErrorKind::UnexpectedEof,
                "unsupported" => ErrorKind::Unsupported,
                "WouldBlock" => ErrorKind::WouldBlock,
                "WriteZero" => ErrorKind::WriteZero,
                _ => ErrorKind::Other,
            },
            self.message,
        )
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub enum RpcError {
    RawMessage(String),
    Io(RpcIoError),
    Custom(String),
}

impl std::error::Error for RpcError {}

impl std::fmt::Display for RpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl RpcError {
    pub fn not_found(message: &str) -> Self {
        Self::Io(IoError::new(ErrorKind::NotFound, message).into())
    }

    pub fn invalid_data(message: &str) -> Self {
        Self::Io(IoError::new(ErrorKind::InvalidData, message).into())
    }

    pub fn invalid_stream() -> Self {
        Self::Io(IoError::new(ErrorKind::InvalidInput, "InvalidStream").into())
    }

    pub fn invalid_stream_with_message(message: &str) -> Self {
        Self::Io(IoError::new(ErrorKind::InvalidInput, message).into())
    }

    pub fn terminated() -> Self {
        Self::Io(IoError::new(ErrorKind::ConnectionAborted, "Terminated").into())
    }

    pub fn unknown() -> Self {
        Self::Io(IoError::new(ErrorKind::Other, "Unknown error").into())
    }

    pub fn timeout(message: &str) -> Self {
        Self::Io(IoError::new(ErrorKind::TimedOut, message).into())
    }

    pub fn internal(message: &str) -> Self {
        Self::RawMessage(message.to_string())
    }

    pub fn already_exists(message: &str) -> Self {
        Self::Io(IoError::new(ErrorKind::AlreadyExists, message).into())
    }

    pub fn unsupported(message: &str) -> Self {
        Self::Io(IoError::new(ErrorKind::Unsupported, message).into())
    }

    pub fn to_string(&self) -> String {
        serde_json::to_string(self).unwrap()
    }

    pub fn into_error<T: DeserializeOwned>(self) -> Error<T> {
        Error::from(self)
    }
}

impl From<IoError> for RpcError {
    fn from(error: IoError) -> Self {
        Self::Io(error.into())
    }
}

impl From<String> for RpcError {
    fn from(value: String) -> Self {
        serde_json::from_str(&value).unwrap_or_else(|_| Self::RawMessage(value))
    }
}

impl<T: Serialize> From<Error<T>> for RpcError {
    fn from(value: Error<T>) -> Self {
        match value {
            Error::Custom(e) => Self::Custom(serde_json::to_string(&e).unwrap()),
            Error::RawMessage(e) => Self::RawMessage(e),
            Error::Io(e) => Self::Io(e.into()),
        }
    }
}
