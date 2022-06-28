use std::fmt::Display;
use crate::{ClientId, parser::ParseError};

/// This enum represents any error that might occur during this program
#[derive(Debug, Clone)]
pub enum Error {
    Runtime(RuntimeError),
    Parse(ParseError),
    Other(String),
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Runtime(x) => f.write_fmt(format_args!("{}", x)),
            Error::Parse(x) => f.write_fmt(format_args!("{}", x)),
            Error::Other(x) => f.write_fmt(format_args!("{}", x)),
        }
    }
}

impl From<RuntimeError> for Error {
    fn from(e: RuntimeError) -> Self {
        Self::Runtime(e)
    }
}

impl From<ParseError> for Error {
    fn from(e: ParseError) -> Self {
        Self::Parse(e)
    }
}

impl From<String> for Error {
    fn from(e: String) -> Self {
        Self::Other(e)
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::Other(format!("IO Error: {e}"))
    }
}

impl From<Error> for String {
    fn from(e: Error) -> Self {
        format!("{e}")
    }
}

#[derive(Debug, Clone)]
pub enum EngineError {
    ClientIdMismatch(ClientId, ClientId),
}

impl Display for EngineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EngineError::ClientIdMismatch(id1, id2) => {
                f.write_fmt(format_args!("Client Id Mismatch: {} != {}", id1, id2))
            }
        }
    }
}

/// This struct represents an `EngineError` occuring on a specific line.
#[derive(Debug, Clone)]
pub struct RuntimeError(pub usize, pub EngineError);

impl Display for RuntimeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("[line {}] {}", self.0, self.1))
    }
}

