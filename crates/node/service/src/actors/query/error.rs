use std::sync::mpsc::SendError;

use thiserror::Error;

/// Errors for `InboundActor`.
#[derive(Error, Debug)]
pub enum L1QueryActorError<T> {
    /// Error that occurs when sending.
    #[error("Error sending the head update event: {0}")]
    SendError(#[from] SendError<T>),
    /// Error in the transport layer.
    #[error("Default error")]
    Error(String),
}

/// Errors for `InboundBuilder`.
#[derive(Error, Debug)]
pub enum L1QueryActorBuilderError {
    /// Any error that can happen when calling `build` method.
    #[error("build error")]
    BuildError(String),
}
