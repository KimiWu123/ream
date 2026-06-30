use std::io;

use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ValidationError {
    #[error("column index {column_index} is outside 0..{number_of_columns}")]
    InvalidColumnIndex {
        column_index: u64,
        number_of_columns: u64,
    },

    #[error("payload format is incorrect: {0}")]
    MalformedPayload(String),

    #[error("id mismatch: expected:{expected} \n actual:{actual}")]
    IdMismatch_ { expected: String, actual: String },

    #[error("commitment is empty")]
    EmptyCommitments,

    #[error("the commitment length: {count} is over max_blobs_per_block: {maximum}")]
    TooManyCommitments { count: usize, maximum: usize },

    #[error(
        "the content of the data sidecar is inconsistent, cell: {cells}, commitment: {commitments}, proof:{proofs}"
    )]
    LengthMismatch {
        cells: usize,
        commitments: usize,
        proofs: usize,
    },

    #[error("invalid proof")]
    InvalidProof,

    #[error("verify failed: {0}")]
    VerifierFailure(String),
}

#[derive(Debug, Error)]
pub enum DaStoreError {
    /// Underlying storage failure: filesystem I/O, a missing backing file, or
    /// corruption. Not a normal "not found" answer — that is `Ok(None)`.
    #[error("storage I/O failure: {0}")]
    Io(#[from] io::Error),
}
