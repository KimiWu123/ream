use alloy_primitives::B256;
use serde::{Deserialize, Serialize};

use crate::{error::ValidationError, id::ColumnId};

/// Consensus-derived context attached to a candidate column.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ColumnContext {
    /// Slot of the block the column belongs to; used for retention only.
    pub slot: u64,
}

/// A candidate column submitted for verification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CandidateColumn {
    pub id: ColumnId,
    pub context: ColumnContext,
    pub payload: Vec<u8>,
}

/// A whole block's worth of candidate columns, submitted as one unit.
///
/// The block root and context are stored exactly once, so a batch that mixes
/// blocks or slots is structurally unrepresentable — the invariant that lets a
/// verifier check every column against one header and lets the pipeline treat
/// "a block" as its unit of work. Construction also rejects out-of-range and
/// duplicate column indices, so a batch is well-formed before any verification
/// runs. Because each of the `NUMBER_OF_COLUMNS` indices can appear at most
/// once, a batch can never exceed one block's column count.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CandidateBlock {
    block_root: B256,
    context: ColumnContext,
    /// `(column index, payload)` pairs, in submission order.
    columns: Vec<(u64, Vec<u8>)>,
}

impl CandidateBlock {
    /// Build a batch from `(column index, payload)` pairs.
    pub fn new(
        block_root: B256,
        context: ColumnContext,
        columns: Vec<(u64, Vec<u8>)>,
    ) -> Result<Self, ValidationError> {
        if columns.is_empty() {
            return Err(ValidationError::EmptyBatch);
        }
        let mut seen = 0u128;
        for (index, _) in &columns {
            // Range-check by constructing the id: same rule, same error as the
            // single-column path.
            ColumnId::new(block_root, *index)?;
            let bit = 1u128 << *index;
            if seen & bit != 0 {
                return Err(ValidationError::DuplicateColumnIndex {
                    column_index: *index,
                });
            }
            seen |= bit;
        }
        Ok(Self {
            block_root,
            context,
            columns,
        })
    }

    pub fn block_root(&self) -> B256 {
        self.block_root
    }

    pub fn context(&self) -> ColumnContext {
        self.context
    }

    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Column indices present in this batch, in submission order.
    pub fn column_indices(&self) -> impl Iterator<Item = u64> + '_ {
        self.columns.iter().map(|(index, _)| *index)
    }

    /// Explode into per-column candidates, in submission order.
    ///
    /// Id construction cannot fail here: every index was validated by
    /// [`CandidateBlock::new`].
    pub fn into_columns(self) -> impl Iterator<Item = CandidateColumn> {
        let block_root = self.block_root;
        let context = self.context;
        self.columns.into_iter().map(move |(index, payload)| {
            let id = ColumnId::new(block_root, index)
                .expect("batch indices are validated at construction");
            CandidateColumn {
                id,
                context,
                payload,
            }
        })
    }

    /// Decompose into `(block_root, context, columns)`, the inverse of
    /// [`CandidateBlock::new`] — for callers that filter the batch (e.g. drop
    /// already-held columns) and rebuild it.
    pub fn into_parts(self) -> (B256, ColumnContext, Vec<(u64, Vec<u8>)>) {
        (self.block_root, self.context, self.columns)
    }
}

/// A column that passed verification — the only type accepted by
/// `ColumnWriteStore`, and only constructed by `ColumnVerifier` implementations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedColumn {
    id: ColumnId,
    context: ColumnContext,
    payload: Vec<u8>,
}

impl VerifiedColumn {
    pub fn new_unchecked(id: ColumnId, context: ColumnContext, payload: Vec<u8>) -> Self {
        Self {
            id,
            context,
            payload,
        }
    }

    pub fn id(&self) -> ColumnId {
        self.id
    }

    pub fn context(&self) -> ColumnContext {
        self.context
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
}
