use serde::{Deserialize, Serialize};

use crate::id::DaColumnId;

/// Opaque, scheme-specific encoding of a DA column payload together with its
/// availability evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaPayload(Vec<u8>);

/// Consensus-derived context attached to a candidate column.
///
/// Only plain data crosses this boundary; no beacon runtime handles.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct DaContext {
    /// Slot of the block the column belongs to. Used for retention decisions,
    /// never for fork choice.
    pub slot: u64,
}

/// A column that passed verification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaColumn {
    id: DaColumnId,
    context: DaContext,
    payload: DaPayload,
}
