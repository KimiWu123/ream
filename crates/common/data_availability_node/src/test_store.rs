use std::{
    collections::{BTreeMap, HashMap},
    sync::{RwLock, RwLockReadGuard, RwLockWriteGuard},
};

use alloy_primitives::B256;
use ream_data_availability::{
    availability::ColumnAvailability,
    column::{ColumnContext, VerifiedColumn},
    error::ColumnStoreError,
    id::{ALL_COLUMNS_MASK, ColumnId, NUMBER_OF_COLUMNS},
    store::{ColumnReadStore, ColumnWriteStore, InsertOutcome},
};

/// A block's slot plus its stored columns, keyed by column index.
#[derive(Debug, Default)]
struct BlockEntry {
    slot: u64,
    columns: BTreeMap<u64, Vec<u8>>,
}

impl BlockEntry {
    /// Presence bitmap derived from the stored indices, so it can never drift
    /// out of sync with the payloads themselves.
    fn held(&self) -> u128 {
        self.columns
            .keys()
            .fold(0u128, |bits, index| bits | 1u128 << index)
    }
}

#[derive(Debug, Default)]
struct State {
    blocks: HashMap<B256, BlockEntry>,
    retention_floor: u64,
}

/// Non-persistent [`ColumnWriteStore`] for this crate's tests
#[derive(Debug, Default)]
pub struct MemoryColumnStore {
    state: RwLock<State>,
}

impl MemoryColumnStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Recovers from a poisoned lock instead of panicking.
    fn read(&self) -> RwLockReadGuard<'_, State> {
        self.state
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn write(&self) -> RwLockWriteGuard<'_, State> {
        self.state
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl ColumnReadStore for MemoryColumnStore {
    fn get(&self, id: &ColumnId) -> Result<Option<VerifiedColumn>, ColumnStoreError> {
        let state = self.read();
        let Some(entry) = state.blocks.get(&id.block_root()) else {
            return Ok(None);
        };
        // Everything in the store was verified before it was written.
        Ok(entry.columns.get(&id.index()).map(|payload| {
            VerifiedColumn::new_unchecked(*id, ColumnContext { slot: entry.slot }, payload.clone())
        }))
    }

    fn availability(&self, block_root: B256) -> Result<ColumnAvailability, ColumnStoreError> {
        let held = self
            .read()
            .blocks
            .get(&block_root)
            .map(BlockEntry::held)
            .unwrap_or(0);
        // Full-custody MVP: every column is expected. Custody groups would
        // pass the node's actual custody set here instead.
        Ok(ColumnAvailability::new(held, ALL_COLUMNS_MASK))
    }

    fn get_retention_floor(&self) -> u64 {
        self.read().retention_floor
    }

    fn is_below_retention(&self, slot: u64) -> bool {
        slot < self.get_retention_floor()
    }
}

impl ColumnWriteStore for MemoryColumnStore {
    fn put(&self, column: VerifiedColumn) -> Result<InsertOutcome, ColumnStoreError> {
        let id = column.id();
        // An out-of-range index must not be shifted into the bitmap.
        if id.index() >= NUMBER_OF_COLUMNS {
            return Ok(InsertOutcome::Duplicated);
        }

        let mut state = self.write();
        let slot = column.context().slot;
        if slot < state.retention_floor {
            return Ok(InsertOutcome::BelowRetention);
        }

        // A block's slot is fixed by its first stored column.
        let entry = state.blocks.entry(id.block_root()).or_insert(BlockEntry {
            slot,
            columns: BTreeMap::new(),
        });
        if entry.columns.contains_key(&id.index()) {
            return Ok(InsertOutcome::Duplicated);
        }
        entry.columns.insert(id.index(), column.payload().to_vec());
        Ok(InsertOutcome::Inserted)
    }

    fn prune_below_slot(&self, slot: u64) -> Result<usize, ColumnStoreError> {
        let mut state = self.write();
        if slot < state.retention_floor {
            return Ok(0);
        }
        state.retention_floor = slot;

        let mut removed = 0;
        state.blocks.retain(|_, entry| {
            if entry.slot < slot {
                removed += entry.columns.len();
                false
            } else {
                true
            }
        });
        Ok(removed)
    }
}
