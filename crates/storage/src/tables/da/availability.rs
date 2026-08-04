use std::sync::Arc;

use alloy_primitives::B256;
use redb::{Database, TableDefinition};
use ssz_derive::{Decode, Encode};

use crate::tables::{ssz_encoder::SSZEncoding, table::REDBTable};

/// A block's slot plus the bitmap of its held column indices; the slot is
/// recorded once per block here so a column read can recover it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub struct BlockEntry {
    pub slot: u64,
    pub held: u128,
}

pub struct AvailabilityTable {
    pub db: Arc<Database>,
}

/// Table definition for the DA availability table
///
/// Key: block_root
/// Value: the block's slot plus its held-columns bitmap
impl REDBTable for AvailabilityTable {
    const TABLE_DEFINITION: TableDefinition<'_, SSZEncoding<B256>, SSZEncoding<BlockEntry>> =
        TableDefinition::new("da_availability");

    type Key = B256;

    type KeyTableDefinition = SSZEncoding<B256>;

    type Value = BlockEntry;

    type ValueTableDefinition = SSZEncoding<BlockEntry>;

    fn database(&self) -> Arc<Database> {
        self.db.clone()
    }
}
