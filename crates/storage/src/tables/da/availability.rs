use std::sync::Arc;

use alloy_primitives::B256;
use redb::{Database, TableDefinition};

use crate::tables::{ssz_encoder::SSZEncoding, table::REDBTable};

pub struct AvailabilityTable {
    pub db: Arc<Database>,
}

/// Table definition for the DA availability table
///
/// Key: block_root
/// Value: held-columns bitmap
impl REDBTable for AvailabilityTable {
    const TABLE_DEFINITION: TableDefinition<'_, SSZEncoding<B256>, u128> =
        TableDefinition::new("da_availability");

    type Key = B256;

    type KeyTableDefinition = SSZEncoding<B256>;

    type Value = u128;

    type ValueTableDefinition = u128;

    fn database(&self) -> Arc<Database> {
        self.db.clone()
    }
}
