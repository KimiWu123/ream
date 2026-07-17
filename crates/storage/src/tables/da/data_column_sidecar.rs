use std::sync::Arc;

use alloy_primitives::B256;
use redb::{Database, TableDefinition};

use crate::tables::{ssz_encoder::SSZEncoding, table::REDBTable};

pub struct DataColumnSidecarTable {
    pub db: Arc<Database>,
}

/// Table definition for the Data Column Sidecar table
///
/// Key: (block_root, column_index)
/// Value: opaque column payload bytes
impl REDBTable for DataColumnSidecarTable {
    const TABLE_DEFINITION: TableDefinition<'_, (SSZEncoding<B256>, u64), SSZEncoding<Vec<u8>>> =
        TableDefinition::new("da_column_sidecar");

    type Key = (B256, u64);

    type KeyTableDefinition = (SSZEncoding<B256>, u64);

    type Value = Vec<u8>;

    type ValueTableDefinition = SSZEncoding<Vec<u8>>;

    fn database(&self) -> Arc<Database> {
        self.db.clone()
    }
}
