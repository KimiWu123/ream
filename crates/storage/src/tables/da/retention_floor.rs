use std::sync::Arc;

use redb::{Database, TableDefinition};

use crate::tables::field::REDBField;

pub struct RetentionFloorField {
    pub db: Arc<Database>,
}

/// Field definition for the DA retention floor
///
/// Value: the lowest slot whose columns are still retained.
impl REDBField for RetentionFloorField {
    const FIELD_DEFINITION: TableDefinition<'_, &str, u64> =
        TableDefinition::new("da_retention_floor");

    const KEY: &str = "retention_floor";

    type Value = u64;

    type ValueFieldDefinition = u64;

    fn database(&self) -> Arc<Database> {
        self.db.clone()
    }
}
