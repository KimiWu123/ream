use std::{fmt::Display, sync::Arc};

use alloy_primitives::B256;
use ream_da::{
    availability::DaAvailability,
    column::{DaContext, VerifiedColumn},
    error::DaStoreError,
    id::{ALL_COLUMNS_MASK, DaColumnId, NUMBER_OF_COLUMNS, column_indices},
    store::{DaReadStore, DaWriteStore, InsertOutcome},
};
use redb::{Database, Durability, ReadableDatabase, ReadableMultimapTable, ReadableTable};
use tracing::{debug, info, trace, warn};

use crate::tables::{
    da::{
        availability::{AvailabilityTable, BlockEntry},
        data_column_sidecar::DataColumnSidecarTable,
        retention_floor::RetentionFloorField,
        slot_index::DATA_AVAILABILITY_SLOT_INDEX_MULTIMAP_TABLE,
    },
    field::REDBField,
    table::REDBTable,
};

fn column_bit(column_index: u64) -> u128 {
    debug_assert!(column_index < NUMBER_OF_COLUMNS);
    1u128 << column_index
}

fn backend(err: impl Display) -> DaStoreError {
    DaStoreError::Backend(err.to_string())
}

/// redb-backed DA store. Only [`crate::db::ReamDB::init_da_db`] constructs
/// one, so holding a `DaDB` proves every DA table exists in the database.
#[derive(Clone, Debug)]
pub struct DaDB {
    pub db: Arc<Database>,
}

impl DaDB {
    fn read_block_entry(&self, block_root: B256) -> Result<Option<BlockEntry>, DaStoreError> {
        let read_txn = self.db.begin_read().map_err(backend)?;
        let availability = read_txn
            .open_table(AvailabilityTable::TABLE_DEFINITION)
            .map_err(backend)?;
        Ok(availability
            .get(block_root)
            .map_err(backend)?
            .map(|entry| entry.value()))
    }

    fn read_retention_floor(&self) -> Result<u64, DaStoreError> {
        let read_txn = self.db.begin_read().map_err(backend)?;
        let floor_table = read_txn
            .open_table(RetentionFloorField::FIELD_DEFINITION)
            .map_err(backend)?;
        Ok(floor_table
            .get(RetentionFloorField::KEY)
            .map_err(backend)?
            .map(|floor| floor.value())
            .unwrap_or(0))
    }
}

impl DaReadStore for DaDB {
    /// `Ok(None)` is "not present"; `Err` is an actual backend failure.
    fn get(&self, id: &DaColumnId) -> Result<Option<VerifiedColumn>, DaStoreError> {
        // An out-of-range index must not be shifted into the bitmap.
        if id.index() >= NUMBER_OF_COLUMNS {
            return Ok(None);
        }

        let read_txn = self.db.begin_read().map_err(backend)?;

        let availability = read_txn
            .open_table(AvailabilityTable::TABLE_DEFINITION)
            .map_err(backend)?;
        let Some(entry) = availability
            .get(id.block_root())
            .map_err(backend)?
            .map(|entry| entry.value())
        else {
            return Ok(None);
        };
        if entry.held & column_bit(id.index()) == 0 {
            return Ok(None);
        }

        let sidecars = read_txn
            .open_table(DataColumnSidecarTable::TABLE_DEFINITION)
            .map_err(backend)?;
        let Some(payload) = sidecars
            .get((id.block_root(), id.index()))
            .map_err(backend)?
            .map(|payload| payload.value())
        else {
            // Bitmap and payload are written in one transaction, so a set bit
            // without a payload row is corruption, not a race.
            warn!(
                "availability bitmap references a column with no stored payload: index={} block_root={:x}",
                id.index(),
                id.block_root()
            );
            return Ok(None);
        };

        // Everything in the store was verified before it was written.
        Ok(Some(VerifiedColumn::new_unchecked(
            *id,
            DaContext { slot: entry.slot },
            payload,
        )))
    }

    fn availability(&self, block_root: B256) -> Result<DaAvailability, DaStoreError> {
        let held = self
            .read_block_entry(block_root)?
            .map(|entry| entry.held)
            .unwrap_or(0);
        // Full-custody MVP: every column is expected. Custody groups would
        // pass the node's actual custody set here instead.
        Ok(DaAvailability::new(held, ALL_COLUMNS_MASK))
    }

    fn get_retention_floor(&self) -> u64 {
        match self.read_retention_floor() {
            Ok(floor) => floor,
            Err(err) => {
                warn!("failed to read the retention floor: {err}; treating as no floor");
                0
            }
        }
    }

    fn is_below_retention(&self, slot: u64) -> bool {
        slot < self.get_retention_floor()
    }
}

impl DaWriteStore for DaDB {
    /// Idempotent per id: a duplicate put keeps the stored column.
    fn put(&self, column: VerifiedColumn) -> Result<InsertOutcome, DaStoreError> {
        let id = column.id();
        let slot = column.context().slot;
        let block_root = id.block_root();
        let column_index = id.index();

        let mut write_txn = self.db.begin_write().map_err(backend)?;
        write_txn
            .set_durability(Durability::Immediate)
            .map_err(backend)?;

        {
            let floor_table = write_txn
                .open_table(RetentionFloorField::FIELD_DEFINITION)
                .map_err(backend)?;
            let floor = floor_table
                .get(RetentionFloorField::KEY)
                .map_err(backend)?
                .map(|floor| floor.value())
                .unwrap_or(0);

            // Early returns drop the transaction without committing
            if slot < floor {
                return Ok(InsertOutcome::BelowRetention);
            }

            let mut availability = write_txn
                .open_table(AvailabilityTable::TABLE_DEFINITION)
                .map_err(backend)?;
            // A block's slot is fixed by its first stored column
            let mut entry = availability
                .get(block_root)
                .map_err(backend)?
                .map(|entry| entry.value())
                .unwrap_or(BlockEntry { slot, held: 0 });
            if entry.held & column_bit(column_index) != 0 {
                trace!("ignoring duplicate column index={column_index} block_root={block_root:x}");
                return Ok(InsertOutcome::Duplicated);
            }

            let mut sidecars = write_txn
                .open_table(DataColumnSidecarTable::TABLE_DEFINITION)
                .map_err(backend)?;
            sidecars
                .insert((block_root, column_index), column.payload().to_vec())
                .map_err(backend)?;

            entry.held |= column_bit(column_index);
            availability.insert(block_root, entry).map_err(backend)?;

            let mut slot_index = write_txn
                .open_multimap_table(DATA_AVAILABILITY_SLOT_INDEX_MULTIMAP_TABLE)
                .map_err(backend)?;
            slot_index.insert(entry.slot, block_root).map_err(backend)?;
        }
        write_txn.commit().map_err(backend)?;

        debug!("stored column index={column_index} slot={slot} block_root={block_root:x}");
        Ok(InsertOutcome::Inserted)
    }

    fn prune_below_slot(&self, slot: u64) -> Result<usize, DaStoreError> {
        let mut write_txn = self.db.begin_write().map_err(backend)?;
        write_txn
            .set_durability(Durability::Immediate)
            .map_err(backend)?;

        let removed = {
            let mut floor_table = write_txn
                .open_table(RetentionFloorField::FIELD_DEFINITION)
                .map_err(backend)?;
            let floor = floor_table
                .get(RetentionFloorField::KEY)
                .map_err(backend)?
                .map(|floor| floor.value())
                .unwrap_or(0);
            if slot < floor {
                info!(
                    "ignoring retention hint below the current floor: hint slot {slot}, floor {floor}"
                );
                return Ok(0);
            }
            floor_table
                .insert(RetentionFloorField::KEY, slot)
                .map_err(backend)?;

            let mut slot_index = write_txn
                .open_multimap_table(DATA_AVAILABILITY_SLOT_INDEX_MULTIMAP_TABLE)
                .map_err(backend)?;
            // Collect first: a table cannot be mutated while iterated.
            let stale: Vec<(u64, B256)> = {
                let mut stale = Vec::new();
                for entry in slot_index.range(..slot).map_err(backend)? {
                    let (stale_slot, roots) = entry.map_err(backend)?;
                    let stale_slot = stale_slot.value();
                    for root in roots {
                        stale.push((stale_slot, root.map_err(backend)?.value()));
                    }
                }
                stale
            };

            let mut availability = write_txn
                .open_table(AvailabilityTable::TABLE_DEFINITION)
                .map_err(backend)?;
            let mut sidecars = write_txn
                .open_table(DataColumnSidecarTable::TABLE_DEFINITION)
                .map_err(backend)?;
            let mut removed = 0;
            for (stale_slot, block_root) in &stale {
                slot_index.remove(stale_slot, block_root).map_err(backend)?;
                let Some(entry) = availability
                    .remove(block_root)
                    .map_err(backend)?
                    .map(|entry| entry.value())
                else {
                    continue;
                };
                for index in column_indices(entry.held) {
                    if sidecars
                        .remove((*block_root, index))
                        .map_err(backend)?
                        .is_some()
                    {
                        removed += 1;
                    }
                }
            }
            removed
        };
        write_txn.commit().map_err(backend)?;
        Ok(removed)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::{Path, PathBuf},
        sync::atomic::{AtomicU64, Ordering},
    };

    use alloy_primitives::B256;
    use ream_da::{
        column::{DaContext, VerifiedColumn},
        id::DaColumnId,
        store::{DaReadStore, DaWriteStore, InsertOutcome},
    };

    use super::DaDB;
    use crate::db::ReamDB;

    fn temp_root() -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        std::env::temp_dir().join(format!("ream-da-db-test-{pid}-{n}"))
    }

    fn open_store(root: &Path) -> DaDB {
        fs::create_dir_all(root).expect("create db dir");
        let ream_db = ReamDB::new(root.to_path_buf()).expect("open database");
        ream_db.init_da_db().expect("init DA tables")
    }

    fn sample_column(block_root: B256, index: u64, slot: u64, payload: &[u8]) -> VerifiedColumn {
        let id = DaColumnId::new(block_root, index).expect("index within range");
        VerifiedColumn::new_unchecked(id, DaContext { slot }, payload.to_vec())
    }

    #[test]
    fn put_stores_a_column_and_get_returns_it() {
        let root = temp_root();
        let store = open_store(&root);
        let column = sample_column(B256::repeat_byte(1), 3, 42, b"payload-bytes");
        let id = column.id();

        assert_eq!(
            store.put(column.clone()).expect("put succeeds"),
            InsertOutcome::Inserted
        );
        assert_eq!(store.get(&id).expect("get succeeds"), Some(column));
        assert!(
            store
                .availability(id.block_root())
                .expect("availability")
                .holds(3)
        );

        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn put_records_multiple_columns_of_a_block() {
        let root = temp_root();
        let store = open_store(&root);
        let block_root = B256::repeat_byte(4);

        store
            .put(sample_column(block_root, 2, 30, b"col-2"))
            .expect("put column 2");
        store
            .put(sample_column(block_root, 5, 30, b"col-5"))
            .expect("put column 5");

        assert_eq!(
            store
                .availability(block_root)
                .expect("availability")
                .held_indices(),
            vec![2, 5]
        );

        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn put_existing_id_is_duplicate_and_keeps_original() {
        let root = temp_root();
        let store = open_store(&root);
        let block_root = B256::repeat_byte(3);
        let id = DaColumnId::new(block_root, 0).expect("index within range");

        let first = store
            .put(sample_column(block_root, 0, 10, b"original"))
            .expect("first put");
        // Same id with different bytes, then with a different slot
        let second = store
            .put(sample_column(block_root, 0, 10, b"tampered"))
            .expect("second put");
        let third = store
            .put(sample_column(block_root, 0, 11, b"original"))
            .expect("third put");

        assert_eq!(first, InsertOutcome::Inserted);
        assert_eq!(second, InsertOutcome::Duplicated);
        assert_eq!(third, InsertOutcome::Duplicated);

        let fetched = store.get(&id).expect("get succeeds").expect("present");
        assert_eq!(fetched.payload(), b"original");
        assert_eq!(fetched.context().slot, 10);

        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn get_unknown_id_returns_none() {
        let root = temp_root();
        let store = open_store(&root);
        let id = DaColumnId::new(B256::repeat_byte(2), 1).expect("index within range");

        assert_eq!(store.get(&id).expect("get succeeds"), None);

        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn prune_below_slot_leaves_no_stale_availability() {
        let root = temp_root();
        let store = open_store(&root);
        let block = B256::repeat_byte(9);
        for index in [0u64, 5, 7] {
            store
                .put(sample_column(block, index, 8, b"x"))
                .expect("put");
        }
        assert_eq!(
            store
                .availability(block)
                .expect("availability")
                .held_count(),
            3
        );

        assert_eq!(store.prune_below_slot(100).expect("prune"), 3);

        assert_eq!(
            store
                .availability(block)
                .expect("availability")
                .held_count(),
            0
        );
        for index in [0u64, 5, 7] {
            let id = DaColumnId::new(block, index).expect("valid index");
            assert_eq!(store.get(&id).expect("get"), None);
        }

        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn prune_removes_every_fork_block_at_a_slot() {
        let root = temp_root();
        let store = open_store(&root);
        // Two competing blocks at the same slot: the slot index is a multimap
        // exactly so the losing fork's columns are not orphaned by a prune.
        let fork_a = B256::repeat_byte(1);
        let fork_b = B256::repeat_byte(2);
        store.put(sample_column(fork_a, 0, 10, b"a")).expect("put");
        store.put(sample_column(fork_b, 0, 10, b"b")).expect("put");

        assert_eq!(store.prune_below_slot(11).expect("prune"), 2);
        assert_eq!(
            store
                .availability(fork_a)
                .expect("availability")
                .held_count(),
            0
        );
        assert_eq!(
            store
                .availability(fork_b)
                .expect("availability")
                .held_count(),
            0
        );

        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn put_refuses_columns_below_the_retention_floor() {
        let root = temp_root();
        let store = open_store(&root);
        store.prune_below_slot(15).expect("set floor");

        // Strictly below the floor: refused, and nothing is stored.
        let stale = sample_column(B256::repeat_byte(1), 0, 14, b"stale");
        let id = stale.id();
        assert_eq!(
            store.put(stale).expect("put"),
            InsertOutcome::BelowRetention
        );
        assert_eq!(store.get(&id).expect("get"), None);
        assert_eq!(
            store
                .availability(id.block_root())
                .expect("availability")
                .held_count(),
            0
        );

        // Exactly at the floor: accepted (the floor keeps slot >= floor).
        let at_floor = sample_column(B256::repeat_byte(2), 1, 15, b"kept");
        assert_eq!(store.put(at_floor).expect("put"), InsertOutcome::Inserted);

        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn retention_hint_below_the_floor_never_lowers_it() {
        let root = temp_root();
        let store = open_store(&root);
        store.prune_below_slot(100).expect("raise floor");

        // A lower hint is a no-op: nothing pruned, floor unchanged...
        assert_eq!(store.prune_below_slot(50).expect("lower hint"), 0);
        assert_eq!(store.get_retention_floor(), 100);
        drop(store);

        // ...including the persisted copy: the lower hint must not reach the database
        let reopened = open_store(&root);
        assert_eq!(reopened.get_retention_floor(), 100);

        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn prune_below_slot_removes_old_keeps_recent() {
        let root = temp_root();
        let store = open_store(&root);

        let old = B256::repeat_byte(1);
        let recent = B256::repeat_byte(2);
        store.put(sample_column(old, 0, 10, b"old-0")).expect("put");
        store.put(sample_column(old, 4, 10, b"old-4")).expect("put");
        store
            .put(sample_column(recent, 1, 20, b"recent-1"))
            .expect("put");

        let removed = store.prune_below_slot(15).expect("prune");
        assert_eq!(removed, 2, "both old columns are removed");

        let old_0 = DaColumnId::new(old, 0).expect("valid index");
        let old_4 = DaColumnId::new(old, 4).expect("valid index");
        assert_eq!(store.get(&old_0).expect("get"), None);
        assert_eq!(store.get(&old_4).expect("get"), None);
        assert_eq!(
            store.availability(old).expect("availability").held_count(),
            0
        );

        let recent_1 = DaColumnId::new(recent, 1).expect("valid index");
        assert!(store.get(&recent_1).expect("get").is_some());
        assert_eq!(
            store
                .availability(recent)
                .expect("availability")
                .held_count(),
            1
        );

        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn prune_below_slot_keeps_block_exactly_at_cutoff() {
        let root = temp_root();
        let store = open_store(&root);
        let block = B256::repeat_byte(7);
        store
            .put(sample_column(block, 0, 32, b"at-cutoff"))
            .expect("put");

        // slot 32 is not < 32, so a cutoff equal to the slot keeps the block.
        assert_eq!(store.prune_below_slot(32).expect("prune"), 0);
        assert_eq!(
            store
                .availability(block)
                .expect("availability")
                .held_count(),
            1
        );

        assert_eq!(store.prune_below_slot(33).expect("prune"), 1);
        assert_eq!(
            store
                .availability(block)
                .expect("availability")
                .held_count(),
            0
        );

        fs::remove_dir_all(&root).ok();
    }
}
