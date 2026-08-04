use alloy_primitives::B256;
use redb::MultimapTableDefinition;

use crate::tables::ssz_encoder::SSZEncoding;

/// Table definition for the Data Availability Slot Index Multimap table
///
/// Key: slot number
/// Value: block_root's
///
/// A multimap because before finality one slot can carry several competing
/// blocks; a 1:1 table would drop the losing fork's columns from pruning.
pub(crate) const DATA_AVAILABILITY_SLOT_INDEX_MULTIMAP_TABLE: MultimapTableDefinition<
    u64,
    SSZEncoding<B256>,
> = MultimapTableDefinition::new("da_slot_index_multimap");
