use alloy_primitives::B256;
use ream_api_types_common::{error::ApiError, id::ID};

pub mod availability;
pub mod column;
pub mod ingest;
pub mod retention;

/// Resolve a request-path [`ID`] to a concrete block root.
///
/// The DA node stores by root and keeps no chain of its own, so it can only
/// honour [`ID::Root`]. The consensus-relative ids (`head`, `finalized`,
/// `genesis`, `justified`, `slot`) are the beacon's to resolve *before* it asks
/// the DA node — accepting them here would promise a lookup the node can't do.
pub(crate) fn block_root_from_id(id: ID) -> Result<B256, ApiError> {
    match id {
        ID::Root(root) => Ok(root),
        other => Err(ApiError::BadRequest(format!(
            "the DA node identifies blocks by root only; `{other}` is not supported"
        ))),
    }
}
