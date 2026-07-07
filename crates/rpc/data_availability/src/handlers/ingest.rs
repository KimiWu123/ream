use actix_web::{
    HttpMessage, HttpRequest, HttpResponse, Responder, post,
    web::{self, Bytes, Data, Path, Query},
};
use alloy_primitives::B256;
use ream_api_types_common::{error::ApiError, id::ID};
use ream_data_availability::{
    column::{CandidateBlock, CandidateColumn, ColumnContext},
    id::ColumnId,
};
use ream_data_availability_node::{error::IngestionError, ingest::IngestHandle};
use serde::Deserialize;
use ssz::Decode;
use ssz_derive::{Decode as SszDecode, Encode as SszEncode};
use ssz_types::{VariableList, typenum};

use crate::handlers::block_root_from_id;

/// Byte ceiling for a batch-ingest request body. Generous headroom over a full
/// block at today's blob limits (~6 MB); the SSZ list bounds below cap the
/// shape more precisely.
pub const MAX_BATCH_INGEST_BODY_BYTES: usize = 1 << 25; // 32 MiB

/// JSON body of `POST /data/v0/ingest`; the payload travels as a hex string.
#[derive(Deserialize)]
pub struct IngestRequest {
    block_root: B256,
    index: u64,
    slot: u64,
    payload: String,
}

impl IngestRequest {
    fn into_candidate(self) -> Result<CandidateColumn, ApiError> {
        let id = ColumnId::new(self.block_root, self.index)
            .map_err(|err| ApiError::BadRequest(format!("invalid column id: {err}")))?;
        let payload = alloy_primitives::hex::decode(&self.payload)
            .map_err(|err| ApiError::BadRequest(format!("payload is not valid hex: {err}")))?;
        Ok(CandidateColumn {
            id,
            context: ColumnContext { slot: self.slot },
            payload,
        })
    }
}

/// Map a queue submission error onto the RPC's status codes.
///
/// Transient backpressure (a full queue) is a retryable 503; a vanished
/// verification service while the RPC is still up is a genuine internal
/// fault (500) that no retry will fix.
fn submission_error(err: IngestionError) -> ApiError {
    match err {
        IngestionError::Overloaded => {
            ApiError::ServiceUnavailable("verification queue is full; retry shortly".to_string())
        }
        IngestionError::Closed => {
            ApiError::InternalError("verification service is unavailable".to_string())
        }
    }
}

/// `POST /data/v0/ingest` — admit a candidate column into the verification pipeline.
///
/// Uses the non-blocking [`IngestHandle::try_submit`] so a full queue sheds
/// load instead of blocking the request. The handler performs no verification;
/// it only decodes, validates the envelope, and hands the candidate off.
#[post("/ingest")]
pub async fn post_ingest(
    handle: Data<IngestHandle>,
    body: web::Json<IngestRequest>,
) -> Result<impl Responder, ApiError> {
    let candidate = body.into_inner().into_candidate()?;
    handle.try_submit(candidate).map_err(submission_error)?;
    Ok(HttpResponse::Accepted().finish())
}

/// One `(column index, payload)` entry of the SSZ batch-ingest body. The
/// payload stays opaque here, exactly as in the JSON envelope.
#[derive(Debug, Clone, PartialEq, Eq, SszEncode, SszDecode)]
pub struct WireIndexedPayload {
    pub index: u64,
    /// Opaque column payload; the bound is a per-column byte ceiling, not a
    /// scheme statement.
    pub payload: VariableList<u8, typenum::U16777216>,
}

/// SSZ body of `POST /data/v0/ingest/block/{block_root}`: at most one payload
/// per column of one block (the list bound mirrors `NUMBER_OF_COLUMNS`).
pub type WireBlockBatch = VariableList<WireIndexedPayload, typenum::U128>;

/// Query string of `POST /data/v0/ingest/block/{block_root}`.
#[derive(Deserialize)]
pub struct BlockIngestQuery {
    /// Slot of the block the columns belong to — consensus context the node
    /// cannot read out of the opaque payloads itself.
    slot: u64,
}

/// `POST /data/v0/ingest/block/{block_root}?slot={slot}` — admit a whole block's
/// columns in one request.
///
/// The body is SSZ over `application/octet-stream`: a list of
/// `(column index, payload)` entries, payloads verbatim — no hex, no JSON.
/// Metadata the node cannot derive from opaque payloads (block root, slot)
/// travels in the URL. The batch is queued or refused as one unit, through the
/// same load-shedding [`IngestHandle::try_submit_block`] path as the
/// single-column endpoint.
#[post("/ingest/block/{block_root}")]
pub async fn post_ingest_block(
    handle: Data<IngestHandle>,
    block_root: Path<ID>,
    query: Query<BlockIngestQuery>,
    request: HttpRequest,
    body: Bytes,
) -> Result<impl Responder, ApiError> {
    if request.content_type() != "application/octet-stream" {
        return Err(ApiError::UnsupportedMediaType(format!(
            "batch ingest takes an SSZ body as application/octet-stream, got `{}`",
            request.content_type()
        )));
    }

    let block_root = block_root_from_id(block_root.into_inner())?;
    let entries = WireBlockBatch::from_ssz_bytes(&body)
        .map_err(|err| ApiError::BadRequest(format!("body is not a valid SSZ batch: {err:?}")))?;

    let columns = entries
        .into_iter()
        .map(|entry| (entry.index, entry.payload.to_vec()))
        .collect();
    // CandidateBlock::new re-checks the batch shape (empty, range, duplicates),
    // so a malformed batch dies here with a 400 instead of inside the pipeline.
    let block = CandidateBlock::new(block_root, ColumnContext { slot: query.slot }, columns)
        .map_err(|err| ApiError::BadRequest(format!("invalid batch: {err}")))?;

    handle.try_submit_block(block).map_err(submission_error)?;
    Ok(HttpResponse::Accepted().finish())
}
