use ream_consensus_beacon::data_column_sidecar::DataColumnSidecar;
use ream_consensus_misc::constants::beacon::{
    BLOB_KZG_COMMITMENTS_INDEX, DATA_COLUMN_SIDECAR_KZG_PROOF_DEPTH,
};
use ream_da::{
    column::{CandidateColumn, VerifiedColumn},
    error::ValidationError,
    id::DaColumnId,
    verifier::DaVerifier,
};
use ream_polynomial_commitments::handlers::verify_data_column_sidecar_kzg_proofs;
use ssz::{Decode, Encode};
use tree_hash::TreeHash;

#[derive(Debug, Clone, Copy, Default)]
pub struct KzgVerifier {
    max_blobs_per_block: usize,
}

impl KzgVerifier {
    pub fn new(max_blobs_per_block: usize) -> Self {
        Self {
            max_blobs_per_block,
        }
    }

    fn decode(&self, bytes: &[u8]) -> Result<DataColumnSidecar, ValidationError> {
        DataColumnSidecar::from_ssz_bytes(bytes)
            .map_err(|err| ValidationError::MalformedPayload(format!("{err:?}")))
    }

    fn encode(&self, sidecar: &DataColumnSidecar) -> Vec<u8> {
        sidecar.as_ssz_bytes()
    }

    fn verify_cells(&self, sidecar: &DataColumnSidecar) -> anyhow::Result<bool> {
        verify_data_column_sidecar_kzg_proofs(sidecar)
    }
}

impl DaVerifier for KzgVerifier {
    fn verify(&self, candidate: CandidateColumn) -> Result<VerifiedColumn, ValidationError> {
        let sidecar = self.decode(candidate.payload.as_bytes())?;
        let root = sidecar.signed_block_header.message.tree_hash_root();

        // check if the indentity is consistent
        let id = DaColumnId::new(root, sidecar.index)?;
        if id != candidate.id {
            return Err(ValidationError::IdMismatch {
                expected: format!("block root {block_root}, column {}", sidecar.index),
                actual: format!(
                    "block root {}, column {}",
                    candidate.id.block_root(),
                    candidate.id.column_index()
                ),
            });
        }

        // sanity checks before verifying the proof
        let commitment_len = sidecar.kzg_commitments.len();
        if commitment_len == 0 {
            return Err(ValidationError::EmptyCommitments);
        }

        if commitment_len > max_blobs_per_block {
            return Err(ValidationError::TooManyCommitments {
                count: commitment_len,
                maximum: self.max_blobs_per_block,
            });
        }
        if sidecar.column.len() != sidecar.kzg_commitments.len()
            || sidecar.column.len() != sidecar.kzg_commitments_inclusion_proof.len()
        {
            return Err(ValidationError::LengthMismatch {
                cells: view.cell_count,
                commitments: view.commitment_count,
                proofs: view.proof_count,
            });
        }

        // verifhy commitments inclusion proof against the signed block header
        if !is_valid_merkle_branch(
            root,
            sidecar.kzg_commitments_inclusion_proof,
            DATA_COLUMN_SIDECAR_KZG_PROOF_DEPTH,
            BLOB_KZG_COMMITMENTS_INDEX,
            sidecar.signed_block_header.message.body_root,
        ) {
            return Err(ValidationError::InvalidInclusionProof);
        }

        match self.verify_cells(&sidecar) {
            Ok(true) => {}
            Ok(false) => return Err(ValidationError::InvalidProof),
            Err(err) => return Err(ValidationError::VerifierFailure(format!("{err:?}"))),
        }

        self.Ok(VerifiedColumn::new_unchecked(
            candidate.id,
            candidate.context,
            candidate.payload,
        ))
    }
}
