use std::sync::Arc;

use ream_da::{
    column::CandidateColumn,
    store::{DaWriteStore, InsertOutcome},
    verifier::DaVerifier,
};
use ream_executor::ReamExecutor;
use tokio::sync::mpsc;
use tracing::{debug, error, info};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DaWorkItem {
    Candidate(CandidateColumn),
}

pub struct DaVerificationService {
    receiver: mpsc::Receiver<DaWorkItem>,
    verifier: Arc<dyn DaVerifier>,
    store: Arc<dyn DaWriteStore>,
    executor: ReamExecutor,
}

impl DaVerificationService {
    pub fn new(
        receiver: mpsc::Receiver<DaWorkItem>,
        verifier: Arc<dyn DaVerifier>,
        store: Arc<dyn DaWriteStore>,
        executor: ReamExecutor,
    ) -> Self {
        Self {
            receiver,
            verifier: verifier,
            store: store,
            executor: executor,
        }
    }
    pub async fn run(mut self) {
        info!("DA verification service started");
        while let Some(item) = self.receiver.recv().await {
            match item {
                DaWorkItem::Candidate(candidate) => self.process_candidate(candidate).await,
            }
        }
        info!("DA verification service stopped: ingestion queue closed");
    }

    async fn process_candidate(&self, candidate: CandidateColumn) {
        let id = candidate.id;
        let verifier = self.verifier.clone();

        // verify the column
        let verified = match self
            .executor
            .spawn_blocking(move || verifier.verify(candidate))
            .await
        {
            Ok(result) => result,
            Err(err) => {
                error!("verification worker panicked or was cancelled: {err}");
                return;
            }
        };

        // persist verified column to local file if verification is success, log error otherwise.
        match verified {
            Ok(verified_column) => match self.store.put(verified_column) {
                Ok(InsertOutcome::Inserted) => {
                    debug!(
                        "stored verified column: block root {root}, column {index}",
                        root = id.block_root(),
                        index = id.index()
                    );
                }
                Ok(InsertOutcome::Duplicated) => {
                    debug!(
                        "duplicated column: block root {root}, column {index}, kept existing verified column",
                        root = id.block_root(),
                        index = id.index()
                    );
                }
                Err(err) => {
                    error!("failed to persist verified column: {err}");
                }
            },
            Err(err) => {
                debug!(
                    "rejected candidate column: block root {root}, column {index}: {err}",
                    root = id.block_root(),
                    index = id.index()
                );
            }
        }
    }
}
