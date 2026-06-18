use std::sync::Arc;

use ream_da::{column::CandidateColumn, store::DaWriteStore, verifier::DaVerifier};
use ream_executor::ReamExecutor;
use tokio::sync::mpsc;
use tracing::info;

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
            verifier,
            store,
            executor,
        }
    }
    pub async fn run(mut self) {
        info!("Da service is running... but doing nothing...");
        while let Some(_item) = self.receiver.recv().await {}
    }
}
