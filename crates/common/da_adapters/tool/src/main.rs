//! `ream-da-tool` — a small end-to-end driver for the standalone DA node.
//!
//! Talks to a running `ream da_node` over its loopback RPC exactly the way the
//! future beacon-side feeder will: submit data columns for verification, query
//! availability, and — the main event — pull a real block's blobs from a live
//! beacon node, derive its data column sidecars locally (real KZG cells and
//! proofs), and feed them through the ingest → verify → store pipeline.
//!
//! Usage:
//!   ream-da-tool health
//!   ream-da-tool availability <block_root>
//!   ream-da-tool feed --beacon-url <url> [block_id] [--columns 0,1,2] [--wait 30]

mod beacon;
mod da;

use alloy_primitives::B256;
use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use ssz::Encode;

use crate::{beacon::build_column_sidecars, da::DaClient};

#[derive(Parser)]
#[command(
    name = "ream-da-tool",
    about = "End-to-end driver for the standalone ream DA node"
)]
struct Cli {
    /// Base URL of the DA node RPC.
    #[arg(long, default_value = "http://127.0.0.1:5062")]
    da_url: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Check that the DA node is up.
    Health,
    /// Query column availability for a block root.
    Availability {
        /// The beacon block root, 0x-prefixed.
        block_root: B256,
    },
    /// Fetch a block's blobs from a beacon node, derive its data column
    /// sidecars, submit them to the DA node, and wait for them to be held.
    Feed {
        /// Beacon API base URL (e.g. http://localhost:5052 or a public
        /// provider).
        #[arg(long)]
        beacon_url: String,
        /// Block to feed: a slot number, "head", "finalized", or a 0x block
        /// root.
        #[arg(default_value = "head")]
        block_id: String,
        /// Submit only these column indices (comma-separated). Default: all.
        #[arg(long, value_delimiter = ',')]
        columns: Option<Vec<u64>>,
        /// Seconds to wait for the DA node to verify the submitted columns.
        #[arg(long, default_value_t = 30)]
        wait: u64,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let da = DaClient::new(cli.da_url);

    match cli.command {
        Command::Health => {
            let health = da.health().await?;
            println!("{}", serde_json::to_string_pretty(&health)?);
        }
        Command::Availability { block_root } => {
            let availability = da.availability(block_root).await?;
            println!("{}", serde_json::to_string_pretty(&availability)?);
        }
        Command::Feed {
            beacon_url,
            block_id,
            columns,
            wait,
        } => feed(&da, &beacon_url, &block_id, columns, wait).await?,
    }

    Ok(())
}

/// The full pipeline: beacon blobs → local sidecar derivation → DA ingest →
/// availability confirmation.
async fn feed(
    da: &DaClient,
    beacon_url: &str,
    block_id: &str,
    columns: Option<Vec<u64>>,
    wait_secs: u64,
) -> Result<()> {
    let blob_sidecars = beacon::fetch_blob_sidecars(beacon_url, block_id)
        .await
        .with_context(|| format!("fetching blob sidecars for block {block_id}"))?;
    if blob_sidecars.is_empty() {
        bail!("block {block_id} has no blobs — nothing to feed");
    }
    println!(
        "fetched {} blob(s) for block {block_id}, slot {}",
        blob_sidecars.len(),
        blob_sidecars[0].signed_block_header.message.slot
    );

    // KZG cell computation is CPU-bound and takes a while (trusted setup load
    // plus per-blob proving); keep it off the async runtime.
    let (block_root, slot, sidecars) =
        tokio::task::spawn_blocking(move || build_column_sidecars(blob_sidecars)).await??;
    println!(
        "derived {} column sidecars (block root {block_root})",
        sidecars.len()
    );

    // Submit the requested columns (default: all of them).
    let selected: Vec<_> = match &columns {
        Some(indices) => sidecars
            .into_iter()
            .filter(|sidecar| indices.contains(&sidecar.index))
            .collect(),
        None => sidecars,
    };
    if selected.is_empty() {
        bail!("no sidecars match the requested column indices");
    }

    let mut submitted = Vec::with_capacity(selected.len());
    for sidecar in &selected {
        let payload = format!(
            "0x{}",
            alloy_primitives::hex::encode(sidecar.as_ssz_bytes())
        );
        da.ingest(block_root, sidecar.index, slot, &payload)
            .await
            .with_context(|| format!("submitting column {}", sidecar.index))?;
        submitted.push(sidecar.index);
    }
    println!("submitted {} column(s) to the DA node", submitted.len());

    // Verification is asynchronous behind the ingest queue: poll availability
    // until every submitted index is held (no longer listed as missing).
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(wait_secs);
    let availability = loop {
        let availability = da.availability(block_root).await?;
        let missing: Vec<u64> = availability["missing"]
            .as_array()
            .map(|values| values.iter().filter_map(|v| v.as_u64()).collect())
            .unwrap_or_default();
        if submitted.iter().all(|index| !missing.contains(index)) {
            break availability;
        }
        if tokio::time::Instant::now() >= deadline {
            println!("{}", serde_json::to_string_pretty(&availability)?);
            bail!(
                "timed out after {wait_secs}s: {} submitted column(s) still not held",
                submitted
                    .iter()
                    .filter(|index| missing.contains(index))
                    .count()
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    };

    println!("all submitted columns verified and held:");
    println!("{}", serde_json::to_string_pretty(&availability)?);
    Ok(())
}
