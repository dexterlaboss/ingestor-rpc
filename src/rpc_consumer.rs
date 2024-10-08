use {
    crate::{
        ledger_storage::{LedgerStorage},
    },
    std::sync::Arc,
    tokio::sync::{Mutex, mpsc},
    tokio::time::{sleep, Duration, Instant},
    futures::future::join_all,
    std::sync::atomic::{AtomicU64, Ordering},
    log::{debug, info, warn, error},
    solana_client::rpc_client::RpcClient,
    solana_binary_encoder::{
        transaction_status::{
            EncodedConfirmedBlock,
            UiTransactionEncoding,
            BlockEncodingOptions,
            TransactionDetails,
        },
        convert_block,
    },
};
use solana_client::rpc_request::RpcRequest;
use serde_json::{json, Value};
use solana_sdk::clock::Slot;
use std::error::Error;

pub fn get_raw_block(rpc_client: &RpcClient, slot: Slot) -> Result<Value, Box<dyn Error + Send + Sync>> {
    let request = RpcRequest::GetBlock;
    let params = json!([slot, {
        "encoding": "json",
        "transactionDetails": "full",
        "rewards": true,
        "maxSupportedTransactionVersion": 0
    }]);

    let response: Value = rpc_client.send(request, params)?;

    Ok(response.clone())
}

pub struct RpcConsumer {
    rpc_client: Arc<RpcClient>,
    storage: LedgerStorage,
    reader_threads: usize,
    writer_threads: usize,
    channel_buffer_size: usize,
    rpc_poll_interval: u64,
}

impl RpcConsumer {
    pub fn new(
        rpc_client: Arc<RpcClient>,
        storage: LedgerStorage,
        reader_threads: usize,
        writer_threads: usize,
        channel_buffer_size: usize,
        rpc_poll_interval: u64,
    ) -> Self {
        RpcConsumer {
            rpc_client,
            storage,
            reader_threads,
            writer_threads,
            channel_buffer_size,
            rpc_poll_interval,
        }
    }

    pub async fn consume(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        debug!("Started consuming blocks from Solana RPC");

        let first_slot = self.rpc_client.get_first_available_block()?;
        let _latest_slot = self.rpc_client.get_slot()?;

        let latest_stored_slot = match self.storage.get_latest_stored_slot(first_slot).await {
            Ok(slot) => slot,
            Err(e) => return Err(Box::new(e)),
        };

        // Check for gaps between the latest stored slot and the first available slot
        if latest_stored_slot < first_slot - 1 {
            println!(
                "Warning: There is a gap between the latest written slot in HBase ({}) and the earliest available block in the validator ({}).",
                latest_stored_slot, first_slot
            );
        }

        let (tx, rx) = mpsc::channel(self.channel_buffer_size);
        let rx = Arc::new(Mutex::new(rx));
        let current_slot = Arc::new(AtomicU64::new(latest_stored_slot + 1));
        let mut message_counter = 0;
        let report_interval = 10;
        let mut batch_time = Instant::now();

        // Spawn fetcher tasks
        let fetcher_handles: Vec<_> = (0..self.reader_threads).map(|_| {
            let rpc_client = Arc::clone(&self.rpc_client);
            let tx = tx.clone();
            let current_slot = Arc::clone(&current_slot);
            let rpc_poll_interval = self.rpc_poll_interval;
            tokio::spawn(async move {
                loop {
                    let slot = current_slot.fetch_add(1, Ordering::SeqCst);

                    match get_raw_block(&rpc_client, slot) {
                        Ok(block_data) => {
                            if tx.send((slot, block_data)).await.is_err() {
                                break;
                            }
                        },
                        Err(e) => {
                            error!("Error fetching block data from slot {}: {:?}", slot, e);
                        }
                    }

                    sleep(Duration::from_millis(rpc_poll_interval)).await;
                }
            })
        }).collect();

        // Spawn writer tasks
        let writer_handles: Vec<_> = (0..self.writer_threads).map(|_| {
            let storage = self.storage.clone();
            let rx = Arc::clone(&rx);
            tokio::spawn(async move {
                while let Some((slot, block_data)) = rx.lock().await.recv().await {
                    info!("Received block from slot {}", slot);

                    if block_data.is_null() {
                        warn!("Block data from slot {} is null. Skipping.", slot);
                        continue;
                    }

                    let block: EncodedConfirmedBlock = match serde_json::from_value(block_data) {
                        Ok(block) => block,
                        Err(e) => {
                            warn!("Failed to parse block data from slot {}: {:?}", slot, e);
                            continue;
                        }
                    };

                    let options = BlockEncodingOptions {
                        transaction_details: TransactionDetails::Full,
                        show_rewards: true,
                        max_supported_transaction_version: Some(0),
                    };

                    let conversion_result = convert_block(block, UiTransactionEncoding::Json, options)
                        .map_err(|e| e.to_string());

                    match conversion_result {
                        Ok(versioned_block) => {
                            match storage.upload_confirmed_block(slot, versioned_block).await {
                                Ok(_) => info!("Finished processing block from slot {}", slot),
                                Err(e) => warn!("Failed to process block from slot {}: {:?}", slot, e),
                            }
                        },
                        Err(e) => {
                            warn!("Failed to convert block from slot {}: {:?}", slot, e);
                        }
                    }

                    message_counter += 1;
                    if message_counter % report_interval == 0 {
                        let batch_duration: Duration = batch_time.elapsed();
                        info!("Processed {} blocks, total time taken: {:?}", report_interval, batch_duration);
                        batch_time = Instant::now();
                    }
                }
            })
        }).collect();

        // Wait for fetchers and writers to finish
        join_all(fetcher_handles).await;
        drop(tx);
        join_all(writer_handles).await;

        Ok(())
    }
}
