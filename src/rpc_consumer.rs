use {
    crate::{
        ledger_storage::{LedgerStorage, LedgerStorageConfig, UploaderConfig},
    },
    std::sync::Arc,
    tokio::sync::{Mutex, mpsc},
    tokio::task,
    tokio::time::{sleep, Duration, Instant},
    futures::future::join_all,
    std::sync::atomic::{AtomicU64, Ordering},
    log::{debug, info, warn},
    solana_client::rpc_client::RpcClient,
    // solana_transaction_status::{BlockEncodingOptions, EncodedConfirmedBlock, UiTransactionEncoding, TransactionDetails},
    // solana_transaction_status::EncodedConfirmedBlock as SdkEncodedConfirmedBlock,
    solana_binary_encoder::{
        convert::generated,
        transaction_status::{
            EncodedConfirmedBlock,
            VersionedConfirmedBlock,
            UiTransactionEncoding,
            BlockEncodingOptions,
            TransactionDetails,
        },
        encode_block,
        convert_block,
    },
};
use solana_client::rpc_request::RpcRequest;
use serde_json::{json, Value};
use solana_sdk::clock::Slot;
use std::error::Error;
// use anyhow::Error as AnyError;
// use std::fmt;

// #[derive(Debug)]
// struct SendSyncError(Box<dyn Error + Send + Sync>);
//
// impl fmt::Display for SendSyncError {
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//         write!(f, "{}", self.0)
//     }
// }
//
// impl Error for SendSyncError {}
//
// impl SendSyncError {
//     fn new<E>(err: E) -> Self
//         where
//             E: Error + Send + Sync + 'static,
//     {
//         SendSyncError(Box::new(err))
//     }
// }
//
// unsafe impl Send for SendSyncError {}
// unsafe impl Sync for SendSyncError {}

pub fn get_raw_block(rpc_client: &RpcClient, slot: Slot) -> Result<Value, Box<dyn Error + Send + Sync>> {
    let request = RpcRequest::GetBlock;
    let params = json!([slot, {
        "encoding": "json",
        "transactionDetails": "full",
        "rewards": true,
        "maxSupportedTransactionVersion": 0
    }]);

    // Send the request and get the response
    let response: Value = rpc_client.send(request, params)?;

    // Return the raw block data from the "result" field
    Ok(response["result"].clone())
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
        let latest_slot = self.rpc_client.get_slot()?;

        let latest_stored_slot = match self.storage.get_latest_stored_slot(first_slot).await {
            Ok(slot) => slot,
            // Err(Error::HBaseError(HBaseError::RowNotFound)) => {
            //     // If the table is empty, start from first_slot - 1
            //     first_slot - 1
            // },
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
                            println!("Error fetching block data for slot {}: {:?}", slot, e);
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
                    info!("Processing block with slot {}", slot);

                    if block_data.is_null() {
                        warn!("Block data for slot {} is null. Skipping...", slot);
                        continue;
                    }

                    let block: EncodedConfirmedBlock = match serde_json::from_value(block_data) {
                        Ok(block) => block,
                        Err(e) => {
                            warn!("Failed to parse block data for slot {}: {:?}", slot, e);
                            continue;
                        }
                    };

                    // let encoder_block = sdk_block.into_encoder();

                    let options = BlockEncodingOptions {
                        transaction_details: TransactionDetails::Full,
                        show_rewards: true,
                        max_supported_transaction_version: Some(0),
                    };

                    let conversion_result = convert_block(block, UiTransactionEncoding::Json, options)
                        .map_err(|e| e.to_string()); // Convert error to string

                    match conversion_result {
                        Ok(versioned_block) => {
                            match storage.upload_confirmed_block(slot, versioned_block).await {
                                Ok(_) => info!("Successfully uploaded block with slot {}", slot),
                                Err(e) => warn!("Failed to upload block with slot {}: {:?}", slot, e),
                            }
                        },
                        Err(e) => {
                            warn!("Failed to convert block with slot {}: {:?}", slot, e);
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
