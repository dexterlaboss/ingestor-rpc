use {
    crate::{
        ledger_storage::{LedgerStorage},
    },
    std::sync::Arc,
    tokio::time::{sleep, Duration},
    futures::future::join_all,
    std::sync::atomic::{AtomicU64, AtomicBool, Ordering},
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
    rpc_poll_interval: u64,
    start_block: Option<u64>,
    reverse: bool,
}

impl RpcConsumer {
    pub fn new(
        rpc_client: Arc<RpcClient>,
        storage: LedgerStorage,
        reader_threads: usize,
        rpc_poll_interval: u64,
        start_block: Option<u64>,
        reverse: bool,
    ) -> Self {
        RpcConsumer {
            rpc_client,
            storage,
            reader_threads,
            rpc_poll_interval,
            start_block,
            reverse,
        }
    }

    pub async fn consume(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        debug!("Started consuming blocks from Solana RPC");

        let first_slot = self.rpc_client.get_first_available_block()?;
        let _latest_slot = self.rpc_client.get_slot()?;

        let latest_stored_slot = if let Some(start_block) = self.start_block {
            if self.reverse {
                info!("Starting reverse backfill from block {}", start_block);
                start_block + 1  // We'll decrement from here
            } else {
                info!("Starting forward backfill from block {}", start_block);
                start_block - 1  // We'll increment from here
            }
        } else {
            match self.storage.get_latest_stored_slot(first_slot).await {
                Ok(slot) => slot,
                Err(e) => return Err(Box::new(e)),
            }
        };

        // Check for gaps between the latest stored slot and the first available slot
        if !self.reverse && latest_stored_slot < first_slot - 1 {
            println!(
                "Warning: There is a gap between the latest written slot in HBase ({}) and the earliest available block in the validator ({}).",
                latest_stored_slot, first_slot
            );
        }

        // For reverse mode, use different logic
        if self.reverse {
            let current_slot = Arc::new(AtomicU64::new(latest_stored_slot));
            
            // Spawn worker tasks for reverse backfilling
            let worker_handles: Vec<_> = (0..self.reader_threads).map(|worker_id| {
                let rpc_client = Arc::clone(&self.rpc_client);
                let storage = self.storage.clone();
                let current_slot = Arc::clone(&current_slot);
                let rpc_poll_interval = self.rpc_poll_interval;
                let first_slot = first_slot;
                
                tokio::spawn(async move {
                    loop {
                        let slot = current_slot.fetch_sub(1, Ordering::SeqCst);
                        
                        // Stop if we've gone below the first available slot
                        if slot < first_slot || slot == 0 {
                            info!("Worker {} reached first available slot {}, stopping", worker_id, first_slot);
                            break;
                        }

                        match get_raw_block(&rpc_client, slot) {
                            Ok(block_data) => {
                                if block_data.is_null() {
                                    warn!("Block data from slot {} is null. Skipping.", slot);
                                    continue;
                                }

                                info!("Worker {} received block from slot {} (reverse)", worker_id, slot);

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
                                            Ok(_) => {
                                                info!("Worker {} finished processing block from slot {} (reverse)", worker_id, slot);
                                            },
                                            Err(e) => warn!("Failed to process block from slot {}: {:?}", slot, e),
                                        }
                                    },
                                    Err(e) => {
                                        warn!("Failed to convert block from slot {}: {:?}", slot, e);
                                    }
                                }
                            },
                            Err(e) => {
                                let error_msg = e.to_string();
                                if error_msg.contains("Block not available") || error_msg.contains("-32004") {
                                    debug!("Block not available for slot {} (reverse)", slot);
                                } else {
                                    error!("Error fetching block data from slot {}: {:?}", slot, e);
                                }
                            }
                        }

                        sleep(Duration::from_millis(rpc_poll_interval)).await;
                    }
                })
            }).collect();

            // Wait for all workers to finish
            join_all(worker_handles).await;
            
        } else {
            // Original forward implementation
            let current_slot = Arc::new(AtomicU64::new(latest_stored_slot + 1));
            let is_caught_up = Arc::new(AtomicBool::new(false));
            let latest_network_slot = Arc::new(AtomicU64::new(_latest_slot));

            // Spawn a task to periodically update the latest network slot
            let network_slot_updater = {
                let rpc_client = Arc::clone(&self.rpc_client);
                let latest_network_slot = Arc::clone(&latest_network_slot);
                let is_caught_up = Arc::clone(&is_caught_up);
                let current_slot = Arc::clone(&current_slot);
                
                tokio::spawn(async move {
                    loop {
                        sleep(Duration::from_secs(2)).await;
                        
                        match rpc_client.get_slot() {
                            Ok(latest) => {
                                latest_network_slot.store(latest, Ordering::SeqCst);
                                
                                // Update caught up status
                                let current = current_slot.load(Ordering::SeqCst);
                                let was_caught_up = is_caught_up.load(Ordering::SeqCst);
                                let now_caught_up = current >= latest.saturating_sub(32); // Within 32 slots is "caught up"
                                
                                if !was_caught_up && now_caught_up {
                                    info!("Caught up to network tip at slot {}", latest);
                                } else if was_caught_up && !now_caught_up {
                                    info!("Fell behind network tip, catching up...");
                                }
                                
                                is_caught_up.store(now_caught_up, Ordering::SeqCst);
                            }
                            Err(e) => {
                                error!("Failed to update latest network slot: {:?}", e);
                            }
                        }
                    }
                })
            };

            // Spawn worker tasks that both fetch and process blocks
            let reader_threads = self.reader_threads;
            let worker_handles: Vec<_> = (0..reader_threads).map(|worker_id| {
                let rpc_client = Arc::clone(&self.rpc_client);
                let storage = self.storage.clone();
                let current_slot = Arc::clone(&current_slot);
                let is_caught_up = Arc::clone(&is_caught_up);
                let latest_network_slot = Arc::clone(&latest_network_slot);
                let rpc_poll_interval = self.rpc_poll_interval;
                
                tokio::spawn(async move {
                    let mut consecutive_not_available = 0;
                    let mut last_slot_check = 0u64;
                    
                    loop {
                        let slot = current_slot.fetch_add(1, Ordering::SeqCst);
                        
                        // If we're caught up, check if this slot is beyond the network tip
                        if is_caught_up.load(Ordering::SeqCst) {
                            let latest = latest_network_slot.load(Ordering::SeqCst);
                            if slot > latest + 1 {
                                // We're ahead of the network, back off
                                current_slot.fetch_sub(1, Ordering::SeqCst);
                                debug!("Worker {} waiting at network tip (slot {})", worker_id, latest);
                                sleep(Duration::from_millis(400)).await; // ~1 slot time
                                continue;
                            }
                        }

                        match get_raw_block(&rpc_client, slot) {
                            Ok(block_data) => {
                                consecutive_not_available = 0;
                                
                                if block_data.is_null() {
                                    warn!("Block data from slot {} is null. Skipping.", slot);
                                    continue;
                                }

                                info!("Worker {} received block from slot {}", worker_id, slot);

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
                                            Ok(_) => {
                                                info!("Worker {} finished processing block from slot {}", worker_id, slot);
                                            },
                                            Err(e) => warn!("Failed to process block from slot {}: {:?}", slot, e),
                                        }
                                    },
                                    Err(e) => {
                                        warn!("Failed to convert block from slot {}: {:?}", slot, e);
                                    }
                                }
                                
                                // Use shorter poll interval when catching up
                                if !is_caught_up.load(Ordering::SeqCst) {
                                    sleep(Duration::from_millis(rpc_poll_interval)).await;
                                }
                            },
                            Err(e) => {
                                let error_msg = e.to_string();
                                if error_msg.contains("Block not available") || error_msg.contains("-32004") {
                                    consecutive_not_available += 1;
                                    
                                    // Only check actual network slot after multiple failures and not too frequently
                                    if consecutive_not_available > 3 && slot.saturating_sub(last_slot_check) > 100 {
                                        match rpc_client.get_slot() {
                                            Ok(actual_latest) => {
                                                last_slot_check = slot;
                                                latest_network_slot.store(actual_latest, Ordering::SeqCst);
                                                
                                                if slot > actual_latest + 5 {
                                                    // We're genuinely ahead, reset to a safe position
                                                    let reset_to = actual_latest.saturating_sub(reader_threads as u64 * 2);
                                                    current_slot.store(reset_to, Ordering::SeqCst);
                                                    info!("Worker {} checked network: latest slot is {}, resetting to {}", 
                                                          worker_id, actual_latest, reset_to);
                                                    consecutive_not_available = 0;
                                                    sleep(Duration::from_millis(1000)).await;
                                                    continue;
                                                } else {
                                                    // We're close to the tip, just wait
                                                    debug!("Worker {} at slot {} near tip ({}), waiting...", 
                                                           worker_id, slot, actual_latest);
                                                    current_slot.fetch_sub(1, Ordering::SeqCst);
                                                    sleep(Duration::from_millis(400)).await;
                                                    continue;
                                                }
                                            }
                                            Err(rpc_err) => {
                                                error!("Worker {} failed to get latest slot: {:?}", worker_id, rpc_err);
                                            }
                                        }
                                    }
                                    
                                    // Don't spam logs when caught up
                                    if !is_caught_up.load(Ordering::SeqCst) && consecutive_not_available < 3 {
                                        debug!("Block not available for slot {}", slot);
                                    }
                                    
                                    // Back off a bit and retry
                                    current_slot.fetch_sub(1, Ordering::SeqCst);
                                    sleep(Duration::from_millis(200)).await;
                                } else {
                                    error!("Error fetching block data from slot {}: {:?}", slot, e);
                                    sleep(Duration::from_millis(500)).await;
                                }
                            }
                        }
                    }
                })
            }).collect();

            // Wait for all workers to finish
            join_all(worker_handles).await;
            network_slot_updater.abort();
        }

        Ok(())
    }
}
