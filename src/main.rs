use {
    crate::{
        rpc_consumer::RpcConsumer,
        ledger_storage::{
            LedgerStorage,
            LedgerStorageConfig,
            FilterTxIncludeExclude,
            UploaderConfig
        },
    },
    solana_client::rpc_client::RpcClient,
    std::{
        collections::{HashSet},
    },
    solana_sdk::{
        pubkey::Pubkey,
        commitment_config::CommitmentConfig,
    },
    clap::{values_t, ArgMatches, App, Arg},
    log::{debug, info},
    std::sync::Arc,
};

pub mod cli;
pub mod hbase;
pub mod ledger_storage;
pub mod rpc_consumer;

/// Create a consumer based on the given configuration.
async fn create_consumer(
    uploader_config: UploaderConfig,
    rpc_url: String,
    hbase_address: String,
    reader_threads: usize,
    rpc_poll_interval: u64,
    start_block: Option<u64>,
    reverse: bool,
) -> RpcConsumer {
    info!("Connecting to Solana RPC: {}", &rpc_url);

    let storage_config = LedgerStorageConfig {
        read_only: false,
        timeout: None,
        address: hbase_address,
        uploader_config: uploader_config.clone(),
    };
    let storage = LedgerStorage::new_with_config(storage_config).await;

    let rpc_client = Arc::new(RpcClient::new_with_commitment(rpc_url, CommitmentConfig::confirmed()));

    RpcConsumer::new(
        rpc_client,
        storage,
        reader_threads,
        rpc_poll_interval,
        start_block,
        reverse,
    )
}

/// Handle the message processing.
async fn handle_message_receiving(
    uploader_config: UploaderConfig,
    rpc_url: String,
    hbase_address: String,
    reader_threads: usize,
    rpc_poll_interval: u64,
    start_block: Option<u64>,
    reverse: bool,
) {
    debug!("Started consuming messages");

    let rpc_consumer = create_consumer(
        uploader_config.clone(),
        rpc_url,
        hbase_address,
        reader_threads,
        rpc_poll_interval,
        start_block,
        reverse,
    ).await;

    let _ = rpc_consumer.consume().await;
}

fn process_arguments(matches: &ArgMatches) -> UploaderConfig {
    let disable_tx = matches.is_present("disable_tx");
    let disable_tx_by_addr = matches.is_present("disable_tx_by_addr");
    let disable_blocks = matches.is_present("disable_blocks");
    let enable_full_tx = matches.is_present("enable_full_tx");
    let use_md5_row_key_salt = matches.is_present("use_md5_row_key_salt");
    let filter_program_accounts = matches.is_present("filter_tx_by_addr_programs");
    let filter_voting_tx = matches.is_present("filter_voting_tx");
    let use_blocks_compression = !matches.is_present("disable_block_compression");
    let use_tx_compression = !matches.is_present("disable_tx_compression");
    let use_tx_by_addr_compression = !matches.is_present("disable_tx_by_addr_compression");
    let use_tx_full_compression = !matches.is_present("disable_tx_full_compression");

    let filter_tx_full_include_addrs: HashSet<Pubkey> =
        values_t!(matches, "filter_tx_full_include_addr", Pubkey)
            .unwrap_or_default()
            .iter()
            .cloned()
            .collect();

    let filter_tx_full_exclude_addrs: HashSet<Pubkey> =
        values_t!(matches, "filter_tx_full_exclude_addr", Pubkey)
            .unwrap_or_default()
            .iter()
            .cloned()
            .collect();

    let filter_tx_by_addr_include_addrs: HashSet<Pubkey> =
        values_t!(matches, "filter_tx_by_addr_include_addr", Pubkey)
            .unwrap_or_default()
            .iter()
            .cloned()
            .collect();

    let filter_tx_by_addr_exclude_addrs: HashSet<Pubkey> =
        values_t!(matches, "filter_tx_by_addr_exclude_addr", Pubkey)
            .unwrap_or_default()
            .iter()
            .cloned()
            .collect();

    let tx_full_filter = create_filter(
        filter_tx_full_exclude_addrs,
        filter_tx_full_include_addrs
    );
    let tx_by_addr_filter = create_filter(
        filter_tx_by_addr_exclude_addrs,
        filter_tx_by_addr_include_addrs
    );

    UploaderConfig {
        tx_full_filter,
        tx_by_addr_filter,
        disable_tx,
        disable_tx_by_addr,
        disable_blocks,
        enable_full_tx,
        use_md5_row_key_salt,
        filter_program_accounts,
        filter_voting_tx,
        use_blocks_compression,
        use_tx_compression,
        use_tx_by_addr_compression,
        use_tx_full_compression,
        max_concurrent_connections: 1,
        ..Default::default()
    }
}

fn create_filter(
    filter_tx_exclude_addrs: HashSet<Pubkey>,
    filter_tx_include_addrs: HashSet<Pubkey>,
) -> Option<FilterTxIncludeExclude> {
    let exclude_tx_addrs = !filter_tx_exclude_addrs.is_empty();
    let include_tx_addrs = !filter_tx_include_addrs.is_empty();

    if exclude_tx_addrs || include_tx_addrs {
        let filter_tx_addrs = FilterTxIncludeExclude {
            exclude: exclude_tx_addrs,
            addrs: if exclude_tx_addrs {
                filter_tx_exclude_addrs
            } else {
                filter_tx_include_addrs
            },
        };
        Some(filter_tx_addrs)
    } else {
        None
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let version = env!("CARGO_PKG_VERSION");
    info!("Solana RPC Ingestor Version: {}", version);

    let matches = App::new("Solana Block Uploader")
        .version("1.0")
        .about("Uploads Solana blocks to HBase")
        .arg(
            Arg::with_name("solana_rpc_url")
                .long("solana-rpc-url")
                .value_name("URL")
                .help("The Solana RPC URL to connect to")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("hbase_address")
                .long("hbase-address")
                .value_name("ADDRESS")
                .help("The HBase address to connect to")
                .default_value("http://localhost:8080")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("start_block")
                .long("start-block")
                .value_name("SLOT")
                .help("The slot number to start backfilling from")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("reverse")
                .long("reverse")
                .help("Backfill blocks in reverse order (requires --start-block)")
                .takes_value(false)
                .requires("start_block"),
        )
        .arg(
            Arg::with_name("reader_threads")
                .long("reader-threads")
                .value_name("N")
                .help("Number of reader threads")
                .default_value("1")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("rpc_poll_interval")
                .long("rpc-poll-interval")
                .value_name("MILLISECONDS")
                .help("Poll interval in milliseconds for Solana RPC")
                .default_value("100")
                .takes_value(true),
        )
        .get_matches();

    // Extract command-line arguments
    let solana_rpc_url = matches.value_of("solana_rpc_url").unwrap().to_string();
    let hbase_address = matches.value_of("hbase_address").unwrap().to_string();
    let reader_threads: usize = matches.value_of("reader_threads").unwrap().parse()?;
    let rpc_poll_interval: u64 = matches.value_of("rpc_poll_interval").unwrap().parse()?;
    let start_block: Option<u64> = matches.value_of("start_block").map(|s| s.parse().unwrap());
    let reverse = matches.is_present("reverse");

    let uploader_config = process_arguments(&matches);

    env_logger::init();

    debug!("Solana block encoder service started");

    handle_message_receiving(
        uploader_config,
        solana_rpc_url,
        hbase_address,
        reader_threads,
        rpc_poll_interval,
        start_block,
        reverse,
    ).await;

    Ok(())
}
