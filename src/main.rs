use {
    crate::{
        rpc_consumer::RpcConsumer,
        cli::{DefaultBlockUploaderArgs, block_uploader_app},
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
    clap::{value_t, value_t_or_exit, values_t, values_t_or_exit, ArgMatches, App, Arg},
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
    writer_threads: usize,
    channel_buffer_size: usize,
    rpc_poll_interval: u64,
) -> RpcConsumer {
    info!("Connecting to Solana RPC: {}", &rpc_url);

    let storage_config = LedgerStorageConfig {
        read_only: false,
        timeout: None,
        address: hbase_address,
        uploader_config: uploader_config.clone(),
    };
    let storage = LedgerStorage::new_with_config(storage_config).await;

    // let rpc_client = RpcClient::new(rpc_url);
    let rpc_client = Arc::new(RpcClient::new_with_commitment(rpc_url, CommitmentConfig::confirmed()));

    RpcConsumer::new(
        rpc_client,
        storage,
        reader_threads,
        writer_threads,
        channel_buffer_size,
        rpc_poll_interval,
    )
}

/// Handle the message processing.
async fn handle_message_receiving(
    uploader_config: UploaderConfig,
    rpc_url: String,
    hbase_address: String,
    reader_threads: usize,
    writer_threads: usize,
    channel_buffer_size: usize,
    rpc_poll_interval: u64,
) {
    debug!("Started consuming messages");

    let rpc_consumer = create_consumer(
        uploader_config.clone(),
        rpc_url,
        hbase_address,
        reader_threads,
        writer_threads,
        channel_buffer_size,
        rpc_poll_interval,
    ).await;

    rpc_consumer.consume().await;
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
            Arg::with_name("reader_threads")
                .long("reader-threads")
                .value_name("N")
                .help("Number of reader threads")
                .default_value("1")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("writer_threads")
                .long("writer-threads")
                .value_name("N")
                .help("Number of writer threads")
                .default_value("5")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("channel_buffer_size")
                .long("channel-buffer-size")
                .value_name("SIZE")
                .help("Size of the channel buffer")
                .default_value("10")
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
    let writer_threads: usize = matches.value_of("writer_threads").unwrap().parse()?;
    let channel_buffer_size: usize = matches.value_of("channel_buffer_size").unwrap().parse()?;
    let rpc_poll_interval: u64 = matches.value_of("rpc_poll_interval").unwrap().parse()?;

    let uploader_config = process_arguments(&matches);

    env_logger::init();

    info!("Solana block encoder service started");

    handle_message_receiving(
        uploader_config,
        solana_rpc_url,
        hbase_address,
        reader_threads,
        writer_threads,
        channel_buffer_size,
        rpc_poll_interval,
    ).await;

    Ok(())
}
