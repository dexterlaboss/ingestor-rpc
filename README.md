# ingestor-rpc
Ingestor that uses regular RPC to write data to Hbase for ArchivalRPC

## Features
- Detect when approaching network tip and wait before requesting blocks that don't exist yet
- Reverse order back-filling support
- Optional argument to specify the block number to start from
  - If not specified then ingestor will start from last populated block in Hbase
- Hbase connection re-use
- RPC endpoint HTTP/2 support with connection re-use
- Number of threads to use for requesting and back-filling data

## Usage
```
USAGE:
    ingestor-rpc [FLAGS] [OPTIONS] --solana-rpc-url <URL>

FLAGS:
    -h, --help       Prints help information
        --reverse    Backfill blocks in reverse order (requires --start-block)
    -V, --version    Prints version information

OPTIONS:
        --hbase-address <ADDRESS>             The HBase address to connect to [default: http://localhost:8080]
        --reader-threads <N>                  Number of reader threads [default: 1]
        --rpc-poll-interval <MILLISECONDS>    Poll interval in milliseconds for Solana RPC [default: 100]
        --solana-rpc-url <URL>                The Solana RPC URL to connect to
        --start-block <SLOT>                  The slot number to start backfilling from
```

### Examples

```
RUST_LOG=debug ./ingestor-rpc --solana-rpc-url <RPC-ENDPOINT> --hbase-address <HBASE-THRIFT-ENDPOINT> --reader-threads 50
```
