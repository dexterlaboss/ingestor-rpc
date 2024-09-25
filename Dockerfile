FROM ubuntu:18.04 AS builder

# Install Rust and build dependencies
RUN apt-get update && apt-get install -y \
    curl \
    build-essential \
    cmake \
    git \
    && curl https://sh.rustup.rs -sSf | sh -s -- -y \
    && . $HOME/.cargo/env

# Set environment variables for Rust
ENV PATH="/root/.cargo/bin:$PATH"

# Set the working directory for the Rust project
WORKDIR /usr/src/sol-rpc-ingestor

# Copy the Rust project into the container
COPY Cargo.toml Cargo.lock ./

# Cache dependencies to optimize builds
RUN cargo fetch

# Copy the Rust project into the container
COPY ./src ./src
COPY cargo rust-toolchain.toml ./

# Build the Rust project
RUN cargo build --release

#COPY hbase-site.xml start-services.sh ./

# Clone and build the solana-lite-rpc tool
WORKDIR /opt/solana-lite-rpc
RUN git clone https://github.com/dexterlaboss/solana-lite-rpc.git . \
    && cargo build --release


FROM ubuntu:18.04

ENV HBASE_VERSION 2.4.11
RUN apt-get update
RUN apt-get -y install supervisor python-pip openjdk-8-jdk-headless curl build-essential cmake git
RUN pip install supervisor-stdout

ENV JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64"
ENV PATH="$JAVA_HOME/bin:$PATH"


WORKDIR /opt

RUN curl -O https://archive.apache.org/dist/hbase/2.4.11/hbase-2.4.11-bin.tar.gz \
    && tar xzf hbase-2.4.11-bin.tar.gz \
    && mv hbase-2.4.11 hbase \
    && rm hbase-2.4.11-bin.tar.gz

COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf
COPY hbase-site.xml /opt/hbase-${HBASE_VERSION}/conf/hbase-site.xml
RUN mkdir -p /data/hbase /data/zookeeper

ENV PATH $PATH:/opt/hbase-${HBASE_VERSION}/bin

COPY --from=builder /usr/src/sol-rpc-ingestor/target/release/sol-rpc-ingestor /usr/local/bin/sol-rpc-ingestor

# Copy over the solana-lite-rpc binary from the builder stage
COPY --from=builder /opt/solana-lite-rpc/target/release/solana-lite-rpc /usr/local/bin/solana-lite-rpc

COPY create-hbase-tables.sh /usr/local/bin/create-hbase-tables.sh
RUN chmod +x /usr/local/bin/create-hbase-tables.sh

# Zookeeper port
EXPOSE 2181 

# REST port
EXPOSE 8080

EXPOSE 8899

EXPOSE 9090

# Master port
EXPOSE 16000

# Master info port
EXPOSE 16010

# Regionserver port
EXPOSE 16020

# Regionserver info port
EXPOSE 16030

VOLUME /data/hbase
VOLUME /data/zookeeper

CMD ["/usr/bin/supervisord"]
