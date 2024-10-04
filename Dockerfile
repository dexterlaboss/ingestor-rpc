FROM rust:1.70 as build

RUN apt-get update && apt-get install -y --no-install-recommends \
    apt-utils \
    software-properties-common \
    cmake \
    libclang-dev \
    libudev-dev

RUN USER=root cargo new --bin solana
WORKDIR /solana

COPY . /solana

RUN cargo build --release



FROM rust:1.70

RUN mkdir -p /solana

WORKDIR /solana

COPY --from=build /solana/target/release/ingestor-rpc .

EXPOSE 8899

ENV RUST_LOG=info
CMD ["./ingestor-rpc"]