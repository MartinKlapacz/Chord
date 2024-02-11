FROM rust:1.76.0

RUN apt-get update && apt-get install -y protobuf-compiler

WORKDIR /usr/src/node

COPY . .

RUN cargo build --release

