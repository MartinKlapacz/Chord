# Chord Implementation in Rust

This implementation is based on the Chord protocol introduced in the paper by [Stoica et al](https://ieeexplore.ieee.org/abstract/document/1180543/). A Chord network is a type of distributed hash table (DHT) that uses consistent hashing to distribute keys across a peer-to-peer network.  It provides efficient methods for key-based lookup and data storage, enabling fault-tolerant and scalable systems. Take a look at the [documentation](docs/finalDocumentation.pdf) for more details on the implementations and its features.

## Installation

1. Install Rust according to the [Rust documentation](https://www.rust-lang.org/tools/install): `curl --proto ’=https’ --tlsv1.2 -sSf https://sh.rustup.rs | sh`
2. Install the [Protobuf compiler](https://grpc.io/docs/protoc-installation/): `sudo apt install -y protobuf-compiler`


## Running a Cluster
Running an instance requires a node config.
The [config folder](configs) contains example configs.

Start a new Chord cluster by running:

`cargo run --package chord --bin chord -- -c configs/config1.ini`

Let more Chord nodes join the cluster by running the following commands:

`cargo run --package chord --bin chord -- -c configs/config2.ini`

`cargo run --package chord --bin chord -- -c configs/config3.ini`

`cargo run --package chord --bin chord -- -c configs/config4.ini`

## Client
This project also contains python client applications.
Use the following commands to set and get key-value-pairs on the node running on address (`-a`) 127.0.0.1 and port (`-p`) 5501:

`python3 dht_client.py -a 127.0.0.1 -p 5501 -s -k hello -d world`

`python3 dht_client.py -a 127.0.0.1 -p 5501 -g -k hello`

