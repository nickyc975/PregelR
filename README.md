# Pregel in Rust

A single-node [Pregel](https://dl.acm.org/citation.cfm?id=1584010) implementation in Rust.

For the previous Java version, see [Pregel](https://github.com/nickyc975/Pregel).

## Features

* Combiner supported

* Aggregators supported

* Multi-thread computing

* Functional API

## Usage

See [examples](https://github.com/nickyc975/PregelR/tree/master/examples).

## Run Examples

1. Download the graph data file from [Stanford website](https://snap.stanford.edu/data/web-Google.html);

2. Create a `data/` folder under the project root directory and extract the `web-Google.txt` file into it;

3. Run with `cargo run --example page_rank --release` or `cargo run --example sssp --release`.

## License

[MIT](./LICENSE)