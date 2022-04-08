build:
	cargo build --release

run_page_rank:
	rm -rf data/page_rank
	cargo run --example page_rank --release

run_sssp:
	rm -rf data/sssp
	cargo run --example sssp --release

run_virus:
	rm -rf data/virus
	cargo run --example virus --release
