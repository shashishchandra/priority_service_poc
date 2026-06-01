# Always build and run in release mode.
# Debug mode is 10-50x slower due to missing SIMD, no inlining, bounds checks everywhere.

.PHONY: run build test clean

run:
	cargo run --release

build:
	cargo build --release

test:
	cargo test --release

bench: build
	./target/release/priority-service

clean:
	cargo clean
