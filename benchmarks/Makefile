.PHONY: install smoke smoke-python smoke-rust local-admission

install:
	uv sync --locked

smoke: smoke-python smoke-rust

smoke-python: install
	PYTHONPATH=harness uv run --locked python -m graphforge_bench.smoke
	PYTHONPATH=harness uv run --locked python -m unittest discover -s tests -p 'test_*.py'

smoke-rust: install
	cargo fmt --all -- --check
	CARGO_TARGET_DIR=$(CURDIR)/target cargo clippy --workspace --locked -- -D warnings
	CARGO_TARGET_DIR=$(CURDIR)/target cargo test --locked --manifest-path Cargo.toml
	PYTHONPATH=harness uv run --locked python -m graphforge_bench.check_rust_graph

local-admission: install
	PYTHONPATH=$(CURDIR)/harness uv run --locked reframe -C reframe/settings.py -c reframe/checks -n '^LocalBenchExecAdmission$$' -l | grep -F LocalBenchExecAdmission
	PYTHONPATH=$(CURDIR)/harness uv run --locked reframe -C reframe/settings.py -c reframe/checks -n '^LocalBenchExecAdmission$$' -r
