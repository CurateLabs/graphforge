.PHONY: install smoke smoke-python smoke-rust

install:
	uv sync --locked

smoke: install smoke-python smoke-rust

smoke-python:
	PYTHONPATH=harness uv run --locked python -m graphforge_bench.smoke
	PYTHONPATH=harness uv run --locked python -m unittest discover -s tests -p 'test_*.py'

smoke-rust:
	cargo test --locked --manifest-path Cargo.toml
