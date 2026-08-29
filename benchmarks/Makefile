.PHONY: install smoke smoke-python smoke-rust fly-adapter-static local-admission progressive-qualification-list progressive-qualification-plan progressive-qualification-run progressive-qualification-project-s20 progressive-qualification-binaries

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

fly-adapter-static: install
	PYTHONPATH=harness uv run --locked python -m unittest tests.test_fly_adapter

local-admission: install
	PYTHONPATH=$(CURDIR)/harness uv run --locked reframe -C reframe/settings.py -c reframe/checks -n '^LocalBenchExecAdmission$$' -l | grep -F LocalBenchExecAdmission
	PYTHONPATH=$(CURDIR)/harness uv run --locked reframe -C reframe/settings.py -c reframe/checks -n '^LocalBenchExecAdmission$$' -r

progressive-qualification-list: install
	PYTHONPATH=$(CURDIR)/harness uv run --locked reframe -C reframe/settings.py -c reframe/checks -n '^Graph500ProgressiveQualificationProfile' -l

progressive-qualification-binaries:
	cargo build --locked --release --manifest-path ../Cargo.toml -p graphforge-cli
	cargo build --locked --release --manifest-path Cargo.toml --bin graphforge-benchmark-certify --bin graphforge-benchmark-graph500-generator

progressive-qualification-plan: install progressive-qualification-binaries
	test -n "$(RUNG)" && test -n "$(OUTPUT_DIR)"
	PYTHONPATH=$(CURDIR)/harness uv run --locked python -m graphforge_bench.progressive_run \
		--rung "$(RUNG)" --output-dir "$(OUTPUT_DIR)" --dry-run \
		--gf "$(CURDIR)/../target/release/gf" \
		--certify "$(CURDIR)/target/release/graphforge-benchmark-certify" \
		--generator "$(CURDIR)/target/release/graphforge-benchmark-graph500-generator"

progressive-qualification-run: install progressive-qualification-binaries
	test -n "$(RUNG)" && test -n "$(OUTPUT_DIR)"
	PYTHONPATH=$(CURDIR)/harness uv run --locked python -m graphforge_bench.progressive_run \
		--rung "$(RUNG)" --output-dir "$(OUTPUT_DIR)" \
		--gf "$(CURDIR)/../target/release/gf" \
		--certify "$(CURDIR)/target/release/graphforge-benchmark-certify" \
		--generator "$(CURDIR)/target/release/graphforge-benchmark-graph500-generator"

progressive-qualification-project-s20: install
	test -n "$(OUTPUT_DIR)" && test -n "$(PROVIDER_CAPACITY)"
	PYTHONPATH=$(CURDIR)/harness uv run --locked python -m graphforge_bench.progressive_run \
		--project-s20 --output-dir "$(OUTPUT_DIR)" \
		--provider-capacity "$(PROVIDER_CAPACITY)"
