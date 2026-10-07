.PHONY: install gdc-acquire smoke smoke-python smoke-rust fly-adapter-static progressive-provider-attempt-static progressive-fly-transport-static local-admission progressive-qualification-list progressive-qualification-plan progressive-qualification-run progressive-qualification-project-s20 progressive-storage-qualification progressive-qualification-binaries progressive-provider-plan qualification-operator esc-readiness ingest-ladder-bundle parity-gate progressive-host-ladder-binaries progressive-host-ladder-plan progressive-host-ladder-run progressive-host-ladder-inventory progressive-host-ladder-reclaim retain-ladder-evidence

install:
	uv sync --locked

smoke: smoke-python smoke-rust

# Download, pin-verify and extract GDC archives (SUITE, default graphalytics;
# IDENTITY_PROFILE, default scorecard) into DATASET_CACHE.
# DATASETS is a space-separated list of ids pinned by that profile.
# DATASET_CACHE must be on the same device as / and outside the repository and
# WORK_ROOT; a checksum mismatch is a typed failure and is never retried.
SUITE ?= graphalytics
IDENTITY_PROFILE ?= scorecard

gdc-acquire: install
	test -n "$(DATASET_CACHE)" && test -n "$(WORK_ROOT)" && test -n "$(DATASETS)"
	PYTHONPATH=$(CURDIR)/harness uv run --locked python -m graphforge_bench.gdc_dataset_cache \
		--suite "$(SUITE)" --profile "$(IDENTITY_PROFILE)" \
		--cache-root "$(DATASET_CACHE)" --work-root "$(WORK_ROOT)" \
		$(foreach dataset,$(DATASETS),--dataset "$(dataset)")

parity-gate: install
	PYTHONPATH=harness uv run --locked python -m graphforge_bench.parity_gate_cli

smoke-python: install
	PYTHONPATH=harness uv run --locked python -m graphforge_bench.smoke
	PYTHONPATH=harness uv run --locked python -m unittest discover -s tests -t . -p 'test_*.py'

smoke-rust: install
	cargo fmt --all -- --check
	CARGO_TARGET_DIR=$(CURDIR)/target cargo clippy --workspace --locked -- -D warnings
	CARGO_TARGET_DIR=$(CURDIR)/target cargo test --locked --manifest-path Cargo.toml
	PYTHONPATH=harness uv run --locked python -m graphforge_bench.check_rust_graph

fly-adapter-static: install
	PYTHONPATH=harness uv run --locked python -m unittest \
		tests.test_fly_adapter tests.test_fly_tiny_qualification

# Provider-free proof of the whole-attempt state machine and recovery contracts.
progressive-provider-attempt-static: install
	PYTHONPATH=harness uv run --locked python -m unittest \
		tests.test_progressive_provider_attempt

# Provider-free proof of the Fly command boundary, ESC capsule, and recovery lease.
progressive-fly-transport-static: install
	PYTHONPATH=harness uv run --locked python -m unittest \
		tests.test_progressive_fly_transport \
		tests.test_progressive_esc \
		tests.test_progressive_recovery_lease

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
	test -n "$(OUTPUT_DIR)"
	PYTHONPATH=$(CURDIR)/harness uv run --locked python -m graphforge_bench.progressive_run \
		--project-s20 --output-dir "$(OUTPUT_DIR)"

# Native host evidence is the release path. The module retains its explicit
# image/result-anchor flags for optional historical provider bundles.
progressive-storage-qualification: install
	test -n "$(LOW_RUNG)" && test -n "$(HIGH_RUNG)" && test -n "$(EVIDENCE)" && test -n "$(WORK_ROOT)"
	PYTHONPATH=$(CURDIR)/harness uv run --locked python -m \
		graphforge_bench.progressive_storage_qualification \
		"$(LOW_RUNG)" "$(HIGH_RUNG)" "$(EVIDENCE)" \
		--work-root "$(WORK_ROOT)" --reserved-headroom-bytes "$(RESERVED_HEADROOM_BYTES)"

# Safe control-plane admission only; this never provisions a provider.
progressive-provider-plan: install
	test -n "$(COMMIT)" && test -n "$(MAXIMUM_SCALE)" && test -n "$(OUTPUT_DIR)" && test -n "$(PLAN_OUT)"
	PYTHONPATH=$(CURDIR)/harness uv run --locked python -m graphforge_bench.qualification_operator \
		plan-progressive -- \
		--root "$(CURDIR)" --output-dir "$(OUTPUT_DIR)" --commit "$(COMMIT)" \
		--maximum-scale "$(MAXIMUM_SCALE)" \
		$(if $(IMAGE_DIGEST),--image-digest "$(IMAGE_DIGEST)",) \
		--plan-out "$(PLAN_OUT)"

# The registry owns the live command. Pulumi ESC supplies provider credentials;
# the Python controller still requires every gate-specific confirmation flag.
qualification-operator: install
	test -n "$(GATE)" && test -n "$(ESC_ENVIRONMENT)"
	PYTHONPATH=$(CURDIR)/harness uv run --locked python ../scripts/ci/gate-registry.py run "$(GATE)" -- \
		--environment "$(ESC_ENVIRONMENT)" $(ARGS)

# Validate that Pulumi ESC projects Fly credentials and spend authorization.
esc-readiness: install
	test -n "$(ESC_ENVIRONMENT)"
	PYTHONPATH=$(CURDIR)/harness uv run --locked python -m graphforge_bench.esc_readiness_cli \
		--environment "$(ESC_ENVIRONMENT)" \
		--gate "$(or $(GATE),progressive-ladder)" \
		$(if $(filter 1 true yes,$(ASSERT_READY)),--assert-ready,)

# Copy a validated #900 output bundle into fixtures/parity/ladder-bundle/ for #959 parity.
ingest-ladder-bundle: install
	test -n "$(SOURCE)"
	PYTHONPATH=$(CURDIR)/harness uv run --locked python -m graphforge_bench.ladder_bundle_ingest \
		--source "$(SOURCE)" $(if $(DESTINATION),--destination "$(DESTINATION)",) \
		$(if $(filter 1 true yes,$(VALIDATE_ONLY)),--validate-only,)

# OVHC-AGENCY / local-linux-cgroups-v2 host ladder (no Fly). WORK_ROOT must share
# the process-root filesystem device with `/` (ext4 on this host).
# Host BenchExec must use system Python (BenchExec + pystemd). The harness venv
# BenchExec cannot attach cgroups even inside a delegated systemd scope.
BENCHEXEC_PYTHON ?= /usr/bin/python3
HOST_TARGET_DIR ?= $(CURDIR)/../target-host-ladder
MAXIMUM_SCALE ?= 26
RESERVED_HEADROOM_BYTES ?= 80530636800
# Per-rung stop: with REFERENCE_EVIDENCE_DIR set to a prior ladder's evidence,
# each rung is staged with that rung's accepted wall plus WALL_MARGIN (capped
# at the 4 h envelope), so a regressed rung stops in minutes, not hours.
REFERENCE_EVIDENCE_DIR ?=
WALL_MARGIN ?= 0.10
HOST_LADDER_ARGS = --output-dir "$(OUTPUT_DIR)" --work-root "$(WORK_ROOT)" \
	--gf "$(HOST_TARGET_DIR)/release/gf" \
	--certify "$(HOST_TARGET_DIR)/release/graphforge-benchmark-certify" \
	--generator "$(HOST_TARGET_DIR)/release/graphforge-benchmark-graph500-generator" \
	--benchexec-python "$(BENCHEXEC_PYTHON)" \
	--reserved-headroom-bytes "$(RESERVED_HEADROOM_BYTES)" \
	--wall-margin "$(WALL_MARGIN)" \
	$(if $(QUIET_HOST_WAIT_SECONDS),--quiet-host-wait-seconds "$(QUIET_HOST_WAIT_SECONDS)",) \
	$(if $(REFERENCE_EVIDENCE_DIR),--reference-evidence-dir "$(REFERENCE_EVIDENCE_DIR)",)

progressive-host-ladder-binaries:
	CARGO_TARGET_DIR="$(HOST_TARGET_DIR)" cargo build --locked --release --manifest-path ../Cargo.toml -p graphforge-cli
	CARGO_TARGET_DIR="$(HOST_TARGET_DIR)" cargo build --locked --release --manifest-path Cargo.toml --bin graphforge-benchmark-certify --bin graphforge-benchmark-graph500-generator

# Preview only the next admissible rung; later rungs need its real measurements.
# Planning reads existing executables and never writes plans or projections.
progressive-host-ladder-plan: install
	test -n "$(OUTPUT_DIR)" && test -n "$(WORK_ROOT)"
	PYTHONPATH=$(CURDIR)/harness uv run --locked python -m graphforge_bench.progressive_host_run \
		$(if $(RUNG),--rung "$(RUNG)",--maximum-scale "$(MAXIMUM_SCALE)") $(HOST_LADDER_ARGS) --dry-run

# Resolve/build once, then sequentially advance, accept evidence, and reclaim.
progressive-host-ladder-run: install progressive-host-ladder-binaries
	test -n "$(OUTPUT_DIR)" && test -n "$(WORK_ROOT)"
	PYTHONPATH=$(CURDIR)/harness uv run --locked python -m graphforge_bench.progressive_host_run \
		$(if $(RUNG),--rung "$(RUNG)",--maximum-scale "$(MAXIMUM_SCALE)") $(HOST_LADDER_ARGS)

progressive-host-ladder-inventory: install
	test -n "$(OUTPUT_DIR)" && test -n "$(WORK_ROOT)"
	PYTHONPATH=$(CURDIR)/harness uv run --locked python -m graphforge_bench.progressive_host_run \
		--inventory --output-dir "$(OUTPUT_DIR)" --work-root "$(WORK_ROOT)"

progressive-host-ladder-reclaim: install
	test -n "$(RUNG_SCALE)" && test -n "$(OUTPUT_DIR)" && test -n "$(WORK_ROOT)"
	PYTHONPATH=$(CURDIR)/harness uv run --locked python -m graphforge_bench.progressive_host_run \
		--reclaim "$(RUNG_SCALE)" --output-dir "$(OUTPUT_DIR)" --work-root "$(WORK_ROOT)"

# #1530: retain one completed clean ladder's evidence before reporting the run.
# DESTINATION is an out-of-tree archive directory; its path and MANIFEST.sha256
# digest go on the citing issue. Raw run output is not committed under docs/ (#1625).
retain-ladder-evidence: install
	test -n "$(DESTINATION)" && test -n "$(EVIDENCE_DIR)$(SUMMARY_LOG)"
	PYTHONPATH=$(CURDIR)/harness uv run --locked python -m graphforge_bench.ladder_retention \
		$(if $(EVIDENCE_DIR),--evidence-dir "$(EVIDENCE_DIR)",) \
		$(if $(SUMMARY_LOG),--summary-log "$(SUMMARY_LOG)",) \
		$(if $(SHA),--sha "$(SHA)",) \
		$(if $(filter 1 true yes,$(INCLUDE_BENCHEXEC)),--include-benchexec,) \
		--destination "$(DESTINATION)"
