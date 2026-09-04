.PHONY: install smoke smoke-python smoke-rust fly-adapter-static progressive-provider-attempt-static progressive-fly-transport-static local-admission progressive-qualification-list progressive-qualification-plan progressive-qualification-run progressive-qualification-project-s20 progressive-storage-qualification progressive-qualification-binaries progressive-provider-plan qualification-operator esc-readiness ingest-ladder-bundle parity-gate progressive-host-ladder-plan progressive-host-ladder-run progressive-host-ladder-inventory progressive-host-ladder-reclaim

install:
	uv sync --locked

smoke: smoke-python smoke-rust

parity-gate: install
	PYTHONPATH=harness uv run --locked python -m graphforge_bench.parity_gate_cli

smoke-python: install
	PYTHONPATH=harness uv run --locked python -m graphforge_bench.smoke
	PYTHONPATH=harness uv run --locked python -m unittest discover -s tests -p 'test_*.py'

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
	test -n "$(OUTPUT_DIR)" && test -n "$(PROVIDER_CAPACITY)"
	PYTHONPATH=$(CURDIR)/harness uv run --locked python -m graphforge_bench.progressive_run \
		--project-s20 --output-dir "$(OUTPUT_DIR)" \
		--provider-capacity "$(PROVIDER_CAPACITY)"

progressive-storage-qualification: install
	test -n "$(LOW_RUNG)" && test -n "$(HIGH_RUNG)" && test -n "$(EVIDENCE)"
	test -n "$(COMMIT)" && test -n "$(IMAGE_DIGEST)"
	test -n "$(LOW_RESULT_SHA256)" && test -n "$(HIGH_RESULT_SHA256)"
	test -n "$(VOLUME_BYTES)" && test -n "$(RESERVED_HEADROOM_BYTES)"
	PYTHONPATH=$(CURDIR)/harness uv run --locked python -m \
		graphforge_bench.progressive_storage_qualification \
		"$(LOW_RUNG)" "$(HIGH_RUNG)" "$(EVIDENCE)" \
		--commit "$(COMMIT)" --image-digest "$(IMAGE_DIGEST)" \
		--low-result-sha256 "$(LOW_RESULT_SHA256)" \
		--high-result-sha256 "$(HIGH_RESULT_SHA256)" \
		--volume-bytes "$(VOLUME_BYTES)" \
		--reserved-headroom-bytes "$(RESERVED_HEADROOM_BYTES)"

# Safe control-plane admission only; this never provisions a provider.
progressive-provider-plan: install
	test -n "$(COMMIT)" && test -n "$(MAXIMUM_SCALE)" && test -n "$(OUTPUT_DIR)" && test -n "$(PLAN_OUT)"
	PYTHONPATH=$(CURDIR)/harness uv run --locked python -m graphforge_bench.qualification_operator \
		plan-progressive -- \
		--root "$(CURDIR)" --output-dir "$(OUTPUT_DIR)" --commit "$(COMMIT)" \
		--maximum-scale "$(MAXIMUM_SCALE)" $(if $(PROVIDER_CAPACITY),--provider-capacity "$(PROVIDER_CAPACITY)",) \
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
progressive-host-ladder-plan: install progressive-qualification-binaries
	test -n "$(RUNG)" && test -n "$(OUTPUT_DIR)" && test -n "$(WORK_ROOT)"
	PYTHONPATH=$(CURDIR)/harness uv run --locked python -m graphforge_bench.progressive_host_run \
		--rung "$(RUNG)" --output-dir "$(OUTPUT_DIR)" --work-root "$(WORK_ROOT)" --dry-run \
		--gf "$(CURDIR)/../target/release/gf" \
		--certify "$(CURDIR)/target/release/graphforge-benchmark-certify" \
		--generator "$(CURDIR)/target/release/graphforge-benchmark-graph500-generator" \
		$(if $(HOST_CAPACITY),--host-capacity "$(HOST_CAPACITY)",)

progressive-host-ladder-run: install progressive-qualification-binaries
	test -n "$(RUNG)" && test -n "$(OUTPUT_DIR)" && test -n "$(WORK_ROOT)"
	PYTHONPATH=$(CURDIR)/harness uv run --locked python -m graphforge_bench.progressive_host_run \
		--rung "$(RUNG)" --output-dir "$(OUTPUT_DIR)" --work-root "$(WORK_ROOT)" \
		--gf "$(CURDIR)/../target/release/gf" \
		--certify "$(CURDIR)/target/release/graphforge-benchmark-certify" \
		--generator "$(CURDIR)/target/release/graphforge-benchmark-graph500-generator" \
		$(if $(HOST_CAPACITY),--host-capacity "$(HOST_CAPACITY)",)

progressive-host-ladder-inventory: install
	test -n "$(OUTPUT_DIR)" && test -n "$(WORK_ROOT)"
	PYTHONPATH=$(CURDIR)/harness uv run --locked python -m graphforge_bench.progressive_host_run \
		--inventory --output-dir "$(OUTPUT_DIR)" --work-root "$(WORK_ROOT)"

progressive-host-ladder-reclaim: install
	test -n "$(RUNG_SCALE)" && test -n "$(OUTPUT_DIR)" && test -n "$(WORK_ROOT)"
	PYTHONPATH=$(CURDIR)/harness uv run --locked python -m graphforge_bench.progressive_host_run \
		--reclaim "$(RUNG_SCALE)" --output-dir "$(OUTPUT_DIR)" --work-root "$(WORK_ROOT)"
