.PHONY: help lint format type-check security workflow-lint license-check third-party-notices third-party-notices-check cargo-deny-licenses test test-rust test-python test-node test-scripts check clean test-tck docstring-coverage test-network benchmark test-perf test-perf-xs test-perf-slow test-perf-large coverage coverage-rust coverage-python coverage-node coverage-quick coverage-report coverage-diff coverage-strict check-coverage check-coverage-rust check-coverage-python check-coverage-node check-patch-coverage test-durations test-analytics docs-serve docs-build docs-clean cargo-build codspeed-build codspeed-build-walltime codspeed-run bench-traversal bench-tck-scenarios tck-perf bench-fixed-hop-limit bench-fixed-hop-livejournal bench-embedded-performance bench-adjacency-200m native-consumers bulk-construction-conformance-check bulk-construction-conformance cargo-test cargo-check cargo-clippy cargo-fmt cargo-fmt-check clean-builds clean-builds-all pnpm-install pnpm-build install build release-version-check package-license-verify publish-dry-run

help:  ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

lint:  ## Run ruff linter
	uv run ruff check .

format:  ## Format code with ruff
	uv run ruff format .

format-check:  ## Check code formatting
	uv run ruff format --check .

type-check:  ## Run mypy type checker
	uv run mypy crates/graphforge-bindings-py/python/graphforge --strict-optional --show-error-codes

security:  ## Run Bandit security scanner
	uv run bandit -c pyproject.toml -r crates/graphforge-bindings-py/python

workflow-lint:  ## Validate GitHub Actions workflows with pinned actionlint
	scripts/check-workflows.sh

.PHONY: gate-registry-check
gate-registry-check:  ## Validate gate classes, owners, commands, evidence, and SHA rules
	python3 scripts/ci/gate-registry.py validate
	python3 scripts/ci/test-gate-registry.py

license-check:  ## Verify Apache-2.0 metadata and distributed copies
	python3 scripts/license_check.py

release-version-check:  ## Verify Cargo/Python/Node/skills versions align
	python3 scripts/set_release_version.py --check

package-license-verify:  ## Verify packaged Cargo/npm/Python artifacts include LICENSE+NOTICE
	python3 scripts/verify_package_licenses.py

publish-dry-run:  ## Package every crate in publish order without uploading
	python3 scripts/publish_crates.py --dry-run
third-party-notices:  ## Regenerate third-party Rust license notices (requires cargo-about)
	python3 scripts/generate_third_party_notices.py

third-party-notices-check:  ## Verify checked-in third-party notices match a fresh generation
	python3 scripts/generate_third_party_notices.py --check

cargo-deny-licenses:  ## Allowlist third-party Rust dependency SPDX licenses
	cargo deny check licenses

docstring-coverage:  ## Check docstring coverage (90% minimum)
	uv run interrogate crates/graphforge-bindings-py/python/graphforge --fail-under 90 --quiet

test:  ## Run the Rust TCK and one native smoke suite per binding
	$(MAKE) test-tck
	python3 scripts/test_environment.py -- timeout 60s uv run --no-sync python crates/graphforge-bindings-py/tests/smoke.py
	python3 scripts/test_environment.py -- timeout 60s pnpm --filter @curatelabs/graphforge test:smoke

test-unit:  ## Run unit tests in parallel
	python3 scripts/test_environment.py -- uv run pytest tests/unit -n $${PYTEST_WORKERS:-4}

test-tck:  ## Run TCK compliance tests via Rust BDD runner
	python3 scripts/test_environment.py -- cargo test -p graphforge-api --test bdd

# Multi-surface coverage thresholds (#742 §2). Override per surface as needed.
COVERAGE_FAIL_UNDER_RUST ?= 80
COVERAGE_FAIL_UNDER_RUST_CRATE ?= 80
COVERAGE_FAIL_UNDER_RUST_PATCH ?= 90
COVERAGE_FAIL_UNDER_RUST_PYTHON_ADAPTER ?= 80
COVERAGE_FAIL_UNDER_RUST_NODE_ADAPTER ?= 80
COVERAGE_FAIL_UNDER_PYTHON ?= 85
COVERAGE_FAIL_UNDER_NODE ?= 85
# Back-compat alias used by coverage-python.
COVERAGE_FAIL_UNDER ?= $(COVERAGE_FAIL_UNDER_PYTHON)
# Thin Python wrapper under crates/graphforge-bindings-py/python/graphforge (not Rust).
PYTHON_COVERAGE_SRC := crates/graphforge-bindings-py/python/graphforge

_ensure-graphforge:  ## Fail fast unless the native graphforge package is importable
	@uv run python -c "import graphforge" 2>/dev/null || \
		(echo "❌ graphforge is not importable. Build the native binding first:"; \
		 echo "   maturin develop --release -m crates/graphforge-bindings-py/Cargo.toml"; \
		 exit 1)

# Default maintainer loop: make check, then the targeted tests. Use this full
# coverage run for coverage-sensitive changes or floor claims.
# Rust adapter acceptance is not pytest-cov/c8 evidence, so wrapper reports
# remain separate fail-closed measurements rather than being silently skipped.
coverage:  ## Rust + Python + Node coverage with per-surface thresholds
	@echo "━━━ Rust coverage ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	@$(MAKE) coverage-rust
	@echo "━━━ Python wrapper coverage ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	@$(MAKE) coverage-python
	@echo "━━━ Node JS surface coverage ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	@$(MAKE) coverage-node
	@echo "━━━ Coverage complete ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	@echo "Rust:   build/coverage-rust/ (ledger + merged lcov + summary; set COVERAGE_RUST_HTML=1 for core HTML)"
	@echo "Python: coverage.xml + htmlcov/"
	@echo "Node:   crates/graphforge-bindings-node/coverage/"
	@echo "✅ All surfaces collected; thresholds enforced per surface"

coverage-python:  ## Run unit tests with Python wrapper coverage (requires maturin develop)
	@$(MAKE) _ensure-graphforge
	uv run coverage erase
	uv run pytest tests/unit crates/graphforge-bindings-py/tests/cli_entrypoint.py \
		-n $${PYTEST_WORKERS:-4} \
		--cov=$(PYTHON_COVERAGE_SRC) --cov-branch \
		--cov-report=term-missing \
		--cov-report=xml \
		--cov-fail-under=$(COVERAGE_FAIL_UNDER_PYTHON)
	@test -s coverage.xml || (echo "❌ coverage.xml is empty — no Python coverage data collected" && exit 1)
	@uv run python -c "import xml.etree.ElementTree as ET; r=ET.parse('coverage.xml').getroot(); lines=int(r.get('lines-valid') or 0); assert lines>0, 'coverage.xml has zero lines-valid'; print(f'✅ Python wrapper coverage data: {lines} lines measured')"
	@$(MAKE) check-patch-coverage

coverage-quick:  ## Quick Python-wrapper-only coverage (no Rust/Node)
	@$(MAKE) _ensure-graphforge
	@echo "Running unit tests with coverage..."
	uv run coverage erase
	uv run pytest tests/unit \
		-n $${PYTEST_WORKERS:-4} \
		--cov=$(PYTHON_COVERAGE_SRC) --cov-branch \
		--cov-report=term-missing \
		--cov-report=xml

test-durations:  ## Generate .test_durations for pytest-split shard balancing
	uv run pytest tests/unit -m "not snap" \
		--store-durations --durations-path=.test_durations -q

test-analytics:  ## Run tests with analytics output (JUnit XML)
	@echo "Running tests with analytics output..."
	uv run pytest tests/unit \
		--junitxml=test-results-local.xml \
		-v

check-coverage:  ## Re-check per-surface thresholds from existing reports
	@$(MAKE) check-coverage-rust
	@$(MAKE) check-coverage-python
	@$(MAKE) check-coverage-node

check-coverage-python:  ## Validate Python wrapper coverage (≥85% default)
	@echo "Checking Python wrapper coverage (≥$(COVERAGE_FAIL_UNDER_PYTHON)%)..."
	@uv run coverage report --fail-under=$(COVERAGE_FAIL_UNDER_PYTHON) || \
		(echo "❌ Python wrapper coverage below $(COVERAGE_FAIL_UNDER_PYTHON)%" && exit 1)
	@echo "✅ Python wrapper coverage meets threshold"

check-coverage-rust:  ## Validate core (≥80%), crates (≥80%), patch (≥90%), adapters (≥80%)
	@test -f build/coverage-rust/ledger.json || \
		(echo "❌ Missing build/coverage-rust/ledger.json — run make coverage-rust first"; exit 1)
	@COVERAGE_FAIL_UNDER_RUST=$(COVERAGE_FAIL_UNDER_RUST) \
		COVERAGE_FAIL_UNDER_RUST_CRATE=$(COVERAGE_FAIL_UNDER_RUST_CRATE) \
		COVERAGE_FAIL_UNDER_RUST_PATCH=$(COVERAGE_FAIL_UNDER_RUST_PATCH) \
		COVERAGE_FAIL_UNDER_RUST_PYTHON_ADAPTER=$(COVERAGE_FAIL_UNDER_RUST_PYTHON_ADAPTER) \
		COVERAGE_FAIL_UNDER_RUST_NODE_ADAPTER=$(COVERAGE_FAIL_UNDER_RUST_NODE_ADAPTER) \
		bash scripts/check-coverage-rust.sh

coverage-strict:  ## Strict 90% coverage check for new features
	@echo "Checking strict coverage (90%)..."
	@uv run coverage report --fail-under=90 || \
		(echo "❌ Coverage below 90% - consider adding more tests" && exit 1)
	@echo "✅ Coverage meets strict threshold"

coverage-report:  ## Generate HTML coverage report and open in browser
	@echo "Generating HTML coverage report..."
	uv run coverage html
	@echo "Opening coverage report in browser..."
	@open htmlcov/index.html || xdg-open htmlcov/index.html || \
		echo "Coverage report generated at htmlcov/index.html"

coverage-diff:  ## Show coverage for changed files only
	@echo "Showing coverage for changed files..."
	@CHANGED_FILES=$$(git diff --name-only origin/main... | grep '\.py$$' || true); \
	if [ -z "$$CHANGED_FILES" ]; then \
		echo "ℹ️  No Python files changed"; \
	else \
		INCLUDE_PATTERN=$$(echo "$$CHANGED_FILES" | tr '\n' ',' | sed 's/,$$//'); \
		uv run coverage report --include="$$INCLUDE_PATTERN"; \
	fi

check-patch-coverage:  ## Validate patch coverage for changed files (90% threshold, uses existing .coverage data)
	@echo "Checking patch coverage for changed files..."
	@DIFF_OUT=$$(git diff --name-only origin/main... 2>&1) || \
		{ echo "❌ git diff failed — check that origin/main is accessible"; exit 1; }; \
	CHANGED_FILES=$$(printf '%s\n' "$$DIFF_OUT" | grep '^crates/graphforge-bindings-py/python/.*\.py$$' || true); \
	if [ -z "$$CHANGED_FILES" ]; then \
		echo "ℹ️  No source files changed - skipping patch coverage check"; \
	else \
		echo "Changed files:"; \
		echo "$$CHANGED_FILES" | sed 's/^/  - /'; \
		INCLUDE_PATTERN=$$(echo "$$CHANGED_FILES" | tr '\n' ',' | sed 's/,$$//'); \
		uv run coverage report --include="$$INCLUDE_PATTERN" --fail-under=90 || \
			(echo "❌ Patch coverage below 90% for changed files" && \
			 echo "   Run 'make coverage-report' to see detailed coverage" && \
			 exit 1); \
		echo "✅ Patch coverage meets 90% threshold"; \
	fi

check:  ## Format, lint, and static checks for every surface (no test run)
	cargo fmt --all -- --check
	cargo clippy --workspace -- -D warnings
	# Benches build only in the nightly CodSpeed workflow; compile them here so
	# an API change that breaks one fails the PR, not the next nightly.
	cargo check --workspace --benches --locked
	uv run ruff format --check .
	uv run ruff check .
	uv run mypy crates/graphforge-bindings-py/python/graphforge --strict-optional --show-error-codes
	uv run bandit -q -c pyproject.toml -r crates/graphforge-bindings-py/python
	scripts/check-workflows.sh
	scripts/ci/repo-checks.sh

# Both binding targets need the native artifacts built from this tree first
# (docs/development/agent-environment.md).
test-python:  ## Run every Python binding suite against the installed wheel
	python3 scripts/test_environment.py -- sh -c 'find crates/graphforge-bindings-py/tests -maxdepth 1 -name "*.py" ! -name "test_*.py" -print0 | sort -z | xargs -0 -n 1 -P $${PYTHON_BINDING_WORKERS:-4} uv run --no-sync python'
	python3 scripts/test_environment.py -- uv run --no-sync pytest tests/unit tests/integration crates/graphforge-bindings-py/tests/test_*.py -q -n $${PYTEST_WORKERS:-4}

test-node:  ## Run every Node binding and CLI suite against the built addon
	python3 scripts/test_environment.py -- pnpm --filter @curatelabs/graphforge test
	pnpm --filter @curatelabs/graphforge format:check
	python3 scripts/test_environment.py -- pnpm test:node-cli
	pnpm format:node-cli
	python3 scripts/test_environment.py -- pnpm smoke:node-cli
	python3 scripts/test_environment.py -- pnpm --filter @curatelabs/graphforge-cli test:lifecycle

test-scripts:  ## Run the scripts/ci self-test suites (needs uv sync --all-extras and pnpm install)
	scripts/ci/run-self-tests.sh

clean:  ## Clean up cache files
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .mypy_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	rm -f test-results*.xml coverage.xml .coverage 2>/dev/null || true
	rm -rf htmlcov/ build/coverage-rust/ crates/graphforge-bindings-node/coverage/ 2>/dev/null || true

docs-serve:  ## Serve Starlight docs locally (http://localhost:4321/)
	pnpm docs:dev

docs-build:  ## Build Starlight docs site to docs-site/dist/
	pnpm docs:build

docs-clean:  ## Remove built docs
	rm -rf docs-site/dist/ docs-site/.astro/ docs-site/src/content/docs/

# ================================================================
# Rust (Cargo) targets
# ================================================================

cargo-build:  ## Build all Rust workspace crates
	cargo build --workspace

cargo-test:  ## Run all Rust workspace tests
	python3 scripts/test_environment.py -- cargo test --workspace

# The CI Rust lane. Narrow it while iterating: make test-rust ARGS="-p graphforge-storage"
test-rust:  ## Run the Rust suite the way CI does (nextest; ARGS narrows it)
	python3 scripts/test_environment.py -- cargo nextest run $(if $(ARGS),$(ARGS),--workspace) --locked --no-fail-fast \
		-E 'not ((package(graphforge-api) and binary(bdd)) or (package(graphforge-observability) and binary(disabled_allocations)))'

# Rust llvm-cov workspace coverage. Requires:
#   cargo install cargo-llvm-cov
#   rustup component add llvm-tools-preview
# Prefer an isolated CARGO_TARGET_DIR when other builds are running (AGENTS.md).
# Set COVERAGE_RUST_HTML=1 to write the optional core HTML report. Set
# COVERAGE_RUST_RESUME=1 only to reuse same-SHA phase stamps and valid outputs.
coverage-rust:  ## Core + same-SHA Python/Node adapter Rust coverage ledger
	@COVERAGE_FAIL_UNDER_RUST=$(COVERAGE_FAIL_UNDER_RUST) \
		COVERAGE_FAIL_UNDER_RUST_CRATE=$(COVERAGE_FAIL_UNDER_RUST_CRATE) \
		COVERAGE_FAIL_UNDER_RUST_PATCH=$(COVERAGE_FAIL_UNDER_RUST_PATCH) \
		COVERAGE_FAIL_UNDER_RUST_PYTHON_ADAPTER=$(COVERAGE_FAIL_UNDER_RUST_PYTHON_ADAPTER) \
		COVERAGE_FAIL_UNDER_RUST_NODE_ADAPTER=$(COVERAGE_FAIL_UNDER_RUST_NODE_ADAPTER) \
		bash scripts/coverage-rust.sh
	@$(MAKE) check-coverage-rust

codspeed-build:  ## Build the CodSpeed benchmark targets (simulation mode; see docs/development/benchmarking.md)
	cargo codspeed build -m simulation -p graphforge-core -p graphforge-cypher
	cargo codspeed build -m simulation -p graphforge-storage --bench storage_kernels

codspeed-build-walltime:  ## Build only the durable storage I/O benchmarks in walltime mode
	cargo codspeed build -m walltime -p graphforge-storage --bench storage_io

codspeed-run:  ## Run the CodSpeed benchmarks locally (requires the codspeed CLI)
	codspeed run --mode simulation -- cargo codspeed run

bench-traversal:  ## Run the #767 traversal scaling Divan benchmarks (release, manual; see benchmarks/traversal_scaling.md)
	cargo bench -p graphforge-exec --bench traversal_scaling -- --sample-count 5

bench-tck-scenarios:  ## Run the #1653 per-scenario openCypher TCK Divan benchmark (manual; raw results under CODSPEED_ENV)
	cargo bench -p graphforge-api --bench tck_scenarios

tck-perf:  ## Host-local TCK performance run: BenchExec whole-TCK + Divan per-scenario, provenance-gated (#1654; manual, native Linux)
	PYTHONPATH=$(CURDIR)/benchmarks/harness /usr/bin/python3 -m graphforge_bench.tck_perf run --repo-root $(CURDIR) $(TCK_PERF_ARGS)

bench-merge-scaling:  ## Run the #1400 node MERGE scaling Divan benchmarks (release, manual)
	cargo bench -p graphforge-exec --bench merge_scaling -- --sample-count 5

bench-fixed-hop-limit:  ## Run the #1248 fixed-hop LIMIT benchmark (release, 1M/10M edges)
	cargo test -p graphforge-api --release --test fixed_hop_limit release_fixed_hop_limit_1m_10m -- --ignored --nocapture --test-threads=1

bench-fixed-hop-livejournal:  ## Run the #1269/#1271 cached LiveJournal LIMIT matrix (requires GF_LIVEJOURNAL_PROJECT)
	@test -n "$$GF_LIVEJOURNAL_PROJECT" || (echo "GF_LIVEJOURNAL_PROJECT is required" && exit 2)
	cargo test -p graphforge-api --release --test fixed_hop_limit release_livejournal_fixed_hop_limits -- --ignored --nocapture --test-threads=1

.PHONY: durability-certification-check
durability-certification-check:  ## Validate seeded durability certification gate (#756)
	python3 scripts/ci/durability-certification-gate.py validate
	python3 scripts/ci/test-durability-certification-gate.py

bench-embedded-performance:  ## Emit the embedded performance baseline large/manual evidence envelope (#334; hardware-specific)
	cargo test -p graphforge-api --release --test embedded_performance_baseline large_manual_matrix_emits_hardware_dataset_evidence -- --ignored --nocapture --test-threads=1

bench-adjacency-200m:  ## >200M-edge public adjacency build evidence (#336; ignored, scale-host)
	GF_ADJACENCY_SCALE_EVIDENCE_OUT="$(CURDIR)/build/adjacency-200m-evidence.json" \
	GF_ADJACENCY_SCALE_WORK="$(CURDIR)/build/adjacency-200m-work" \
	cargo test -p graphforge-api --release --test adjacency_scale_evidence adjacency_over_200m_public_build_emits_evidence -- --ignored --nocapture --test-threads=1

bench-file-backed-128m:  ## 8M/128M densified public file-backed reopen evidence (#338; ignored, scale-host)
	GF_FILE_BACKED_SCALE_EVIDENCE_OUT="$(CURDIR)/build/file-backed-128m-evidence.json" \
	GF_FILE_BACKED_SCALE_WORK="$(CURDIR)/build/file-backed-128m-work" \
	cargo test -p graphforge-api --release --test file_backed_scale_evidence densified_8m_128m_public_reopen_emits_evidence -- --ignored --nocapture --test-threads=1



native-consumers:  ## Run audited algorithm/search consumers against the installed native wheel
	python scripts/ci/run-native-consumers.py

bulk-construction-conformance-check:  ## Validate the opt-in bulk construction conformance contract
	python3 scripts/ci/bulk-construction-conformance.py validate
	python3 scripts/ci/test-bulk-construction-conformance.py

bulk-construction-conformance:  ## Run same-SHA Rust/Python/Node bulk construction conformance
	python3 scripts/ci/bulk-construction-conformance.py run \
		--output "$${GF_BULK_OUTPUT:-build/bulk-construction-conformance}"

.PHONY: bulk-construction-conformance-check bulk-construction-conformance

cargo-check:  ## Type-check all Rust workspace crates (fast)
	cargo check --workspace

cargo-clippy:  ## Run Clippy linter on all Rust workspace crates
	cargo clippy --workspace -- -D warnings

cargo-fmt:  ## Format all Rust code
	cargo fmt --all

cargo-fmt-check:  ## Check Rust code formatting (CI mode)
	cargo fmt --all -- --check

clean-builds:  ## Reclaim disk: GC stale build artifacts (keeps recent builds warm)
	scripts/clean-builds.sh stale

clean-builds-all:  ## Reclaim disk: remove ALL build artifacts (forces a full rebuild)
	scripts/clean-builds.sh all

# ================================================================
# Node (pnpm) targets
# ================================================================

pnpm-install:  ## Install all Node workspace dependencies
	pnpm install

pnpm-build:  ## Build all Node workspace packages
	pnpm -r build

# Node JS API coverage via c8 over hand-written lib/*.mjs (and exercised loader).
# Requires a built native addon (*.node); does not build it here (heavy — see docs/development/agent-environment.md).
coverage-node:  ## Run @curatelabs/graphforge JS API tests under c8 (requires *.node)
	@set -- crates/graphforge-bindings-node/*.node; \
	if [ ! -e "$$1" ]; then \
	  echo "❌ Native addon missing under crates/graphforge-bindings-node/*.node"; \
	  echo "   Build first: pnpm --filter @curatelabs/graphforge exec napi build --platform --release"; \
	  exit 1; \
	fi
	pnpm --filter @curatelabs/graphforge run test:coverage
	@test -s crates/graphforge-bindings-node/coverage/lcov.info || \
		(echo "❌ crates/graphforge-bindings-node/coverage/lcov.info missing or empty" && exit 1)
	@$(MAKE) check-coverage-node

check-coverage-node:  ## Validate Node c8 summary meets ≥85% lines (lib/ surface)
	@test -f crates/graphforge-bindings-node/coverage/coverage-summary.json || \
		(echo "❌ Missing coverage-summary.json — run make coverage-node first"; exit 1)
	@node -e "const s=require('./crates/graphforge-bindings-node/coverage/coverage-summary.json').total; \
		const min=Number(process.env.COVERAGE_FAIL_UNDER_NODE||'$(COVERAGE_FAIL_UNDER_NODE)'); \
		const pct=s.lines.pct; console.log('Node lines:', pct+'%'); \
		if (!(pct >= min)) { console.error('❌ Node coverage below '+min+'%'); process.exit(1); } \
		console.log('✅ Node coverage meets '+min+'% threshold');"

# ================================================================
# Polyglot combined targets
# ================================================================

install:  ## Install all toolchain dependencies (Python + Rust + Node)
	# The native graphforge wheel is installed outside this deps-only uv project.
	# Keep it (and other explicitly installed tooling) while syncing workspace deps.
	uv sync --all-extras --inexact
	cargo check --workspace
	pnpm install

build:  ## Build all compiled artifacts (Rust + Node)
	cargo build --workspace
	pnpm -r build
