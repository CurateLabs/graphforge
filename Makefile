.PHONY: help lint format type-check security workflow-lint license-check third-party-notices third-party-notices-check cargo-deny-licenses test pre-push pre-push-fast clean test-tck docstring-coverage test-network benchmark test-perf test-perf-xs test-perf-slow test-perf-large coverage coverage-rust coverage-python coverage-node coverage-quick coverage-report coverage-diff coverage-strict check-coverage check-coverage-rust check-coverage-python check-coverage-node check-patch-coverage test-durations test-analytics docs-serve docs-build docs-clean cargo-build bench-traversal bench-fixed-hop-limit bench-fixed-hop-livejournal native-consumers release-load-matrix-check release-load-matrix bulk-construction-conformance-check bulk-construction-conformance cargo-test cargo-check cargo-clippy cargo-fmt cargo-fmt-check clean-builds clean-builds-all pnpm-install pnpm-build pnpm-test-bdd install build release-version-check package-license-verify publish-dry-run publish-dry-run-npm publish-dry-run-docs publish-dry-run-python publish-dry-run-cargo record-release-artifacts clean-env-verify-check clean-env-verify-preflight clean-env-verify

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

license-check:  ## Verify Apache-2.0 metadata and distributed copies
	python3 scripts/license_check.py

release-version-check:  ## Verify Cargo/Python/Node/skills versions align
	python3 scripts/set_release_version.py --check

package-license-verify:  ## Verify packaged Cargo/npm/Python artifacts include LICENSE+NOTICE
	python3 scripts/verify_package_licenses.py

publish-dry-run:  ## Local v0.5 publish dry-runs (npm/docs/python); never prod registries
	python3 scripts/publish_dry_run.py --surface npm,docs,python --report target/publish-dry-run/evidence.json
publish-dry-run-npm:  ## npm publish --dry-run for Node binding + agent-skills
	python3 scripts/publish_dry_run.py --surface npm
publish-dry-run-docs:  ## Docs preview build (pnpm docs:build)
	python3 scripts/publish_dry_run.py --surface docs
publish-dry-run-python:  ## Local maturin sdist packaging check (not TestPyPI upload)
	python3 scripts/publish_dry_run.py --surface python
publish-dry-run-cargo:  ## cargo package --list for all 15 crates.io packages in plan order
	python3 scripts/publish_dry_run.py --surface cargo-package

record-release-artifacts:  ## Hash artifacts in DIST_DIR into a release record JSON
	python3 scripts/record_release_artifacts.py --version $${VERSION:?set VERSION} --dist-dir $${DIST_DIR:?set DIST_DIR} --out $${OUT:-docs/releases/records/v$${VERSION}-artifacts.json}

third-party-notices:  ## Regenerate third-party Rust license notices (requires cargo-about)
	python3 scripts/generate_third_party_notices.py

third-party-notices-check:  ## Verify checked-in third-party notices match a fresh generation
	python3 scripts/generate_third_party_notices.py --check

cargo-deny-licenses:  ## Allowlist third-party Rust dependency SPDX licenses
	cargo deny check licenses

docstring-coverage:  ## Check docstring coverage (90% minimum)
	uv run interrogate crates/graphforge-bindings-py/python/graphforge --fail-under 90 --quiet

test:  ## Run all tests in parallel (excludes snap/network downloads)
	uv run pytest tests/ -n $${PYTEST_WORKERS:-4} -m "not snap"

test-unit:  ## Run unit tests in parallel
	uv run pytest tests/unit -n $${PYTEST_WORKERS:-4}

test-tck:  ## Run TCK compliance tests via Rust BDD runner
	cargo test -p graphforge-core --test bdd

# Multi-surface coverage thresholds (#742 §2). Override per surface as needed.
COVERAGE_FAIL_UNDER_RUST ?= 95
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

check-coverage-rust:  ## Validate core (≥95%), crates (≥80%), patch (≥90%), adapters (≥80%)
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

pre-push-fast:  ## Run fast checks only — format, lint, type, security, docstrings (no coverage, ~30s)
	@echo "━━━ Public API BDD policy ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	@python3 scripts/ci/api-bdd-policy.py --check-issues
	@python3 scripts/ci/test-api-bdd-policy.py
	@echo "━━━ Format check ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	@$(MAKE) format-check
	@echo "━━━ Lint ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	@$(MAKE) lint
	@echo "━━━ Type check ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	@$(MAKE) type-check
	@echo "━━━ Security ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	@$(MAKE) security
	@echo "━━━ Workflow validation ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	@$(MAKE) workflow-lint
	@echo "━━━ License policy ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	@$(MAKE) license-check
	@echo "━━━ Docstring coverage ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	@$(MAKE) docstring-coverage
	@echo "✅ Fast checks passed! Run 'make pre-push' to include coverage."

pre-push:  ## Run local policy checks plus multi-surface coverage thresholds
	@$(MAKE) pre-push-fast
	@uv run --no-sync python scripts/ci/test-rust-coverage-ledger.py
	@bash scripts/ci/test-coverage-rust.sh
	@echo "━━━ Coverage + thresholds (Rust + Python + Node) ━━━━━━━━━━━━━━━━━━━━━━━"
	@$(MAKE) coverage
	@echo "━━━ Public API BDD mutation sentinels ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	@uv run --no-sync python scripts/ci/test-api-bdd-mutations.py
	@echo "✅ All pre-push checks passed!"

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
	cargo test --workspace

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

bench-traversal:  ## Run the #767 traversal scaling benchmark (release, manual; see benchmarks/traversal_scaling.md)
	cargo test -p graphforge-exec --release --test bench_traversal_scaling -- --ignored --nocapture --test-threads=1

bench-fixed-hop-limit:  ## Run the #1248 fixed-hop LIMIT benchmark (release, 1M/10M edges)
	cargo test -p graphforge-api --release --test fixed_hop_limit release_fixed_hop_limit_1m_10m -- --ignored --nocapture --test-threads=1

bench-fixed-hop-livejournal:  ## Run the #1269/#1271 cached LiveJournal LIMIT matrix (requires GF_LIVEJOURNAL_PROJECT)
	@test -n "$$GF_LIVEJOURNAL_PROJECT" || (echo "GF_LIVEJOURNAL_PROJECT is required" && exit 2)
	cargo test -p graphforge-api --release --test fixed_hop_limit release_livejournal_fixed_hop_limits -- --ignored --nocapture --test-threads=1

native-consumers:  ## Run audited M18/M19 consumers against the installed native wheel
	python scripts/ci/run-native-consumers.py

release-load-matrix-check:  ## Validate the versioned XS-XL release-load contracts
	python3 scripts/ci/release-load-matrix.py validate
	python3 scripts/ci/test-release-load-matrix.py
	python3 scripts/ci/test-release-load-executor.py
	python3 scripts/ci/test-release-load-probe-parity.py

release-load-matrix:  ## Run the complete local/release-machine matrix against built native artifacts
	@test -n "$$GF_LOAD_SHA" || (echo "GF_LOAD_SHA is required" && exit 2)
	python3 scripts/ci/release-load-matrix.py run \
		--sha "$$GF_LOAD_SHA" \
		--work "$${GF_LOAD_WORK:-build/release-load}" \
		--output "$${GF_LOAD_OUTPUT:-build/release-load-evidence.json}"

clean-env-verify-check:  ## Unit-test the post-publication clean-env harness (#2795)
	python3 scripts/ci/test-clean-env-verify.py

clean-env-verify-preflight:  ## Probe public registries for VERSION (fails closed if unpublished)
	@test -n "$(VERSION)" || (echo "VERSION is required (e.g. VERSION=0.5.0)" && exit 2)
	python3 scripts/ci/clean-env-verify.py preflight --version "$(VERSION)" \
		$(if $(OUTPUT),--output "$(OUTPUT)",)

clean-env-verify:  ## Run clean-env lanes against public registries (post-§6 only)
	@test -n "$(VERSION)" || (echo "VERSION is required (e.g. VERSION=0.5.0)" && exit 2)
	python3 scripts/ci/clean-env-verify.py run --version "$(VERSION)" \
		$(if $(RELEASE_RECORD),--release-record "$(RELEASE_RECORD)" --all,--lane pip --lane npm --lane cli --lane skills --lane reopen --lane urls) \
		$(if $(OUTPUT),--output "$(OUTPUT)",) \
		$(if $(WORK),--work "$(WORK)",)

bulk-construction-conformance-check:  ## Validate the opt-in bulk construction conformance contract
	python3 scripts/ci/bulk-construction-conformance.py validate
	python3 scripts/ci/test-bulk-construction-conformance.py

bulk-construction-conformance:  ## Run same-SHA Rust/Python/Node bulk construction conformance
	python3 scripts/ci/bulk-construction-conformance.py run \
		--output "$${GF_BULK_OUTPUT:-build/bulk-construction-conformance}"

.PHONY: bulk-construction-conformance-check bulk-construction-conformance

.PHONY: m1-release-certification-check
m1-release-certification-check:  ## Validate the final M1 release certification aggregate gate
	python3 scripts/ci/test-m1-release-certification.py

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

pnpm-test-bdd:  ## Run BDD tests across Node workspace packages
	pnpm -r test:bdd

# Node JS API coverage via c8 over hand-written lib/*.mjs (and exercised loader).
# Requires a built native addon (*.node); does not build it here (heavy — see AGENTS.md).
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
	uv sync --all-extras
	cargo check --workspace
	pnpm install

build:  ## Build all compiled artifacts (Rust + Node)
	cargo build --workspace
	pnpm -r build
