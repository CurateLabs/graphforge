#!/bin/bash
# Reclaim disk space from Rust build artifacts.
#
# `target/` grows fast in this workspace: the dependency tree (DataFusion/Arrow)
# is large, each test/bench binary statically links it, AND cargo never garbage-
# collects the per-commit/branch binaries it leaves in `target/debug/deps/`.
# Left alone, `target/` reaches tens of GB and fills the disk (the recurring CI
# and local ENOSPC failures). This script reclaims that space.
#
# Usage:
#   scripts/clean-builds.sh [stale|incremental|all]
#
#   stale        (default) GC artifacts older than 7 days via cargo-sweep,
#                plus those from other toolchains. Falls back to `incremental`
#                if cargo-sweep is not installed. Keeps your current build warm.
#   incremental  Delete only the incremental compile cache (always safe; it is
#                regenerated on the next build). Fastest partial reclaim.
#   all          `cargo clean` — removes everything (forces a full rebuild).
#
# Install the stale-artifact GC once with: cargo install cargo-sweep

set -euo pipefail
cd "$(dirname "$0")/.."

mode="${1:-stale}"

size() { du -sh target 2>/dev/null | cut -f1 || echo "0B"; }

if [ ! -d target ]; then
  echo "No target/ directory — nothing to clean."
  exit 0
fi

echo "🧹 target/ before: $(size)"

case "$mode" in
  incremental)
    rm -rf target/debug/incremental target/release/incremental
    ;;
  stale)
    if command -v cargo-sweep >/dev/null 2>&1; then
      cargo sweep --time 7
      cargo sweep --installed
    else
      echo "ℹ️  cargo-sweep not installed — removing the incremental cache only."
      echo "    For stale-artifact GC (keeps recent builds warm): cargo install cargo-sweep"
      rm -rf target/debug/incremental target/release/incremental
    fi
    ;;
  all)
    cargo clean
    ;;
  *)
    echo "usage: $0 [stale|incremental|all]" >&2
    exit 1
    ;;
esac

echo "✅ target/ after:  $(size)"
