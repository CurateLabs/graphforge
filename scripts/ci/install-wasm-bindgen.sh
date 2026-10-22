#!/usr/bin/env bash
# Install the wasm-bindgen CLI whose version equals the `wasm-bindgen` crate in
# Cargo.lock (the CLI refuses a module built by a different version).
#
#   scripts/ci/install-wasm-bindgen.sh --print-version   print the locked version
#   scripts/ci/install-wasm-bindgen.sh <install-root>    install into <install-root>/bin
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
version="$(awk '$0 == "name = \"wasm-bindgen\"" { getline; gsub(/[^0-9.]/, "", $3); print $3; exit }' "$root/Cargo.lock")"
test -n "$version"

if [[ "${1:-}" == "--print-version" ]]; then
  echo "$version"
  exit 0
fi

install_root="${1:?usage: install-wasm-bindgen.sh <install-root> | --print-version}"
if [[ "$("$install_root/bin/wasm-bindgen" --version 2>/dev/null || true)" != "wasm-bindgen $version" ]]; then
  cargo install wasm-bindgen-cli --version "$version" --locked --root "$install_root"
fi
