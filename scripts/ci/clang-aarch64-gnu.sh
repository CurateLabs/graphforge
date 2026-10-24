#!/usr/bin/env bash
set -euo pipefail

# The legacy cross GCC miscompiles ring's certificate-signature verification.
# Keep napi's glibc-2.17 sysroot/linker, but compile C with Clang (#2260).
: "${TARGET_SYSROOT:?napi must provide the aarch64 GNU sysroot}"
exec clang --target=aarch64-unknown-linux-gnu --sysroot="$TARGET_SYSROOT" "$@"
