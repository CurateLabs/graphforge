#!/usr/bin/env bash
set -euo pipefail

version=1.7.12
cache_root=${XDG_CACHE_HOME:-"$HOME/.cache"}
install_dir="$cache_root/graphforge/actionlint/$version"
binary="$install_dir/actionlint"

case "$(uname -s)-$(uname -m)" in
  Darwin-arm64)
    archive="actionlint_${version}_darwin_arm64.tar.gz"
    checksum="aba9ced2dee8d27fecca3dc7feb1a7f9a52caefa1eb46f3271ea66b6e0e6953f"
    ;;
  Darwin-x86_64)
    archive="actionlint_${version}_darwin_amd64.tar.gz"
    checksum="5b44c3bc2255115c9b69e30efc0fecdf498fdb63c5d58e17084fd5f16324c644"
    ;;
  Linux-aarch64 | Linux-arm64)
    archive="actionlint_${version}_linux_arm64.tar.gz"
    checksum="325e971b6ba9bfa504672e29be93c24981eeb1c07576d730e9f7c8805afff0c6"
    ;;
  Linux-x86_64)
    archive="actionlint_${version}_linux_amd64.tar.gz"
    checksum="8aca8db96f1b94770f1b0d72b6dddcb1ebb8123cb3712530b08cc387b349a3d8"
    ;;
  *)
    echo "Unsupported platform for actionlint: $(uname -s) $(uname -m)" >&2
    exit 1
    ;;
esac

if [[ ! -x "$binary" ]]; then
  mkdir -p "$install_dir"
  archive_path="$install_dir/$archive"
  curl --fail --location --silent --show-error \
    "https://github.com/rhysd/actionlint/releases/download/v${version}/${archive}" \
    --output "$archive_path"
  printf '%s  %s\n' "$checksum" "$archive_path" | shasum -a 256 --check
  tar -xzf "$archive_path" -C "$install_dir" actionlint
  rm "$archive_path"
fi

"$binary" -shellcheck= -pyflakes= "$@"
