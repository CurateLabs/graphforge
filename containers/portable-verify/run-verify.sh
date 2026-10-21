#!/bin/sh
set -eu
: "${GF_VERIFY_URL:?GF_VERIFY_URL must contain an HTTPS portable object URL}"
exec gf portable verify --url "$GF_VERIFY_URL" --json
