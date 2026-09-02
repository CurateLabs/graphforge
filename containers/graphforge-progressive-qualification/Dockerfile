# syntax=docker/dockerfile:1.7

FROM rust:1.96-bookworm@sha256:a339861ae23e9abb272cea45dfafde21760d2ce6577a70f8a926153677902663 AS rust-build

ARG TARGETARCH
ARG TARGETOS
WORKDIR /source
COPY . .
RUN test "${TARGETOS}/${TARGETARCH}" = linux/amd64 \
    && tar --sort=name --mtime='UTC 1970-01-01' --owner=0 --group=0 --numeric-owner \
        -cf /tmp/source-tree.tar . \
    && sha256sum /tmp/source-tree.tar | cut -d ' ' -f 1 > /source-tree-sha256 \
    && rm /tmp/source-tree.tar \
    && chmod 0444 /source-tree-sha256
RUN --mount=type=cache,id=graphforge-progressive-cargo-registry-v1,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=graphforge-progressive-cargo-git-v1,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=graphforge-progressive-target-v1,target=/cargo-target,sharing=locked \
    CARGO_TARGET_DIR=/cargo-target cargo build --locked --release --package graphforge-cli --bin gf \
    && CARGO_TARGET_DIR=/cargo-target cargo build --manifest-path benchmarks/Cargo.toml --locked --release \
        --package graphforge-benchmark-certify \
        --package graphforge-benchmark-graph500-generator \
    && install -D -m 0555 /cargo-target/release/gf /artifacts/gf \
    && install -D -m 0555 /cargo-target/release/graphforge-benchmark-certify \
        /artifacts/graphforge-benchmark-certify \
    && install -D -m 0555 /cargo-target/release/graphforge-benchmark-graph500-generator \
        /artifacts/graphforge-benchmark-graph500-generator
ARG GRAPHFORGE_COMMIT
RUN printf '%s\n' "${GRAPHFORGE_COMMIT}" | grep -Eq '^[0-9a-f]{40}$' \
    && printf '%s\n' "${GRAPHFORGE_COMMIT}" > /graphforge-commit \
    && chmod 0444 /graphforge-commit


FROM python:3.12.11-slim-bookworm@sha256:519591d6871b7bc437060736b9f7456b8731f1499a57e22e6c285135ae657bf7 AS python-deps

ENV UV_LINK_MODE=copy
WORKDIR /opt/graphforge/benchmarks
RUN apt-get update \
    && apt-get install -y --no-install-recommends fuse-overlayfs \
    && rm -rf /var/lib/apt/lists/*
RUN python -m pip install --no-cache-dir "uv==0.11.33"
COPY benchmarks/pyproject.toml benchmarks/uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project


FROM python:3.12.11-slim-bookworm@sha256:519591d6871b7bc437060736b9f7456b8731f1499a57e22e6c285135ae657bf7 AS qualification

ARG GRAPHFORGE_COMMIT
LABEL org.opencontainers.image.revision="${GRAPHFORGE_COMMIT}"
RUN mkdir -p /opt/graphforge /work \
    && chown 10001:10001 /work \
    && chmod 0755 /work

COPY --from=python-deps /usr/bin/fuse-overlayfs /usr/bin/fuse-overlayfs
COPY --from=python-deps /lib/x86_64-linux-gnu/libfuse3.so.3 /lib/x86_64-linux-gnu/libfuse3.so.3

COPY --from=rust-build /artifacts/gf /usr/local/bin/gf
COPY --from=rust-build /artifacts/graphforge-benchmark-certify \
    /usr/local/bin/graphforge-benchmark-certify
COPY --from=rust-build /artifacts/graphforge-benchmark-graph500-generator \
    /usr/local/bin/graphforge-benchmark-graph500-generator
COPY --from=rust-build --chmod=0444 /graphforge-commit /opt/graphforge/commit
COPY --from=python-deps /opt/graphforge/benchmarks/.venv /opt/graphforge/benchmarks/.venv
COPY benchmarks/harness /opt/graphforge/benchmarks/harness
COPY benchmarks/profiles /opt/graphforge/benchmarks/profiles
COPY benchmarks/schemas /opt/graphforge/benchmarks/schemas
COPY benchmarks/definitions /opt/graphforge/benchmarks/definitions
COPY benchmarks/runners/graph500-generator/src/main.rs \
    /opt/graphforge/benchmarks/runners/graph500-generator/src/main.rs
COPY --chmod=0555 containers/graphforge-progressive-qualification/run-qualification.py \
    /usr/local/bin/run-progressive-qualification
COPY --from=rust-build /source-tree-sha256 /tmp/source-tree-sha256

RUN gf_sha256="$(sha256sum /usr/local/bin/gf | cut -d ' ' -f 1)" \
    && certify_sha256="$(sha256sum /usr/local/bin/graphforge-benchmark-certify | cut -d ' ' -f 1)" \
    && generator_sha256="$(sha256sum /usr/local/bin/graphforge-benchmark-graph500-generator | cut -d ' ' -f 1)" \
    && benchexec_python_sha256="$(sha256sum /opt/graphforge/benchmarks/.venv/bin/python | cut -d ' ' -f 1)" \
    && source_tree_sha256="$(cat /tmp/source-tree-sha256)" \
    && printf '%s\n' \
        "{\"schema\":\"graphforge-progressive-provider-build/1\",\"commit\":\"${GRAPHFORGE_COMMIT}\",\"source_tree_sha256\":\"${source_tree_sha256}\",\"executables\":{\"gf_sha256\":\"${gf_sha256}\",\"certify_sha256\":\"${certify_sha256}\",\"generator_executable_sha256\":\"${generator_sha256}\",\"benchexec_python_sha256\":\"${benchexec_python_sha256}\"}}" \
        > /opt/graphforge/build-manifest.json \
    && chmod 0444 /opt/graphforge/build-manifest.json \
    && rm /tmp/source-tree-sha256

ENV PATH="/opt/graphforge/benchmarks/.venv/bin:${PATH}" \
    PYTHONPATH="/opt/graphforge/benchmarks/harness" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /
ENTRYPOINT ["/usr/local/bin/run-progressive-qualification"]
CMD ["--help"]
