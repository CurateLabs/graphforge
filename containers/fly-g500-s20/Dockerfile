FROM rust:1.96-bookworm AS build
WORKDIR /source
COPY . .
ARG GRAPHFORGE_GIT_SHA
RUN test -n "$GRAPHFORGE_GIT_SHA" \
    && cargo test --locked --release -p graphforge-api --test scale_g500_ladder --no-run \
    && binary="$(find target/release/deps -maxdepth 1 -type f -name 'scale_g500_ladder-*' ! -name '*.d' | head -n 1)" \
    && test -n "$binary" \
    && install -Dm755 "$binary" /out/scale-g500-ladder

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates coreutils \
    && rm -rf /var/lib/apt/lists/*
ARG GRAPHFORGE_GIT_SHA
LABEL org.opencontainers.image.revision="$GRAPHFORGE_GIT_SHA" \
      dev.graphforge.fly-s20="graphforge-fly-s20-runtime/1"
COPY --from=build /out/scale-g500-ladder /usr/local/bin/scale-g500-ladder
COPY containers/fly-g500-s20/run-s20.sh /usr/local/bin/run-s20
RUN chmod 0555 /usr/local/bin/scale-g500-ladder /usr/local/bin/run-s20
ENTRYPOINT ["/usr/local/bin/run-s20"]
