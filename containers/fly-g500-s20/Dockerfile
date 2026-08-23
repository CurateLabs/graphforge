FROM rust:1.96-bookworm AS build
WORKDIR /source
COPY . .
RUN cargo test --locked --release -p graphforge-api --test scale_g500_ladder --no-run
RUN set -eu; \
    binary="$(find target/release/deps -maxdepth 1 -type f -name 'scale_g500_ladder-*' ! -name '*.d' | head -n 1)"; \
    test -n "$binary"; \
    install -Dm755 "$binary" /out/scale-g500-ladder

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates coreutils \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/scale-g500-ladder /usr/local/bin/scale-g500-ladder
COPY containers/fly-g500-s20/run-s20.sh /usr/local/bin/run-s20
RUN chmod 0555 /usr/local/bin/scale-g500-ladder /usr/local/bin/run-s20
ENTRYPOINT ["/usr/local/bin/run-s20"]
