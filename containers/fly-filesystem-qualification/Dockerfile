FROM rust:1.90-bookworm AS build
WORKDIR /source
COPY . .
RUN cargo build --locked --release -p graphforge-api --bin graphforge-fly-filesystem-smoke

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates coreutils \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /source/target/release/graphforge-fly-filesystem-smoke /usr/local/bin/graphforge-fly-filesystem-smoke
COPY containers/fly-filesystem-qualification/run-smoke.sh /usr/local/bin/run-fly-filesystem-smoke
RUN chmod 0555 /usr/local/bin/run-fly-filesystem-smoke /usr/local/bin/graphforge-fly-filesystem-smoke
ENTRYPOINT ["/usr/local/bin/run-fly-filesystem-smoke"]
