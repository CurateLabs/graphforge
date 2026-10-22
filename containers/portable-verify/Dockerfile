FROM rust:1.96-bookworm AS build
WORKDIR /source
COPY . .
RUN cargo build --locked --release -p graphforge-cli --bin gf

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /source/target/release/gf /usr/local/bin/gf
COPY containers/portable-verify/run-verify.sh /usr/local/bin/run-verify
RUN chmod 0555 /usr/local/bin/gf /usr/local/bin/run-verify
ENTRYPOINT ["/usr/local/bin/run-verify"]
