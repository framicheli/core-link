# Builds the gateway from source: docker build -f bin/bitcoin-gateway/Dockerfile .
# Releases use packaging/Dockerfile, which copies the CI-built binaries.
FROM rust:1-slim-bookworm AS build
WORKDIR /src
COPY . .
RUN cargo build --release --locked -p bitcoin-gateway

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=build /src/target/release/bitcoin-gateway /usr/local/bin/bitcoin-gateway
EXPOSE 50051 8080 9100
ENTRYPOINT ["/usr/local/bin/bitcoin-gateway"]
CMD ["-c", "/etc/bitcoin-gateway/gateway.toml"]
