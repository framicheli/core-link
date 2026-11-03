# The release image: the prebuilt binary for each platform, on distroless.
# The release workflow puts the binaries in dist/<arch>/ first:
#   docker buildx build -f bin/bitcoin-gateway/packaging/Dockerfile \
#     --platform linux/amd64,linux/arm64 .
FROM gcr.io/distroless/cc-debian12:nonroot
ARG TARGETARCH
COPY dist/${TARGETARCH}/bitcoin-gateway /usr/local/bin/bitcoin-gateway
# gRPC, HTTP (REST and WebSocket), metrics. Listening on 0.0.0.0 needs
# [tls] in the config, or --insecure behind a proxy that terminates TLS.
EXPOSE 50051 8080 9100
ENTRYPOINT ["/usr/local/bin/bitcoin-gateway"]
CMD ["-c", "/etc/bitcoin-gateway/gateway.toml"]
