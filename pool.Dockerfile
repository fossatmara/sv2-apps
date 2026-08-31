# ── Builder ──────────────────────────────────────────────────────────────────
FROM rust:1.88 AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    capnproto libcapnp-dev curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pool-apps/ pool-apps/
COPY bitcoin-core-sv2/ bitcoin-core-sv2/
COPY stratum-apps/ stratum-apps/

# Cache mounts keep the cargo download caches and the compiled target/ dir warm
# across CI runs (persisted via buildkit-cache-dance), so a source change only
# recompiles what changed instead of every dependency. target/ lives in the
# mount and is not part of the image, so the binary is copied out in the same step.
RUN --mount=type=cache,id=cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=cargo-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=cargo-target,target=/app/target \
    cargo build --release --manifest-path pool-apps/pool/Cargo.toml --target-dir /app/target && \
    cp /app/target/release/pool_sv2 /app/pool_sv2

# ── Runtime ───────────────────────────────────────────────────────────────────
FROM ubuntu:24.04

RUN apt-get update && apt-get install -y --no-install-recommends \
    gettext-base && \
    rm -rf /var/lib/apt/lists/*

ENV IPC_DIR=/root/.bitcoin/

WORKDIR /app

COPY --from=builder /app/pool_sv2 /app/pool_sv2
COPY config/pool-jds-config.toml.template /app/pool-jds-config.toml.template
# check if the IPC file exists on a loop and wait until it does before starting the pool_sv2
ENTRYPOINT ["/bin/sh", "-c", "envsubst < /app/pool-jds-config.toml.template > /app/pool-config.toml && while [ ! -S \"$IPC_DIR/node.sock\" ]; do sleep 1; echo waiting for IPC file...; done && exec /app/pool_sv2"]
