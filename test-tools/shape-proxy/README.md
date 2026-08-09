# Shape-Proxy

SV2 share-gating proxy for testing pool difficulty adjustment algorithms. Sits
between mining devices and a pool, enabling controlled modification of the share
submission rate without touching mining hardware.

Modulating share streams against a live pool is otherwise difficult — miners
produce shares at a rate determined by physics, not configuration. This tool
complements the vardiff simulation framework by providing calibration testing
against real pool infrastructure with real miners.

```
[Miner(s)] <--SV2/Noise--> [Shape-Proxy] <--SV2/Noise--> [Pool]
                                 |
                              HTTP API
```

## Building

```bash
cargo build --release --manifest-path test-tools/shape-proxy/Cargo.toml
```

## Usage

```bash
shape-proxy --config config.example.toml
```

Set `RUST_LOG=shape_proxy=debug` for verbose output.

## Configuration

See [`config.example.toml`](config.example.toml) for all options. Key settings:

| Field | Description |
|-------|-------------|
| `upstream_address` | Pool's SV2 address |
| `downstream_listen` | Address miners connect to |
| `api_listen` | HTTP API bind address |
| `min_downstream_difficulty` | Difficulty floor (0 = disabled) |

## HTTP API

The API has no authentication. Bind `api_listen` to `127.0.0.1` if the
host is network-accessible and you don't want external profile control.

### `GET /status`

Returns JSON with upstream connection state and per-channel metrics:

```json
{
  "upstream_connected": true,
  "channels": [{
    "id": 1,
    "miner_connected": true,
    "profile": {"type": "track", "description": "1.0x supply"},
    "target_spm": 60.0,
    "forwarded_spm": 59.8,
    "supply_spm": 60.2,
    "headroom": "comfortable",
    "shares_forwarded": 120,
    "shares_gated": 5
  }]
}
```

### `POST /profile`

Set profile for all channels.

### `POST /channels/{id}/profile`

Set profile for a specific channel.

## Rate Profiles

Profiles control how shares are gated. All values are **supply-relative
multipliers** (e.g., 0.5 = forward 50% of measured hashrate). Set dynamically
via the HTTP API.

### Track (default)

Forward a constant fraction of supply.

```json
{"type": "track", "factor": 1.0}
```

### Step

Instant transition between two factors at a given time.

```json
{"type": "step", "before": 1.0, "after": 0.5, "at_secs": 300}
```

### Ramp

Linear interpolation between two factors over a duration.

```json
{"type": "ramp", "from": 0.5, "to": 1.0, "duration_secs": 600}
```

### Stall

Drop to zero for a period, then resume at 100%.

```json
{"type": "stall", "at_secs": 120, "duration_secs": 60}
```

### Burst

Spike to a higher factor for a period.

```json
{"type": "burst", "base": 0.8, "peak": 1.5, "at_secs": 180, "duration_secs": 60}
```

### Oscillate

Sinusoidal wave: `factor = base + amp * sin(2pi * t / period)`.

```json
{"type": "oscillate", "base": 0.9, "amp": 0.1, "period_secs": 300}
```

## Architecture

### Share Gating

The proxy uses a token bucket to smooth share flow:

- **Capacity**: 10 seconds of tokens at the target rate (minimum 3)
- **Refill**: target_spm / 60 tokens per second
- **Supply measurement**: count-based window (last 15 shares, difficulty-weighted)

Each arriving share is immediately ACKed to the miner (decoupling it from
pool behavior), then the gate decides whether to forward or drop.

### Difficulty Handling

The proxy forwards `SetTarget` from pool to miner so shares always meet the
pool's current difficulty. Supply is tracked as difficulty-weighted shares
(true hashrate), making relative profiles stable regardless of vardiff changes.

An optional difficulty floor (`min_downstream_difficulty`) absorbs SetTarget
messages that would lower difficulty below the configured threshold.

### Connection Resilience

- **Upstream**: Reconnects with exponential backoff (1s to 30s). Miners stay
  connected during pool disconnects; channels are re-opened on reconnect.
- **Downstream**: Each miner runs in its own task. Disconnect cleans up
  associated channel mappings.

## License

MIT OR Apache-2.0
