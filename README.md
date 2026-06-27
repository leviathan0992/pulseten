# shamantun

`shamantun` is a lightweight, secure TUN-to-Proxy client built in pure Go.
It captures system-wide traffic via a TUN interface and forwards it to remote
upstreams using encrypted tunnels.

## Features

- **TUN-to-Proxy**: Captures all system traffic via a Layer 3 TUN interface.
- **Protocol Support**: SOCKS5-over-TLS (TCP/UDP) and HTTPS CONNECT-over-TLS.
- **Generic UDP Relay**:
  - In `socks5tls` mode with `enable_udp=true`, forwards UDP traffic through
    SOCKS5 UDP Associate. Proxied UDP/443 is intentionally dropped so QUIC
    clients quickly fall back to TCP when the upstream UDP path blackholes it.
- **DNS over TCP**:
  - DNS traffic (`udp/53`) is relayed through a proxied TCP exchange, while
    the rest of the UDP path remains generic.
- **Auto-Routing**: Can manage system routing tables for zero-config
  operation, and can also be disabled.
- **Pure Go Performance**: Built with the `gvisor` netstack and `sync.Pool`
  buffer management; no CGO or native library dependencies.

## Build and Deployment

### Build
Requires Go 1.25.9 or newer.
```bash
go build -o shamantun ./cmd/shamantun
```

### Run
```bash
# Create a local config from the tracked sample, then edit the upstreams and credentials.
cp config.example.json config.json

# Basic run with configuration
sudo ./shamantun -c config.json
```

*Note: Root privileges are required to create and manage the TUN interface.*

## Configuration

```json
{
  "mode": "socks5tls",
  "upstreams": [
    {
      "addr": "1.2.3.4:443",
      "username": "user",
      "password": "pass"
    },
    {
      "addr": "5.6.7.8:443"
    }
  ],
  "client_pem": "client.pem",
  "client_key": "client.key",
  "ca_pem": "upstream-ca.pem",
  "auto_route": true,
  "enable_udp": true,
  "udp_idle_timeout_ms": 5000,
  "insecure_skip_verify": false
}
```

### Key Parameters

- `mode`: Protocol for upstream communication (`socks5tls` or `https`).
- `upstreams`: Remote proxy endpoints. Each endpoint has an `addr` field and
  optional per-server `username` / `password`. Multiple endpoints prefer the
  last successful endpoint and fall back through the others on failure.
- `client_pem` / `client_key`: mTLS credentials. Required for SOCKS5-over-TLS
  and for HTTPS upstreams without complete `username` / `password` basic-auth
  credentials; optional for HTTPS upstreams that provide both.
- `ca_pem`: Optional PEM bundle added to the system trust store for private or
  self-signed upstream certificates. It may be set globally, overridden per
  `upstreams` entry, or explicitly cleared with `"ca_pem": ""` for an endpoint.
- `auto_route`: Automatically manage system routes. Default: `true`.
- `enable_udp`: Enables UDP capture. It is required for DNS-over-TCP handling;
  generic UDP relay is available only in SOCKS5 mode. Default: `false`.
- `udp_idle_timeout_ms`: Idle timeout for generic UDP relay flows. Default:
  `5000`. Proxied UDP/443 is disabled internally so browsers quickly fall back
  to TCP when the upstream UDP path blackholes.
- `dns_cache_size`: Maximum cached DNS entry count. Cached key/response payloads
  also share a fixed 64 MiB per-engine byte budget. Pressure eviction selects
  arbitrary entries after a bounded expiry sweep; this affects hit rate, not DNS
  response correctness. The byte budget counts logical key bytes and exact-sized
  response backing arrays; Go map and allocator metadata remain outside it.
- `insecure_skip_verify`: Disables upstream TLS certificate validation.
  Default: `false`. Only use for controlled testing.

Relay buffers and handler concurrency use bounded internal defaults, keeping the
normal configuration surface small while limiting per-flow memory retention.
Idle DNS-over-TCP connections are capped globally and per target and expire
after 30 seconds; network changes invalidate both active and pooled transports.

## Network Behavior

- **DNS over TCP**: With `enable_udp=true`, DNS queries (`udp/53`) are relayed
  through a proxied TCP exchange.
- **UDP Relay**: In `socks5tls` mode with `enable_udp=true`, UDP destinations
  are forwarded through the upstream SOCKS5 relay.
- **HTTPS Mode**: HTTPS CONNECT mode proxies TCP and, with `enable_udp=true`,
  relays DNS (`udp/53`) over TCP. Other UDP traffic is not forwarded.
- **Auto-Route Scope**: Linux and macOS auto-route manage both IPv4 and IPv6
  default routes when the host has active defaults for those families.
- **Direct Route Safety**: Direct destinations use `/32` or `/128` host routes.
  User CIDRs are never installed as aggregate system routes, preventing broad
  rules from replacing the TUN default route or bypassing protocol policy.
- **Proxy Route Safety**: Auto-route refuses to install a direct LAN/private
  bypass that overlaps a configured proxy CIDR and shuts down safely if a later
  network change creates a conflict.
- **Upstream DNS Pinning**: With auto-route enabled, hostname upstreams are
  resolved once at startup so route bypass and proxy dials use the same IP.
  Restart the process to pick up upstream DNS changes.

## Validation

```bash
go test ./...
go test -race ./...
go vet ./...
go test -run '^$' -bench . -benchmem ./cmd/shamantun
```

CI benchmark runs are informational and do not enforce a cross-run timing
threshold because shared runners do not provide stable performance isolation.

The root-only TUN end-to-end suite remains opt-in:

```bash
sudo env SHAMANTUN_TUN_E2E=1 go test ./cmd/shamantun -run TestTUNProxyEndToEnd
```

TCP proxy connections intentionally use one TLS + SOCKS5/CONNECT tunnel per
captured TCP flow. TLS session resumption reduces handshake work, but transport
multiplexing would require a compatible server-side protocol and is therefore
outside the client-only optimization boundary.

## License

MIT License. See the [LICENSE](LICENSE) file for details.
