# satorip2p Docker Testing

Test satorip2p in Docker **bridge mode** - no `--network host` required.

## Key Feature: Bridge Mode Compatible

satorip2p is designed to work in Docker's default bridge networking mode.
NAT traversal is handled automatically via:

- **Circuit Relay v2**: Peers with open ports relay traffic for others
- **DCUtR**: Direct Connection Upgrade through Relay (hole punching)
- **AutoNAT**: Automatic NAT detection

## Quick Start

### Build the image

```bash
cd /path/to/satorip2p
docker build -t satorip2p-test -f docker/Dockerfile .
```

### Run unit tests

```bash
docker run --rm satorip2p-test
```

### Run integration tests

```bash
docker run --rm satorip2p-test pytest tests/test_integration.py -v
```

## Multi-Peer Testing with Docker Compose

Test a complete P2P network with relay, publisher, and subscriber:

```bash
# Start the network
docker-compose -f docker/docker-compose.yml up --build

# In another terminal, watch the logs
docker-compose -f docker/docker-compose.yml logs -f

# Stop the network
docker-compose -f docker/docker-compose.yml down
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Docker Bridge Network                     │
│                                                             │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐     │
│  │ relay-node  │    │  publisher  │    │  subscriber │     │
│  │  (port 4001)│    │             │    │             │     │
│  │             │◄───│ Circuit     │───►│ Circuit     │     │
│  │  Relay      │    │ Relay       │    │ Relay       │     │
│  │  Service    │    │ Client      │    │ Client      │     │
│  └─────────────┘    └─────────────┘    └─────────────┘     │
│         │                  │                  │             │
│         └──────────────────┴──────────────────┘             │
│                    (Bridge Network)                         │
└─────────────────────────────────────────────────────────────┘
```

The relay node has "open ports" within the Docker network and relays
messages between publisher and subscriber nodes that cannot receive
direct incoming connections.

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `SATORI_P2P_ROLE` | Node role (relay, publisher, subscriber) | - |
| `SATORI_P2P_LISTEN_PORT` | Port for relay node | 4001 |
| `SATORI_P2P_BOOTSTRAP` | Bootstrap multiaddr | - |
| `SATORI_P2P_ENABLE_RELAY` | Enable relay service | true |

## Verifying Bridge Mode

To confirm you're NOT using host networking:

```bash
# Check network mode (should NOT say "host")
docker inspect satorip2p-relay --format='{{.HostConfig.NetworkMode}}'
# Expected output: satori-p2p (or default)

# Check container has its own IP (not host IP)
docker exec satorip2p-relay hostname -I
# Expected: 172.x.x.x (not your host IP)
```

## Security

- **No privileged mode required**
- **No --network host required**
- **No special capabilities required**
- Works with standard Docker security defaults
