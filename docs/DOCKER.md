# satorip2p Docker Guide

Guide for running satorip2p in Docker containers.

## Table of Contents

1. [Quick Start](#quick-start)
2. [Building Images](#building-images)
3. [Docker Compose](#docker-compose)
4. [Multi-Peer Testing](#multi-peer-testing)
5. [Production Deployment](#production-deployment)
6. [Networking](#networking)
7. [Troubleshooting](#troubleshooting)

---

## Quick Start

### Run a Single Node

```bash
cd ~/Satori_Work/satorip2p
docker build -t satorip2p .
docker run -p 4001:4001 satorip2p
```

### Run Multi-Peer Test

```bash
cd ~/Satori_Work/satorip2p
docker-compose -f docker/docker-compose.yml up --build
```

---

## Building Images

### Standard Build

```bash
docker build -t satorip2p:latest .
```

### With Custom Wallet

```bash
docker build \
  --build-arg WALLET_PATH=/app/wallet.yaml \
  -t satorip2p:latest .
```

### Multi-Stage Build

The Dockerfile uses multi-stage builds for smaller images:

```dockerfile
# Build stage
FROM python:3.12-slim as builder
WORKDIR /app
COPY requirements.txt .
RUN pip install --user -r requirements.txt

# Runtime stage
FROM python:3.12-slim
COPY --from=builder /root/.local /root/.local
COPY . /app
```

---

## Docker Compose

### Basic Setup

```yaml
# docker-compose.yml
version: '3.8'

services:
  satori-node:
    build: .
    ports:
      - "4001:4001/tcp"
      - "4001:4001/udp"
    environment:
      - SATORIP2P_MODE=HYBRID
      - SATORIP2P_PORT=4001
    volumes:
      - ./wallet.yaml:/app/wallet.yaml:ro
    restart: unless-stopped
```

### With REST API

```yaml
version: '3.8'

services:
  satori-node:
    build: .
    ports:
      - "4001:4001/tcp"
      - "4001:4001/udp"
      - "8080:8080/tcp"  # REST API
    environment:
      - SATORIP2P_MODE=HYBRID
      - SATORIP2P_API_PORT=8080
```

### With Central Fallback

```yaml
version: '3.8'

services:
  satori-node:
    build: .
    ports:
      - "4001:4001/tcp"
      - "4001:4001/udp"
    environment:
      - SATORIP2P_MODE=HYBRID
      - SATORIP2P_CENTRAL_URL=ws://pubsub.satorinet.io:24603
```

---

## Multi-Peer Testing

### Test Network Setup

```yaml
# docker/docker-compose.yml
version: '3.8'

services:
  relay:
    build:
      context: ..
      dockerfile: docker/Dockerfile
    environment:
      - SATORIP2P_MODE=P2P_ONLY
      - SATORIP2P_IS_RELAY=true
    ports:
      - "4001:4001/tcp"
      - "4001:4001/udp"
    networks:
      - satori-net

  publisher:
    build:
      context: ..
      dockerfile: docker/Dockerfile
    environment:
      - SATORIP2P_MODE=P2P_ONLY
      - SATORIP2P_BOOTSTRAP=/dns4/relay/tcp/4001
    depends_on:
      - relay
    networks:
      - satori-net

  subscriber:
    build:
      context: ..
      dockerfile: docker/Dockerfile
    environment:
      - SATORIP2P_MODE=P2P_ONLY
      - SATORIP2P_BOOTSTRAP=/dns4/relay/tcp/4001
    depends_on:
      - relay
    networks:
      - satori-net

networks:
  satori-net:
    driver: bridge
```

### Running Tests

```bash
# Start the network
docker-compose -f docker/docker-compose.yml up -d

# Watch logs
docker-compose -f docker/docker-compose.yml logs -f

# Run integration tests
docker-compose -f docker/docker-compose.yml exec publisher \
  python -m pytest tests/test_docker_integration.py

# Cleanup
docker-compose -f docker/docker-compose.yml down
```

### Scaling Peers

```bash
# Start with 5 subscriber instances
docker-compose -f docker/docker-compose.yml up -d --scale subscriber=5
```

---

## Production Deployment

### Bootstrap Node

```yaml
# docker-compose.bootstrap.yml
version: '3.8'

services:
  bootstrap:
    build: .
    ports:
      - "4001:4001/tcp"
      - "4001:4001/udp"
    environment:
      - SATORIP2P_MODE=P2P_ONLY
      - SATORIP2P_IS_RELAY=true
      - SATORIP2P_LOG_LEVEL=INFO
    volumes:
      - bootstrap-data:/app/data
      - ./wallet.yaml:/app/wallet.yaml:ro
    restart: always
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/health"]
      interval: 30s
      timeout: 10s
      retries: 3

volumes:
  bootstrap-data:
```

### Full Node with Monitoring

```yaml
version: '3.8'

services:
  satori-node:
    build: .
    ports:
      - "4001:4001/tcp"
      - "4001:4001/udp"
      - "8080:8080/tcp"
    environment:
      - SATORIP2P_MODE=HYBRID
      - SATORIP2P_METRICS_PORT=9090
    volumes:
      - node-data:/app/data
      - ./wallet.yaml:/app/wallet.yaml:ro
    restart: always

  prometheus:
    image: prom/prometheus
    ports:
      - "9091:9090"
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
    depends_on:
      - satori-node

  grafana:
    image: grafana/grafana
    ports:
      - "3000:3000"
    depends_on:
      - prometheus

volumes:
  node-data:
```

---

## Networking

### Bridge Mode (Recommended)

Default Docker networking. Uses NAT with Circuit Relay for peer connectivity.

```yaml
services:
  node:
    networks:
      - satori-net

networks:
  satori-net:
    driver: bridge
```

**Pros:**
- Container isolation
- Works with relay nodes
- No special permissions needed

**Cons:**
- Requires relay for incoming connections
- Slightly higher latency

### Host Mode (Alternative)

Use host networking for direct port access:

```yaml
services:
  node:
    network_mode: host
    environment:
      - SATORIP2P_PORT=4001
```

**Pros:**
- Direct port access
- Lower latency
- No relay needed for incoming

**Cons:**
- No container network isolation
- Port conflicts possible
- Less portable

### Macvlan Mode (Advanced)

Give container its own IP on the network:

```yaml
networks:
  satori-macvlan:
    driver: macvlan
    driver_opts:
      parent: eth0
    ipam:
      config:
        - subnet: 192.168.1.0/24
          gateway: 192.168.1.1
```

### Port Mapping

```yaml
services:
  node:
    ports:
      # TCP for libp2p
      - "4001:4001/tcp"
      # UDP for QUIC
      - "4001:4001/udp"
      # REST API (optional)
      - "8080:8080/tcp"
      # Metrics (optional)
      - "9090:9090/tcp"
```

---

## Volumes

### Persistent Data

```yaml
services:
  node:
    volumes:
      # Message store
      - node-data:/app/data

      # Wallet (read-only)
      - ./wallet.yaml:/app/wallet.yaml:ro

      # Configuration
      - ./config.yaml:/app/config.yaml:ro

volumes:
  node-data:
    driver: local
```

### Named Volumes vs Bind Mounts

```yaml
volumes:
  # Named volume (managed by Docker)
  - node-data:/app/data

  # Bind mount (host path)
  - /host/path:/container/path

  # Read-only bind mount
  - ./config.yaml:/app/config.yaml:ro
```

---

## Health Checks

### Basic Health Check

```yaml
services:
  node:
    healthcheck:
      test: ["CMD", "python", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:8080/health')"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 10s
```

### Custom Health Script

```bash
#!/bin/bash
# docker/healthcheck.sh

curl -sf http://localhost:8080/health || exit 1
```

```yaml
services:
  node:
    healthcheck:
      test: ["CMD", "/app/docker/healthcheck.sh"]
      interval: 30s
      timeout: 10s
      retries: 3
```

---

## Resource Limits

```yaml
services:
  node:
    deploy:
      resources:
        limits:
          cpus: '1.0'
          memory: 512M
        reservations:
          cpus: '0.5'
          memory: 256M
```

---

## Troubleshooting

### Container Won't Start

```bash
# Check logs
docker logs <container_id>

# Check container status
docker inspect <container_id>

# Run interactively
docker run -it satorip2p:latest /bin/bash
```

### Network Issues

```bash
# Test connectivity from container
docker exec <container_id> ping -c 3 8.8.8.8

# Check port mapping
docker port <container_id>

# Inspect network
docker network inspect satori-net
```

### Peer Discovery Issues

```bash
# Check if relay is reachable
docker exec <container_id> python -c "
from satorip2p import Peers
import trio

async def test():
    peers = Peers(port=0)
    await peers.start()
    print(f'PeerID: {peers.peer_id}')
    print(f'Peers: {peers.connected_peers}')

trio.run(test)
"
```

### NAT Detection

```bash
# Check NAT status
docker exec <container_id> python -c "
from satorip2p.nat.docker import DockerNATDetector
detector = DockerNATDetector()
print(f'Is Docker: {detector.is_docker()}')
print(f'NAT Type: {detector.detect_nat_type()}')
"
```

### Performance Issues

```bash
# Check resource usage
docker stats <container_id>

# Check for high CPU
docker exec <container_id> top -bn1

# Check memory
docker exec <container_id> free -m
```

---

## Example Dockerfile

```dockerfile
# Dockerfile
FROM python:3.12-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    cmake \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY . .

# Install satorip2p
RUN pip install -e .

# Expose ports
EXPOSE 4001/tcp
EXPOSE 4001/udp
EXPOSE 8080/tcp

# Health check
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8080/health')"

# Default command
CMD ["python", "-m", "satorip2p.cli", "run"]
```

---

## Environment Reference

| Variable | Default | Description |
|----------|---------|-------------|
| `SATORIP2P_PORT` | `4001` | P2P listen port |
| `SATORIP2P_MODE` | `HYBRID` | Operating mode |
| `SATORIP2P_CENTRAL_URL` | `ws://pubsub.satorinet.io:24603` | Central server URL |
| `SATORIP2P_BOOTSTRAP` | (empty) | Bootstrap peers (comma-separated) |
| `SATORIP2P_IS_RELAY` | `false` | Act as relay node |
| `SATORIP2P_LOG_LEVEL` | `INFO` | Log level |
| `SATORIP2P_API_PORT` | `8080` | REST API port |
| `SATORIP2P_METRICS_PORT` | `9090` | Prometheus metrics port |
