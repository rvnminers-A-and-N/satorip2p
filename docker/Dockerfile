# satorip2p/docker/Dockerfile
#
# Dockerfile for testing satorip2p in Docker bridge mode.
# IMPORTANT: This does NOT require --network host or privileged mode.
# NAT traversal is handled via Circuit Relay and DCUtR.
#
# Build:   docker build -t satorip2p-test -f docker/Dockerfile .
# Run:     docker run --rm satorip2p-test
# Tests:   docker run --rm satorip2p-test pytest tests/ -v

FROM python:3.12-slim

LABEL maintainer="Satori Network"
LABEL description="satorip2p P2P module for Satori - Bridge mode compatible"

# Install system dependencies for libp2p compilation and kawpow
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    git \
    cmake \
    make \
    libffi-dev \
    libgmp-dev \
    libssl-dev \
    python3-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the package (excluding .venv, __pycache__, etc. via .dockerignore)
COPY . .

# Install the package in development mode
RUN pip install -e .

# Install test dependencies
RUN pip install pytest pytest-asyncio pytest-trio pytest-timeout pandas

# IMPORTANT: Fix multihash library AFTER all installs
# py-multihash gets installed as dependency but has incompatible API
# pymultihash provides correct 'multihash' module with Func attribute
RUN pip uninstall py-multihash -y 2>/dev/null || true && \
    pip install pymultihash==0.8.2 --force-reinstall

# Expose default libp2p ports (for when ports CAN be opened)
# These are optional - Circuit Relay works without exposed ports
EXPOSE 4001/tcp
EXPOSE 4001/udp

# Default command runs tests
CMD ["pytest", "tests/", "-v", "--timeout=60"]

# Health check - verify package imports correctly
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "from satorip2p import Peers; print('OK')" || exit 1
