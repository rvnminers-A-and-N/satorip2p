# Contributing to satorip2p

Thank you for your interest in contributing to satorip2p! This document provides guidelines for contributing to the project.

## Table of Contents

1. [Getting Started](#getting-started)
2. [Development Setup](#development-setup)
3. [Code Style](#code-style)
4. [Testing](#testing)
5. [Pull Request Process](#pull-request-process)
6. [Architecture Overview](#architecture-overview)
7. [Key Modules](#key-modules)

---

## Getting Started

### Prerequisites

- Python >=3.12, <3.14
- Git
- Docker (optional, for containerized development)

### Fork and Clone

```bash
# Fork the repository on GitHub, then:
git clone https://github.com/YOUR_USERNAME/satorip2p.git
cd satorip2p
```

---

## Development Setup

### Virtual Environment

```bash
# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install in development mode
pip install -e ".[dev]"
```

### Dependencies

Core dependencies:
- `py-libp2p` - P2P networking
- `python-evrmorelib` - Evrmore blockchain signing/verification
- `trio` - Async runtime
- `aiohttp` - HTTP client/server

Development dependencies:
- `pytest` - Testing framework
- `pytest-trio` - Async test support
- `pytest-cov` - Coverage reporting

---

## Code Style

### General Guidelines

- Follow PEP 8 style guide
- Use type hints for function signatures
- Keep functions focused and single-purpose
- Prefer explicit over implicit

### Naming Conventions

```python
# Classes: PascalCase
class EvrmoreWallet:
    pass

# Functions/methods: snake_case
def sign_message(message: str) -> bytes:
    pass

# Constants: UPPER_SNAKE_CASE
DEFAULT_PORT = 4001

# Private members: leading underscore
def _internal_helper():
    pass
```

### Imports

```python
# Standard library first
import os
import json
from typing import Optional, List

# Third-party packages
import trio
from libp2p import new_host

# Local imports
from satorip2p.signing import sign_message
from satorip2p.peers import Peers
```

### Docstrings

Use Google-style docstrings:

```python
def verify_message(
    message: str,
    signature: bytes,
    address: Optional[str] = None,
) -> bool:
    """Verify a signed message.

    Args:
        message: The original message that was signed.
        signature: The signature bytes (base64 encoded).
        address: Evrmore address to verify against.

    Returns:
        True if signature is valid, False otherwise.

    Raises:
        ValueError: If neither address nor pubkey provided.
    """
```

---

## Testing

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=satorip2p --cov-report=html

# Run specific test file
pytest tests/test_signing.py

# Run specific test
pytest tests/test_signing.py::TestSigning::test_sign_message

# Run with verbose output
pytest -v
```

### Writing Tests

Tests are located in the `tests/` directory. Follow these patterns:

```python
import pytest
from satorip2p.signing import EvrmoreWallet, sign_message

# Use descriptive class names
class TestEvrmoreWallet:
    """Tests for EvrmoreWallet class."""

    def test_create_from_entropy(self):
        """Test creating wallet from entropy."""
        entropy = bytes(32)  # 32 bytes
        wallet = EvrmoreWallet.from_entropy(entropy)
        assert wallet is not None
        assert wallet.address.startswith('E')

    def test_entropy_must_be_32_bytes(self):
        """Test that entropy must be exactly 32 bytes."""
        with pytest.raises(ValueError, match="32 bytes"):
            EvrmoreWallet.from_entropy(bytes(16))
```

### Test Categories

- **Unit tests**: Test individual functions/classes in isolation
- **Integration tests**: Test component interactions
- **Protocol tests**: Test network protocol behavior

### Mocking

Use mocks for external dependencies:

```python
from unittest.mock import Mock, patch

def test_with_mock_identity():
    mock_identity = Mock()
    mock_identity.address = "ETestAddress123"
    mock_identity.sign.return_value = b"mock_signature"

    # Use mock in test...
```

---

## Pull Request Process

### Before Submitting

1. **Create a feature branch**:
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Run tests**:
   ```bash
   pytest
   ```

3. **Check code style**:
   ```bash
   # If using flake8/black
   flake8 src/
   black --check src/
   ```

4. **Update documentation** if needed

### PR Guidelines

- Use descriptive PR titles
- Reference any related issues
- Include a summary of changes
- Add tests for new functionality
- Keep PRs focused on a single feature/fix

### PR Template

```markdown
## Summary
Brief description of changes.

## Changes
- Added X
- Fixed Y
- Updated Z

## Testing
- [ ] Unit tests pass
- [ ] Integration tests pass
- [ ] Manual testing performed

## Related Issues
Fixes #123
```

### Review Process

1. Submit PR against `main` branch
2. Automated tests run via CI
3. Maintainer reviews code
4. Address feedback if needed
5. PR is merged once approved

---

## Architecture Overview

satorip2p is organized into several layers:

```
satorip2p/
├── src/satorip2p/
│   ├── __init__.py          # Public API exports
│   ├── peers.py             # Core P2P networking
│   ├── signing.py           # Evrmore signing (python-evrmorelib)
│   ├── identity/            # Identity management
│   │   └── evrmore_bridge.py
│   ├── protocol/            # Satori protocols
│   │   ├── consensus.py     # Stake-weighted consensus
│   │   ├── signer.py        # Multi-sig signing
│   │   ├── uptime.py        # Uptime tracking
│   │   ├── heartbeat.py     # Heartbeat protocol
│   │   ├── oracle.py        # Oracle network
│   │   └── rewards.py       # Reward calculations
│   ├── hybrid/              # Hybrid P2P/Central mode
│   ├── compat/              # Backward compatibility
│   └── integration/         # Drop-in replacements
└── tests/                   # Test suite
```

---

## Key Modules

### signing.py

Core ECDSA signing using python-evrmorelib:

```python
from satorip2p.signing import (
    EvrmoreWallet,        # Wallet from entropy
    sign_message,         # Sign arbitrary message
    verify_message,       # Verify signature
    generate_address,     # Address from pubkey
    recover_pubkey_from_signature,  # Recover signer
)
```

### protocol/consensus.py

Stake-weighted consensus voting:

```python
from satorip2p.protocol.consensus import (
    ConsensusManager,     # Manages consensus rounds
    ConsensusVote,        # Individual vote
)
```

### protocol/signer.py

Multi-signature coordination:

```python
from satorip2p.protocol.signer import (
    SignerNode,           # Multi-sig participant
    DistributionTrigger,  # Reward distribution
)
```

### identity/evrmore_bridge.py

Bridge between Evrmore wallet and libp2p:

```python
from satorip2p.identity.evrmore_bridge import (
    EvrmoreIdentityBridge,  # Wallet-to-PeerID bridge
)
```

---

## Questions?

- Check existing [documentation](./README.md)
- Open an issue for bugs or feature requests
- Join the Satori community for discussions

Thank you for contributing!
