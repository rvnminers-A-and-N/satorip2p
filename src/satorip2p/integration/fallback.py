"""
satorip2p.integration.fallback - Fallback Handler with Circuit Breaker

Provides robust fallback handling between P2P and Central modes with:
- Circuit breaker pattern to prevent cascading failures
- Automatic retry with exponential backoff
- Smart fallback decisions based on error types
- Recovery detection for auto-switching back

Usage:
    from satorip2p.integration.fallback import FallbackHandler, with_fallback

    handler = FallbackHandler()

    # Use decorator
    @with_fallback(handler)
    def my_operation():
        # P2P operation
        pass

    # Or use context manager
    async with handler.try_p2p("publish") as ctx:
        if ctx.should_try_p2p:
            await p2p_publish()
        if ctx.needs_fallback:
            await central_publish()
"""

from typing import Callable, Optional, Any, TypeVar, Generic
from dataclasses import dataclass, field
from enum import Enum, auto
from contextlib import asynccontextmanager, contextmanager
from functools import wraps
import asyncio
import time
import logging
import threading

from .metrics import IntegrationMetrics, log_operation, log_fallback

logger = logging.getLogger("satorip2p.integration.fallback")

T = TypeVar('T')


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = auto()      # Normal operation, requests flow through
    OPEN = auto()        # Failing, requests are blocked
    HALF_OPEN = auto()   # Testing if service recovered


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker."""
    failure_threshold: int = 5      # Failures before opening circuit
    success_threshold: int = 3      # Successes to close circuit from half-open
    timeout_seconds: float = 30.0   # Time before trying half-open
    half_open_max_calls: int = 3    # Max concurrent calls in half-open


class CircuitBreaker:
    """
    Circuit breaker implementation for P2P operations.

    States:
    - CLOSED: Normal operation. Failures increment counter.
    - OPEN: After failure_threshold failures. All calls blocked for timeout_seconds.
    - HALF_OPEN: After timeout. Limited calls allowed to test recovery.

    If test calls succeed, circuit closes. If they fail, circuit opens again.
    """

    def __init__(self, name: str, config: Optional[CircuitBreakerConfig] = None):
        self.name = name
        self.config = config or CircuitBreakerConfig()
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: Optional[float] = None
        self._half_open_calls = 0
        self._lock = threading.Lock()

    @property
    def state(self) -> CircuitState:
        """Get current state, potentially transitioning from OPEN to HALF_OPEN."""
        with self._lock:
            if self._state == CircuitState.OPEN:
                if self._last_failure_time and (
                    time.time() - self._last_failure_time > self.config.timeout_seconds
                ):
                    self._state = CircuitState.HALF_OPEN
                    self._half_open_calls = 0
                    logger.info(f"Circuit {self.name}: OPEN -> HALF_OPEN")
            return self._state

    def is_available(self) -> bool:
        """Check if requests can go through."""
        state = self.state
        if state == CircuitState.CLOSED:
            return True
        if state == CircuitState.HALF_OPEN:
            with self._lock:
                if self._half_open_calls < self.config.half_open_max_calls:
                    self._half_open_calls += 1
                    return True
            return False
        return False  # OPEN

    def record_success(self):
        """Record a successful call."""
        with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self.config.success_threshold:
                    self._state = CircuitState.CLOSED
                    self._failure_count = 0
                    self._success_count = 0
                    logger.info(f"Circuit {self.name}: HALF_OPEN -> CLOSED (recovered)")
            elif self._state == CircuitState.CLOSED:
                # Reset failure count on success
                self._failure_count = max(0, self._failure_count - 1)

    def record_failure(self):
        """Record a failed call."""
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()

            if self._state == CircuitState.HALF_OPEN:
                # Failed during test, go back to OPEN
                self._state = CircuitState.OPEN
                self._success_count = 0
                logger.warning(f"Circuit {self.name}: HALF_OPEN -> OPEN (test failed)")
            elif self._state == CircuitState.CLOSED:
                if self._failure_count >= self.config.failure_threshold:
                    self._state = CircuitState.OPEN
                    logger.warning(
                        f"Circuit {self.name}: CLOSED -> OPEN "
                        f"(failures: {self._failure_count})"
                    )

    def reset(self):
        """Reset the circuit breaker."""
        with self._lock:
            self._state = CircuitState.CLOSED
            self._failure_count = 0
            self._success_count = 0
            self._last_failure_time = None
            self._half_open_calls = 0


class RetryConfig:
    """Configuration for retry behavior."""
    def __init__(
        self,
        max_retries: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 30.0,
        exponential_base: float = 2.0,
        jitter: bool = True,
    ):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
        self.jitter = jitter

    def get_delay(self, attempt: int) -> float:
        """Calculate delay for retry attempt."""
        import random
        delay = min(
            self.base_delay * (self.exponential_base ** attempt),
            self.max_delay
        )
        if self.jitter:
            delay *= (0.5 + random.random())
        return delay


@dataclass
class FallbackContext:
    """Context for fallback operations."""
    operation: str
    attempt: int = 0
    should_try_p2p: bool = True
    p2p_succeeded: bool = False
    p2p_error: Optional[str] = None
    needs_fallback: bool = False
    fallback_succeeded: bool = False
    fallback_error: Optional[str] = None
    start_time: float = field(default_factory=time.time)

    def mark_p2p_success(self):
        """Mark P2P operation as successful."""
        self.p2p_succeeded = True
        self.needs_fallback = False

    def mark_p2p_failure(self, error: str):
        """Mark P2P operation as failed, triggering fallback."""
        self.p2p_succeeded = False
        self.p2p_error = error
        self.needs_fallback = True

    def mark_fallback_success(self):
        """Mark fallback operation as successful."""
        self.fallback_succeeded = True

    def mark_fallback_failure(self, error: str):
        """Mark fallback operation as failed."""
        self.fallback_succeeded = False
        self.fallback_error = error

    @property
    def overall_success(self) -> bool:
        """Check if operation succeeded (via P2P or fallback)."""
        return self.p2p_succeeded or self.fallback_succeeded

    @property
    def duration_ms(self) -> float:
        """Get operation duration in milliseconds."""
        return (time.time() - self.start_time) * 1000


class FallbackHandler:
    """
    Handler for fallback operations between P2P and Central.

    Features:
    - Circuit breaker per operation type
    - Automatic retry with backoff
    - Metrics collection
    - Smart fallback decisions
    """

    def __init__(
        self,
        circuit_config: Optional[CircuitBreakerConfig] = None,
        retry_config: Optional[RetryConfig] = None,
        enable_fallback: bool = True,
    ):
        self._circuit_config = circuit_config or CircuitBreakerConfig()
        self._retry_config = retry_config or RetryConfig()
        self._enable_fallback = enable_fallback
        self._circuits: dict[str, CircuitBreaker] = {}
        self._lock = threading.Lock()
        self._metrics = IntegrationMetrics.get_instance()

    def get_circuit(self, operation: str) -> CircuitBreaker:
        """Get or create circuit breaker for operation."""
        with self._lock:
            if operation not in self._circuits:
                self._circuits[operation] = CircuitBreaker(
                    f"p2p_{operation}",
                    self._circuit_config
                )
            return self._circuits[operation]

    def should_try_p2p(self, operation: str) -> bool:
        """Check if P2P should be attempted for this operation."""
        circuit = self.get_circuit(operation)
        return circuit.is_available()

    @asynccontextmanager
    async def try_p2p(self, operation: str):
        """
        Async context manager for P2P operations with fallback.

        Usage:
            async with handler.try_p2p("publish") as ctx:
                if ctx.should_try_p2p:
                    try:
                        await p2p_publish()
                        ctx.mark_p2p_success()
                    except Exception as e:
                        ctx.mark_p2p_failure(str(e))

                if ctx.needs_fallback:
                    try:
                        await central_publish()
                        ctx.mark_fallback_success()
                    except Exception as e:
                        ctx.mark_fallback_failure(str(e))
        """
        circuit = self.get_circuit(operation)
        ctx = FallbackContext(
            operation=operation,
            should_try_p2p=circuit.is_available(),
        )

        try:
            yield ctx
        finally:
            # Record results
            if ctx.p2p_succeeded:
                circuit.record_success()
                log_operation(operation, True, "p2p", ctx.duration_ms)
            elif ctx.p2p_error:
                circuit.record_failure()
                log_operation(operation, False, "p2p", ctx.duration_ms, ctx.p2p_error)

                if ctx.needs_fallback and self._enable_fallback:
                    log_fallback(operation, ctx.p2p_error)

                    if ctx.fallback_succeeded:
                        log_operation(operation, True, "central", ctx.duration_ms)
                    elif ctx.fallback_error:
                        log_operation(operation, False, "central", ctx.duration_ms, ctx.fallback_error)

    @contextmanager
    def try_p2p_sync(self, operation: str):
        """
        Synchronous context manager for P2P operations with fallback.

        Usage:
            with handler.try_p2p_sync("publish") as ctx:
                if ctx.should_try_p2p:
                    try:
                        p2p_publish()
                        ctx.mark_p2p_success()
                    except Exception as e:
                        ctx.mark_p2p_failure(str(e))

                if ctx.needs_fallback:
                    try:
                        central_publish()
                        ctx.mark_fallback_success()
                    except Exception as e:
                        ctx.mark_fallback_failure(str(e))
        """
        circuit = self.get_circuit(operation)
        ctx = FallbackContext(
            operation=operation,
            should_try_p2p=circuit.is_available(),
        )

        try:
            yield ctx
        finally:
            if ctx.p2p_succeeded:
                circuit.record_success()
                log_operation(operation, True, "p2p", ctx.duration_ms)
            elif ctx.p2p_error:
                circuit.record_failure()
                log_operation(operation, False, "p2p", ctx.duration_ms, ctx.p2p_error)

                if ctx.needs_fallback and self._enable_fallback:
                    log_fallback(operation, ctx.p2p_error)

                    if ctx.fallback_succeeded:
                        log_operation(operation, True, "central", ctx.duration_ms)
                    elif ctx.fallback_error:
                        log_operation(operation, False, "central", ctx.duration_ms, ctx.fallback_error)

    async def execute_with_fallback(
        self,
        operation: str,
        p2p_func: Callable[[], Any],
        central_func: Callable[[], Any],
        is_async: bool = True,
    ) -> Any:
        """
        Execute operation with automatic P2P -> Central fallback.

        Args:
            operation: Operation name for metrics
            p2p_func: P2P implementation function
            central_func: Central server implementation function
            is_async: Whether functions are async

        Returns:
            Result from successful operation

        Raises:
            Exception: If both P2P and Central fail
        """
        circuit = self.get_circuit(operation)
        start_time = time.time()
        p2p_error = None

        # Try P2P if circuit allows
        if circuit.is_available():
            for attempt in range(self._retry_config.max_retries):
                try:
                    if is_async:
                        result = await p2p_func()
                    else:
                        result = p2p_func()

                    circuit.record_success()
                    log_operation(
                        operation, True, "p2p",
                        (time.time() - start_time) * 1000
                    )
                    return result

                except Exception as e:
                    p2p_error = str(e)
                    logger.warning(
                        f"P2P {operation} attempt {attempt + 1} failed: {e}"
                    )

                    if attempt < self._retry_config.max_retries - 1:
                        delay = self._retry_config.get_delay(attempt)
                        if is_async:
                            await asyncio.sleep(delay)
                        else:
                            time.sleep(delay)

            # All retries failed
            circuit.record_failure()
            log_operation(
                operation, False, "p2p",
                (time.time() - start_time) * 1000,
                p2p_error
            )

        # Fallback to Central
        if self._enable_fallback:
            log_fallback(operation, p2p_error or "circuit_open")
            fallback_start = time.time()

            try:
                if is_async:
                    result = await central_func()
                else:
                    result = central_func()

                log_operation(
                    operation, True, "central",
                    (time.time() - fallback_start) * 1000
                )
                return result

            except Exception as e:
                log_operation(
                    operation, False, "central",
                    (time.time() - fallback_start) * 1000,
                    str(e)
                )
                raise

        # No fallback, re-raise P2P error
        if p2p_error:
            raise Exception(f"P2P {operation} failed: {p2p_error}")
        raise Exception(f"P2P circuit open for {operation}")

    def reset_circuit(self, operation: str):
        """Reset circuit breaker for an operation."""
        circuit = self.get_circuit(operation)
        circuit.reset()

    def reset_all_circuits(self):
        """Reset all circuit breakers."""
        with self._lock:
            for circuit in self._circuits.values():
                circuit.reset()

    def get_circuit_states(self) -> dict[str, str]:
        """Get states of all circuit breakers."""
        with self._lock:
            return {
                name: circuit.state.name
                for name, circuit in self._circuits.items()
            }


# Global handler instance
_global_handler: Optional[FallbackHandler] = None


def get_fallback_handler() -> FallbackHandler:
    """Get global fallback handler instance."""
    global _global_handler
    if _global_handler is None:
        _global_handler = FallbackHandler()
    return _global_handler


def with_fallback(
    handler: Optional[FallbackHandler] = None,
    operation: Optional[str] = None,
):
    """
    Decorator for functions with P2P/Central fallback.

    The decorated function should accept a 'use_central' keyword argument
    that indicates whether to use central server.

    Usage:
        @with_fallback(operation="publish")
        def publish(data, use_central=False):
            if use_central:
                return central_publish(data)
            return p2p_publish(data)

        # Or with explicit P2P and Central implementations
        @with_fallback(operation="subscribe")
        async def subscribe(topic):
            # This is the P2P version
            await p2p_subscribe(topic)
    """
    handler = handler or get_fallback_handler()

    def decorator(func):
        op_name = operation or func.__name__

        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            with handler.try_p2p_sync(op_name) as ctx:
                if ctx.should_try_p2p:
                    try:
                        result = func(*args, use_central=False, **kwargs)
                        ctx.mark_p2p_success()
                        return result
                    except Exception as e:
                        ctx.mark_p2p_failure(str(e))

                if ctx.needs_fallback:
                    try:
                        result = func(*args, use_central=True, **kwargs)
                        ctx.mark_fallback_success()
                        return result
                    except Exception as e:
                        ctx.mark_fallback_failure(str(e))
                        raise

        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            async with handler.try_p2p(op_name) as ctx:
                if ctx.should_try_p2p:
                    try:
                        result = await func(*args, use_central=False, **kwargs)
                        ctx.mark_p2p_success()
                        return result
                    except Exception as e:
                        ctx.mark_p2p_failure(str(e))

                if ctx.needs_fallback:
                    try:
                        result = await func(*args, use_central=True, **kwargs)
                        ctx.mark_fallback_success()
                        return result
                    except Exception as e:
                        ctx.mark_fallback_failure(str(e))
                        raise

        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        return sync_wrapper

    return decorator
