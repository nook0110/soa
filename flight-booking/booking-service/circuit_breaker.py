import time
import logging
from enum import Enum
from threading import Lock
from typing import Callable, Any

logger = logging.getLogger(__name__)

class CircuitState(Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"

class CircuitBreaker:
    def __init__(self, failure_threshold: int = 5, timeout_seconds: int = 30):
        self.failure_threshold = failure_threshold
        self.timeout_seconds = timeout_seconds
        self.failure_count = 0
        self.last_failure_time = None
        self.state = CircuitState.CLOSED
        self.lock = Lock()

    def call(self, func: Callable, *args, **kwargs) -> Any:
        with self.lock:
            if self.state == CircuitState.OPEN:
                if time.time() - self.last_failure_time >= self.timeout_seconds:
                    logger.info("Circuit Breaker: OPEN → HALF_OPEN")
                    self.state = CircuitState.HALF_OPEN
                else:
                    logger.warning("Circuit Breaker: OPEN - Request blocked")
                    raise Exception("Circuit breaker is OPEN - service unavailable")

        try:
            result = func(*args, **kwargs)
            
            with self.lock:
                if self.state == CircuitState.HALF_OPEN:
                    logger.info("Circuit Breaker: HALF_OPEN → CLOSED (success)")
                    self.state = CircuitState.CLOSED
                    self.failure_count = 0
            
            return result
            
        except Exception as e:
            with self.lock:
                self.failure_count += 1
                self.last_failure_time = time.time()
                
                if self.state == CircuitState.HALF_OPEN:
                    logger.warning("Circuit Breaker: HALF_OPEN → OPEN (failure)")
                    self.state = CircuitState.OPEN
                elif self.failure_count >= self.failure_threshold:
                    logger.warning(f"Circuit Breaker: CLOSED → OPEN (threshold {self.failure_threshold} reached)")
                    self.state = CircuitState.OPEN
                else:
                    logger.debug(f"Circuit Breaker: failure {self.failure_count}/{self.failure_threshold}")
            
            raise

    def get_state(self) -> CircuitState:
        return self.state

    def reset(self):
        with self.lock:
            self.state = CircuitState.CLOSED
            self.failure_count = 0
            self.last_failure_time = None
            logger.info("Circuit Breaker: RESET")
