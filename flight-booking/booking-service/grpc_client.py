import grpc
import time
import logging
import os
from typing import Optional
from circuit_breaker import CircuitBreaker

logger = logging.getLogger(__name__)

class RetryConfig:
    def __init__(self):
        self.max_attempts = int(os.getenv("RETRY_MAX_ATTEMPTS", "3"))
        self.initial_backoff_ms = int(os.getenv("RETRY_INITIAL_BACKOFF_MS", "100"))

class GrpcClient:
    RETRYABLE_CODES = {
        grpc.StatusCode.UNAVAILABLE,
        grpc.StatusCode.DEADLINE_EXCEEDED,
    }
    
    NON_RETRYABLE_CODES = {
        grpc.StatusCode.INVALID_ARGUMENT,
        grpc.StatusCode.NOT_FOUND,
        grpc.StatusCode.RESOURCE_EXHAUSTED,
        grpc.StatusCode.UNAUTHENTICATED,
    }

    def __init__(self, channel, api_key: str):
        self.channel = channel
        self.api_key = api_key
        self.retry_config = RetryConfig()
        
        cb_threshold = int(os.getenv("CB_FAILURE_THRESHOLD", "5"))
        cb_timeout = int(os.getenv("CB_TIMEOUT_SECONDS", "30"))
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=cb_threshold,
            timeout_seconds=cb_timeout
        )

    def _create_metadata(self):
        return [('api-key', self.api_key)]

    def _should_retry(self, error: grpc.RpcError) -> bool:
        if isinstance(error, grpc.Call):
            code = error.code()
            return code in self.RETRYABLE_CODES
        return False

    def _exponential_backoff(self, attempt: int) -> float:
        return (self.retry_config.initial_backoff_ms * (2 ** attempt)) / 1000.0

    def call_with_retry(self, func, *args, **kwargs):
        def wrapped_call():
            last_error = None
            
            for attempt in range(self.retry_config.max_attempts):
                try:
                    metadata = self._create_metadata()
                    result = func(*args, metadata=metadata, **kwargs)
                    
                    if attempt > 0:
                        logger.info(f"Retry succeeded on attempt {attempt + 1}")
                    
                    return result
                    
                except grpc.RpcError as e:
                    last_error = e
                    code = e.code()
                    
                    if code in self.NON_RETRYABLE_CODES:
                        logger.warning(f"Non-retryable error: {code} - {e.details()}")
                        raise
                    
                    if self._should_retry(e) and attempt < self.retry_config.max_attempts - 1:
                        backoff = self._exponential_backoff(attempt)
                        logger.warning(
                            f"Attempt {attempt + 1} failed with {code}. "
                            f"Retrying in {backoff:.3f}s..."
                        )
                        time.sleep(backoff)
                    else:
                        logger.error(f"All retry attempts exhausted or non-retryable error")
                        raise
            
            raise last_error
        
        return self.circuit_breaker.call(wrapped_call)

    def get_circuit_state(self):
        return self.circuit_breaker.get_state()
