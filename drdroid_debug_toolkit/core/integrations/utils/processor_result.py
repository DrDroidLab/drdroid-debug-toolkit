"""
Standard return type for source API processors.

API processors should return ProcessorResult instead of raising for expected
operational failures (HTTP 4xx/5xx, empty required data, vendor error payloads).
Programmer errors may still raise.
"""
from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class ProcessorResult:
    success: bool
    data: Any = None
    error: Optional[str] = None
    status_code: int = 200
    metadata: dict = field(default_factory=dict)

    @classmethod
    def ok(cls, data: Any = None, *, status_code: int = 200, metadata: dict = None) -> "ProcessorResult":
        return cls(success=True, data=data, status_code=status_code, metadata=metadata or {})

    @classmethod
    def fail(
        cls,
        error: str,
        *,
        status_code: int = 500,
        data: Any = None,
        metadata: dict = None,
    ) -> "ProcessorResult":
        return cls(
            success=False,
            data=data,
            error=error,
            status_code=status_code,
            metadata=metadata or {},
        )

    @classmethod
    def from_exception(cls, exc: BaseException, *, status_code: int = 500) -> "ProcessorResult":
        return cls.fail(str(exc), status_code=status_code, metadata={"exception_type": type(exc).__name__})


def run_processor_call(callable_fn, *args, **kwargs) -> ProcessorResult:
    """Execute a processor callable; map exceptions to ProcessorResult.fail."""
    try:
        data = callable_fn(*args, **kwargs)
        return ProcessorResult.ok(data)
    except Exception as exc:
        return ProcessorResult.from_exception(exc)
