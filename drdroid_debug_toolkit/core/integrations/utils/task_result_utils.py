"""
Helpers for building PlaybookTaskResult values that downstream agent/UI code
recognizes as failures (not successful TEXT responses).

Zero-output vs operational error:
- ``build_task_empty_result`` — valid query, no rows/points/logs in range (404, failure_kind=empty_query_result)
- ``build_task_failure_result`` — API/validation/unexpected failures (5xx/4xx, failure_kind=operational_error)
"""
from typing import Any, Callable, Dict, Optional, TypeVar

from google.protobuf.struct_pb2 import Struct
from google.protobuf.wrappers_pb2 import StringValue, UInt64Value

from core.integrations.utils.processor_result import ProcessorResult, run_processor_call
from core.protos.base_pb2 import Source
from core.utils.proto_utils import proto_to_dict
from core.protos.playbooks.playbook_commons_pb2 import (
    ApiResponseResult,
    PlaybookTaskResult,
    PlaybookTaskResultType,
)

T = TypeVar("T")

FAILURE_KIND_OPERATIONAL = "operational_error"
FAILURE_KIND_EMPTY_QUERY = "empty_query_result"


def _build_task_api_failure(
    source: Source,
    error_message: str,
    *,
    failure_kind: str,
    request_method: str = "EXECUTE",
    request_url: str = "",
    response_status: int = 500,
    metadata: Optional[Dict[str, Any]] = None,
    task_metadata: Optional[Struct] = None,
    exception_type: Optional[str] = None,
) -> PlaybookTaskResult:
    response_body = Struct()
    response_body.update({
        "is_error": True,
        "error": error_message,
        "failure_kind": failure_kind,
    })

    error_struct = Struct()
    error_payload: Dict[str, Any] = {"message": error_message, "failure_kind": failure_kind}
    if exception_type:
        error_payload["exception_type"] = exception_type
    error_struct.update(error_payload)

    api_response = ApiResponseResult(
        request_method=StringValue(value=request_method),
        request_url=StringValue(value=request_url or "/task/execute"),
        response_status=UInt64Value(value=response_status),
        response_body=response_body,
        error=error_struct,
    )

    if metadata:
        metadata_struct = Struct()
        metadata_struct.update(metadata)
        api_response.metadata.CopyFrom(metadata_struct)

    result = PlaybookTaskResult(
        type=PlaybookTaskResultType.API_RESPONSE,
        api_response=api_response,
        source=source,
    )
    if task_metadata is not None:
        result.metadata.CopyFrom(task_metadata)
    return result


def build_task_failure_result(
    source: Source,
    error_message: str,
    *,
    request_method: str = "EXECUTE",
    request_url: str = "",
    response_status: int = 500,
    metadata: Optional[Dict[str, Any]] = None,
    task_metadata: Optional[Struct] = None,
    exception_type: Optional[str] = None,
) -> PlaybookTaskResult:
    """
    Operational failure: HTTP errors, validation, unexpected response shape, raised exceptions.
    """
    return _build_task_api_failure(
        source,
        error_message,
        failure_kind=FAILURE_KIND_OPERATIONAL,
        request_method=request_method,
        request_url=request_url,
        response_status=response_status,
        metadata=metadata,
        task_metadata=task_metadata,
        exception_type=exception_type,
    )


def build_task_empty_result(
    source: Source,
    message: str,
    *,
    request_method: str = "EXECUTE",
    request_url: str = "",
    response_status: int = 404,
    metadata: Optional[Dict[str, Any]] = None,
    task_metadata: Optional[Struct] = None,
) -> PlaybookTaskResult:
    """
    Zero-output for logs/metrics/dashboard widgets: query ran but returned no data.

    Surfaces as a failed tool call in the agent UI (not success TEXT) while
    ``failure_kind=empty_query_result`` lets consumers distinguish empty vs fault.
    """
    return _build_task_api_failure(
        source,
        message,
        failure_kind=FAILURE_KIND_EMPTY_QUERY,
        request_method=request_method,
        request_url=request_url,
        response_status=response_status,
        metadata=metadata,
        task_metadata=task_metadata,
    )


def playbook_task_result_indicates_failure(result: PlaybookTaskResult) -> bool:
    """True when a PlaybookTaskResult should be treated as failed (agent UI + bulk playbooks)."""
    if result.HasField("error") and result.error.value:
        return True

    if result.type != PlaybookTaskResultType.API_RESPONSE or not result.HasField("api_response"):
        return False

    api = result.api_response
    if api.HasField("error") and api.error.fields:
        return True

    if api.HasField("response_status") and api.response_status.value >= 400:
        return True

    if api.HasField("response_body"):
        body = proto_to_dict(api.response_body) or {}
        if body.get("is_error") in (True, "true", "True", "1", 1):
            return True
        if body.get("error"):
            return True

    return False


def extract_playbook_task_result_error_message(result: PlaybookTaskResult) -> Optional[str]:
    if result.HasField("error") and result.error.value:
        return result.error.value

    if result.type != PlaybookTaskResultType.API_RESPONSE or not result.HasField("api_response"):
        return None

    api = result.api_response
    if api.HasField("error") and api.error.fields:
        msg = api.error.fields.get("message")
        if msg and hasattr(msg, "string_value") and msg.string_value:
            return msg.string_value

    if api.HasField("response_body"):
        body = proto_to_dict(api.response_body) or {}
        err = body.get("error")
        if isinstance(err, str) and err.strip():
            return err.strip()

    if api.HasField("response_status") and api.response_status.value >= 400:
        return f"HTTP {api.response_status.value}"

    return None


def playbook_result_from_processor_result(
    source: Source,
    processor_result: ProcessorResult,
    *,
    request_method: str = "EXECUTE",
    request_url: str = "",
    task_metadata: Optional[Struct] = None,
    success_result_builder: Optional[Callable[[Any], PlaybookTaskResult]] = None,
) -> PlaybookTaskResult:
    """
    Map ProcessorResult → PlaybookTaskResult.

    On failure, returns build_task_failure_result. On success, either uses
    success_result_builder(data) or a default API_RESPONSE with data in response_body.
    """
    if not processor_result.success:
        return build_task_failure_result(
            source,
            processor_result.error or "Tool execution failed",
            request_method=request_method,
            request_url=request_url,
            response_status=processor_result.status_code or 500,
            metadata=processor_result.metadata or None,
            task_metadata=task_metadata,
            exception_type=(processor_result.metadata or {}).get("exception_type"),
        )

    if success_result_builder is not None:
        result = success_result_builder(processor_result.data)
        if task_metadata is not None:
            result.metadata.CopyFrom(task_metadata)
        return result

    response_body = Struct()
    if isinstance(processor_result.data, dict):
        response_body.update(processor_result.data)
    elif processor_result.data is not None:
        response_body.update({"result": processor_result.data})

    api_response = ApiResponseResult(
        request_method=StringValue(value=request_method),
        request_url=StringValue(value=request_url or "/task/execute"),
        response_status=UInt64Value(value=processor_result.status_code or 200),
        response_body=response_body,
    )
    result = PlaybookTaskResult(
        type=PlaybookTaskResultType.API_RESPONSE,
        api_response=api_response,
        source=source,
    )
    if task_metadata is not None:
        result.metadata.CopyFrom(task_metadata)
    return result


def call_processor(
    source: Source,
    fn: Callable[..., T],
    *args,
    success_result_builder: Optional[Callable[[Any], PlaybookTaskResult]] = None,
    request_method: str = "EXECUTE",
    request_url: str = "",
    task_metadata: Optional[Struct] = None,
    **kwargs,
) -> PlaybookTaskResult:
    """Run processor method via ProcessorResult and return PlaybookTaskResult."""
    pr = run_processor_call(fn, *args, **kwargs)
    return playbook_result_from_processor_result(
        source,
        pr,
        request_method=request_method,
        request_url=request_url,
        task_metadata=task_metadata,
        success_result_builder=success_result_builder,
    )
