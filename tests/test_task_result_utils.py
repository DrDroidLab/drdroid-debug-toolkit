"""Unit tests for empty vs operational failure helpers.

Run: python -m pytest tests/test_task_result_utils.py -v
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "drdroid_debug_toolkit"))

from core.integrations.utils.processor_result import ProcessorResult, run_processor_call
from core.integrations.utils.task_result_utils import (
    FAILURE_KIND_EMPTY_QUERY,
    FAILURE_KIND_OPERATIONAL,
    build_task_empty_result,
    build_task_failure_result,
    playbook_result_from_processor_result,
    playbook_task_result_indicates_failure,
)
from core.protos.base_pb2 import Source
from core.protos.playbooks.playbook_commons_pb2 import PlaybookTaskResultType
from core.utils.proto_utils import proto_to_dict


def test_build_task_empty_result_sets_empty_query_kind():
    result = build_task_empty_result(Source.DATADOG, "No logs returned from Datadog for query: x")
    assert result.type == PlaybookTaskResultType.API_RESPONSE
    assert result.api_response.response_status.value == 404
    body = proto_to_dict(result.api_response.response_body)
    assert body["failure_kind"] == FAILURE_KIND_EMPTY_QUERY
    assert body["is_error"] is True
    assert "No logs returned" in body["error"]
    assert playbook_task_result_indicates_failure(result)


def test_build_task_failure_result_sets_operational_kind():
    result = build_task_failure_result(
        Source.DATADOG,
        "Unauthorized",
        response_status=401,
        exception_type="ApiException",
    )
    assert result.type == PlaybookTaskResultType.API_RESPONSE
    assert result.api_response.response_status.value == 401
    body = proto_to_dict(result.api_response.response_body)
    assert body["failure_kind"] == FAILURE_KIND_OPERATIONAL
    err = proto_to_dict(result.api_response.error)
    assert err["exception_type"] == "ApiException"
    assert playbook_task_result_indicates_failure(result)


def test_processor_result_ok_and_fail():
    assert ProcessorResult.ok({"a": 1}).success is True
    fail = ProcessorResult.fail("boom", status_code=503)
    assert fail.success is False
    assert fail.status_code == 503

    mapped = playbook_result_from_processor_result(Source.DATADOG, fail)
    body = proto_to_dict(mapped.api_response.response_body)
    assert body["failure_kind"] == FAILURE_KIND_OPERATIONAL


def test_run_processor_call_maps_exception():
    def boom():
        raise ValueError("nope")

    pr = run_processor_call(boom)
    assert pr.success is False
    assert "nope" in pr.error
    assert pr.metadata.get("exception_type") == "ValueError"
