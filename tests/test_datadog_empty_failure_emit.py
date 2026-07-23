"""Datadog empty vs operational-failure emit contract.

Run: python -m pytest tests/test_datadog_empty_failure_emit.py -v
"""
import os
import sys
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "drdroid_debug_toolkit"))

# Source manager imports PrototypeClient which touches Django settings at import.
import django
from django.conf import settings

if not settings.configured:
    settings.configure(
        IS_PROD_ENV=False,
        SECRET_KEY="test",
        USE_TZ=True,
        INSTALLED_APPS=[],
    )

from core.integrations.utils.task_result_utils import FAILURE_KIND_EMPTY_QUERY
from core.integrations.source_managers.datadog_source_manager import (
    DatadogSourceManager,
    _is_datadog_not_found,
)
from core.protos.base_pb2 import Source
from core.protos.playbooks.playbook_commons_pb2 import PlaybookTaskResultType
from core.utils.proto_utils import proto_to_dict


def test_is_datadog_not_found_phrases():
    assert _is_datadog_not_found(Exception("ApiException (404)"))
    assert _is_datadog_not_found(Exception("Reason: Not found"))
    assert not _is_datadog_not_found(Exception("401 Unauthorized"))


def test_empty_helper_emits_empty_query_kind():
    mgr = DatadogSourceManager()
    result = mgr._empty_datadog_result("No logs returned from Datadog for query: x")
    assert result.type == PlaybookTaskResultType.API_RESPONSE
    body = proto_to_dict(result.api_response.response_body)
    assert body["failure_kind"] == FAILURE_KIND_EMPTY_QUERY
    assert result.source == Source.DATADOG


def test_finish_exception_not_found_is_empty():
    mgr = DatadogSourceManager()
    result = mgr._finish_datadog_task_exception(
        Exception("ApiException (404)\nReason: Not Found"),
        empty_message="No logs returned from Datadog for query: x",
    )
    body = proto_to_dict(result.api_response.response_body)
    assert body["failure_kind"] == FAILURE_KIND_EMPTY_QUERY


def test_finish_exception_operational_reraises():
    mgr = DatadogSourceManager()
    with pytest.raises(Exception, match="Error while executing Datadog task"):
        mgr._finish_datadog_task_exception(
            Exception("401 Unauthorized"),
            empty_message="should not use this",
        )


def test_query_monitors_exception_rebuilds_metadata():
    """API failures before metadata assignment must not UnboundLocalError."""
    mgr = DatadogSourceManager()
    dd_task = MagicMock()
    dd_task.query_monitors.query.value = "status:alert"
    dd_task.query_monitors.sort.value = None
    connector = MagicMock()

    with patch.object(mgr, "get_connector_processor") as get_proc, patch.object(
        mgr, "_extract_api_domain_from_connector", return_value="https://app.datadoghq.com"
    ), patch.object(
        mgr, "_create_metadata_with_datadog_url", return_value=MagicMock()
    ) as make_meta, patch.object(
        mgr, "_finish_datadog_task_exception", return_value=MagicMock(name="result")
    ) as finish:
        get_proc.return_value.query_monitors.side_effect = Exception("ApiException (404)")
        out = mgr.execute_query_monitors(MagicMock(), dd_task, connector)
        assert out is finish.return_value
        make_meta.assert_called()
        finish.assert_called_once()
        assert "No monitors found" in finish.call_args.kwargs["empty_message"]


def test_no_text_result_type_in_module_source():
    """Regression: empties must not ship as success TEXT."""
    path = os.path.join(
        os.path.dirname(__file__),
        "..",
        "drdroid_debug_toolkit",
        "core",
        "integrations",
        "source_managers",
        "datadog_source_manager.py",
    )
    src = open(path).read()
    assert "PlaybookTaskResultType.TEXT" not in src
    assert "build_task_empty_result" in src
