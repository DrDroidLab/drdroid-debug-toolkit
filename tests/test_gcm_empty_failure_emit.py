"""GCM empty/failure emit contract (keeps cloud timestamp / filter helpers).

Run: python -m pytest tests/test_gcm_empty_failure_emit.py -v
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "drdroid_debug_toolkit"))

import django
from django.conf import settings

if not settings.configured:
    settings.configure(IS_PROD_ENV=False, SECRET_KEY="test", USE_TZ=True, INSTALLED_APPS=[])

from core.integrations.source_managers.gcm_source_manager import (
    GcmSourceManager,
    _parse_gcm_timestamp,
    _strip_orphan_bool_ops,
)
from core.integrations.utils.task_result_utils import (
    FAILURE_KIND_EMPTY_QUERY,
    FAILURE_KIND_OPERATIONAL,
)
from core.protos.playbooks.playbook_commons_pb2 import PlaybookTaskResultType
from core.utils.proto_utils import proto_to_dict


def test_parse_gcm_timestamp_whole_seconds():
    dt = _parse_gcm_timestamp("2026-07-10T15:30:00Z")
    assert dt.year == 2026 and dt.hour == 15


def test_strip_orphan_bool_ops():
    assert _strip_orphan_bool_ops('resource.type="gce" AND') == 'resource.type="gce"'
    assert "AND AND" not in _strip_orphan_bool_ops("a AND AND b")


def test_gcm_empty_helper():
    mgr = GcmSourceManager()
    # build via public helper path: empty_query for logs-style message
    from core.integrations.utils.task_result_utils import build_task_empty_result

    result = build_task_empty_result(mgr.source, "No data returned from GCM Logs for query: x")
    assert result.type == PlaybookTaskResultType.API_RESPONSE
    assert proto_to_dict(result.api_response.response_body)["failure_kind"] == FAILURE_KIND_EMPTY_QUERY


def test_gcm_failure_helper_kind():
    from core.integrations.utils.task_result_utils import build_task_failure_result

    mgr = GcmSourceManager()
    result = build_task_failure_result(mgr.source, "boom")
    assert proto_to_dict(result.api_response.response_body)["failure_kind"] == FAILURE_KIND_OPERATIONAL


def test_gcm_source_has_no_text_emit():
    path = os.path.join(
        os.path.dirname(__file__),
        "..",
        "drdroid_debug_toolkit",
        "core",
        "integrations",
        "source_managers",
        "gcm_source_manager.py",
    )
    src = open(path).read()
    assert "PlaybookTaskResultType.TEXT" not in src
    assert "_parse_gcm_timestamp" in src
    assert "_strip_orphan_bool_ops" in src
    assert "build_task_empty_result" in src
