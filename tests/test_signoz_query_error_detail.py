"""SigNoz query_range failures must carry a reason, not a bare status code.

Measured 2026-08-01 -> 2026-08-04: 15 signoz_clickhouse_query failures (all account 40720)
reached the agent as the literal string "HTTP 500" with no body. The processor did capture
the body in `raw_response`, but nothing downstream reads that field, so the reason was
dropped between the processor and the agent — undiagnosable from the aiops side and
impossible for the agent to correct.

Run: ./testing/bin/python -m pytest tests/test_signoz_query_error_detail.py -v
"""
import os
import sys
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "drdroid_debug_toolkit"))

import django
from django.conf import settings

if not settings.configured:
    settings.configure(IS_PROD_ENV=False, SECRET_KEY="test", USE_TZ=True, INSTALLED_APPS=[])

from core.integrations.source_api_processors.signoz_api_processor import SignozApiProcessor


def _processor():
    return SignozApiProcessor(signoz_api_url="https://signoz.example.com", signoz_api_token="k")


def _response(status, text):
    r = MagicMock()
    r.status_code = status
    r.text = text
    return r


@pytest.mark.parametrize("method", ["_post_query_range", "_post_query_range_v5"])
def test_error_includes_response_body(method):
    """The body is the only thing that says WHY the query failed."""
    body = '{"status":"error","errorType":"bad_data","error":"table default.signoz_index_v3 does not exist"}'
    with patch("core.integrations.source_api_processors.signoz_api_processor.requests.post",
               return_value=_response(500, body)):
        out = getattr(_processor(), method)({"q": 1})

    assert "HTTP 500" in out["error"], "status code must survive"
    assert "does not exist" in out["error"], "the actual reason must reach the caller"
    assert out["raw_response"] == body, "raw_response must still be populated"


@pytest.mark.parametrize("method", ["_post_query_range", "_post_query_range_v5"])
def test_error_is_not_bare_status_code(method):
    """Regression guard for the exact observed string."""
    with patch("core.integrations.source_api_processors.signoz_api_processor.requests.post",
               return_value=_response(500, "upstream clickhouse timeout")):
        out = getattr(_processor(), method)({"q": 1})

    assert out["error"] != "HTTP 500"
    assert "upstream clickhouse timeout" in out["error"]


@pytest.mark.parametrize("method", ["_post_query_range", "_post_query_range_v5"])
def test_empty_body_is_stated_explicitly(method):
    """An empty body is itself a finding — say so rather than emitting a naked code."""
    with patch("core.integrations.source_api_processors.signoz_api_processor.requests.post",
               return_value=_response(502, "")):
        out = getattr(_processor(), method)({"q": 1})

    assert "502" in out["error"]
    assert "empty response body" in out["error"]


@pytest.mark.parametrize("method", ["_post_query_range", "_post_query_range_v5"])
def test_long_body_is_truncated(method):
    """Whole HTML error pages must not be pasted into the agent's context."""
    with patch("core.integrations.source_api_processors.signoz_api_processor.requests.post",
               return_value=_response(500, "x" * 5000)):
        out = getattr(_processor(), method)({"q": 1})

    assert len(out["error"]) < 500


@pytest.mark.parametrize("method", ["_post_query_range", "_post_query_range_v5"])
def test_success_path_unchanged(method):
    r = MagicMock()
    r.status_code = 200
    r.json.return_value = {"data": {"result": []}}
    with patch("core.integrations.source_api_processors.signoz_api_processor.requests.post",
               return_value=r):
        out = getattr(_processor(), method)({"q": 1})

    assert out == {"data": {"result": []}}
    assert "error" not in out
