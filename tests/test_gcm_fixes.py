"""Regression tests for two GCM tool bugs.

1. execute_mql() dropped the `interval` argument its caller passes -> every MQL call
   raised "takes 3 positional arguments but 4 were given" (100% failure in prod).
2. GCM log-filter timestamp parsing hard-coded microsecond precision, so whole-second
   timestamps ('2026-07-10T15:30:00Z') failed to parse.

Run: python -m pytest tests/test_gcm_fixes.py -v
"""
import inspect
import os
import sys
from unittest.mock import MagicMock, patch

import pytest

# Add the drdroid_debug_toolkit directory to path (same as test_backward_compatibility.py)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'drdroid_debug_toolkit'))

MOD = "core.integrations.source_api_processors.gcm_api_processor"


def _make_processor():
    """Build a GcmApiProcessor without touching real GCP auth."""
    with patch(f"{MOD}.get_gcm_credentials", return_value=MagicMock()):
        from core.integrations.source_api_processors.gcm_api_processor import GcmApiProcessor
        return GcmApiProcessor("proj-1", "{}")


def _fake_service(captured, response):
    """A fake monitoring service that records the MQL query body and returns `response`."""
    service = MagicMock()
    request = MagicMock()
    request.execute.return_value = response

    def _query(name, body):
        captured["query"] = body["query"]
        return request

    service.projects.return_value.timeSeries.return_value.query.side_effect = _query
    return service


# ── Bug 1: execute_mql signature / interval ────────────────────────────────
def test_execute_mql_signature_accepts_interval():
    from core.integrations.source_api_processors.gcm_api_processor import GcmApiProcessor
    params = list(inspect.signature(GcmApiProcessor.execute_mql).parameters.keys())
    assert "interval" in params  # the exact regression that caused 100% failure


def test_execute_mql_with_interval_does_not_raise_and_returns_data():
    proc = _make_processor()
    captured = {}
    svc = _fake_service(captured, {"timeSeriesData": [{"pointData": []}]})
    with patch(f"{MOD}.build", return_value=svc):
        out = proc.execute_mql("fetch gce_instance | align rate(10m)", "proj-1", interval="300")
    assert out == [{"pointData": []}]
    assert "every 300s" in captured["query"]  # interval injected as a sampling clause


def test_execute_mql_without_interval_leaves_query_untouched():
    proc = _make_processor()
    captured = {}
    svc = _fake_service(captured, {"timeSeriesData": []})
    with patch(f"{MOD}.build", return_value=svc):
        proc.execute_mql("fetch gce_instance", "proj-1")
    assert "every" not in captured["query"]


def test_execute_mql_interval_larger_than_align_returns_error():
    proc = _make_processor()
    with patch(f"{MOD}.build") as build_mock:
        out = proc.execute_mql("fetch x | align rate(60s)", "proj-1", interval="300")
    assert isinstance(out, str) and "cannot be larger than align rate" in out
    build_mock.assert_not_called()  # short-circuits before hitting the API


# ── Bug 2: tolerant timestamp parsing ──────────────────────────────────────
def _parse():
    from core.integrations.source_managers.gcm_source_manager import _parse_gcm_timestamp
    return _parse_gcm_timestamp


def test_parse_ts_whole_seconds_with_z():
    # the case that broke prod: no microseconds
    assert _parse()("2026-07-10T15:30:00Z").isoformat() == "2026-07-10T15:30:00+00:00"


def test_parse_ts_microseconds_with_z():
    dt = _parse()("2026-07-10T15:30:00.123456Z")
    assert dt.microsecond == 123456 and dt.tzinfo is not None


def test_parse_ts_without_z_is_utc_aware():
    dt = _parse()("2026-07-10T15:30:00")
    assert dt.tzinfo is not None


def test_parse_ts_invalid_raises():
    with pytest.raises(ValueError):
        _parse()("not-a-timestamp")
