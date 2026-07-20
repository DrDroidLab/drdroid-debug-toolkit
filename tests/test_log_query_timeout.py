"""Log-query calls must use the 60s LOG_QUERY_TIMEOUT budget.

Logs are slower than metric/config calls and were timing out early: SigNoz log
fetches used a hardcoded 30s (cloud), and on the VPC-agent path the prototype
proxy-poll cap (30s) clipped them. We introduced core.settings.LOG_QUERY_TIMEOUT
and route the log processors through it. These lock the value + wiring in.

Run: python -m pytest tests/test_log_query_timeout.py -v
"""
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "drdroid_debug_toolkit"))

from core import settings  # noqa: E402
from core.integrations.source_api_processors.grafana_loki_api_processor import GrafanaLokiApiProcessor  # noqa: E402
from core.integrations.source_api_processors.victoria_logs_api_processor import VictoriaLogsApiProcessor  # noqa: E402

PROCESSORS = Path(__file__).resolve().parents[1] / "drdroid_debug_toolkit" / "core" / "integrations" / "source_api_processors"


def _ok_response():
    r = MagicMock()
    r.status_code = 200
    r.json.return_value = {"data": {"result": []}}
    r.text = "{}"
    r.__bool__ = lambda self: True
    return r


def test_log_query_timeout_value_is_60():
    assert settings.LOG_QUERY_TIMEOUT == 60


def test_loki_query_uses_log_query_timeout():
    proc = GrafanaLokiApiProcessor(host="h", port=3100, protocol="http")
    with patch("core.integrations.source_api_processors.grafana_loki_api_processor.requests.get",
               return_value=_ok_response()) as g:
        proc.query("{app=\"x\"}", start=1, end=2)
    assert g.call_args.kwargs["timeout"] == settings.LOG_QUERY_TIMEOUT == 60


def test_victoria_http_get_uses_log_query_timeout():
    proc = VictoriaLogsApiProcessor(VICTORIA_LOGS_HOST="h")
    with patch("core.integrations.source_api_processors.victoria_logs_api_processor.requests.get",
               return_value=_ok_response()) as g:
        proc._http_get("/select/logsql/query")
    assert g.call_args.kwargs["timeout"] == 60


def test_victoria_post_uses_log_query_timeout():
    proc = VictoriaLogsApiProcessor(VICTORIA_LOGS_HOST="h")
    with patch("core.integrations.source_api_processors.victoria_logs_api_processor.requests.post",
               return_value=_ok_response()) as p:
        proc._http_post_form("/select/logsql/query", {"query": "* | limit 1"})
    assert p.call_args.kwargs["timeout"] == 60


def test_signoz_v5_log_helper_wired_to_constant():
    # SignozApiProcessor construction pulls in heavier config; assert the log-path helper
    # (_post_query_range_v5, used by fetch_logs_with_filter) references the constant, not a literal.
    src = (PROCESSORS / "signoz_api_processor.py").read_text()
    v5 = src[src.index("def _post_query_range_v5"): src.index("def _post_query_range_v5") + 700]
    assert "timeout=LOG_QUERY_TIMEOUT" in v5, "signoz v5 log helper must use LOG_QUERY_TIMEOUT"
    assert "timeout=30" not in v5, "signoz v5 log helper still has a hardcoded 30s"
