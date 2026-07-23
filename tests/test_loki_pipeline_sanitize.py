"""Tests for GrafanaSourceManager._sanitize_loki_pipeline.

Kimi routinely appends `| limit N` and `| sort_desc(...)` — PromQL/SQL-isms that are NOT valid LogQL
log-pipeline operators, so Loki's /api/ds/query returns a 400 parse error (masked previously as a
FINISHED "no logs" result). The sanitizer strips exactly those constructs and preserves the model's
intended line cap as max_lines. Real prod queries (account 41292) are used as fixtures: 13/15 of the
400-failing ones used these stages; 0/19 succeeding ones did — so stripping must fix the former and
leave the latter byte-identical.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'drdroid_debug_toolkit'))
from core.integrations.source_managers.grafana_source_manager import GrafanaSourceManager  # noqa: E402

san = GrafanaSourceManager._sanitize_loki_pipeline


# ---- real failing prod queries: must become valid + carry the limit into max_lines ----
def test_sort_desc_with_trailing_limit():
    q, ml = san('{namespace="core", pod=~"central-.*"} |= "error" | json | line_format "{{.time}} {{.level}} {{.msg}}" | sort_desc(time) limit 100', 1000)
    assert "sort_desc" not in q and "limit" not in q
    assert q == '{namespace="core", pod=~"central-.*"} |= "error" | json | line_format "{{.time}} {{.level}} {{.msg}}"'
    assert ml == 100


def test_sort_desc_json_limit():
    q, ml = san('{namespace="core", pod=~"central-.*"} |= "503" | json | sort_desc(time) limit 50', 1000)
    assert q == '{namespace="core", pod=~"central-.*"} |= "503" | json'
    assert ml == 50


def test_standalone_pipe_limit():
    q, ml = san('{namespace="core"} |= `x` | json | line_format "{{.message}}" | limit 20', 1000)
    assert "limit" not in q and ml == 20
    assert q == '{namespace="core"} |= `x` | json | line_format "{{.message}}"'


def test_trailing_pipe_removed():
    q, _ = san('{app="a"} | sort_desc(time)', 1000)
    assert q == '{app="a"}'          # sort stage was last -> no dangling pipe


# ---- succeeding prod queries: must be left byte-identical ----
def test_valid_queries_untouched():
    for q_in in [
        '{namespace="core", app="central"} |= "error"',
        '{namespace="core", pod=~"central.*"}',
        '{namespace="core", pod=~"sms-pubsub-services.*"} |= "ReplyError" | line_format "{{.message}}"',
        '{app="calling-voice-tools"} |~ "redis|Redis|connect|error|Error"',
    ]:
        q_out, ml = san(q_in, 1000)
        assert q_out == q_in and ml == 1000


# ---- safety: don't strip legitimate uses ----
def test_metric_sort_desc_not_stripped():
    # top-level metric function (no leading pipe) must survive
    q, _ = san('sort_desc(sum by (level) (count_over_time({app="a"}[5m])))', 1000)
    assert q == 'sort_desc(sum by (level) (count_over_time({app="a"}[5m])))'


def test_quoted_limit_word_not_touched():
    # the word "limit" inside a line filter (no leading pipe, no number-as-stage) stays
    q, ml = san('{app="a"} |= "rate limit exceeded"', 1000)
    assert q == '{app="a"} |= "rate limit exceeded"' and ml == 1000


def test_empty_and_none():
    assert san("", 1000) == ("", 1000)
    assert san(None, 500) == ("", 500)
