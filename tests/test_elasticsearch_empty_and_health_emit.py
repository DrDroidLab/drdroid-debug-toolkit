"""Elasticsearch emit contract: no-data is EMPTY, and a successful health/stats fetch is SUCCESS.

Measured across the cloud platform 2026-08-01 -> 2026-08-04 (6,444 tool calls):

  74  elasticsearch_query_logs returned "Index 'X' not found or no data returned ..." as a HARD
      FAILURE. Those queries ran fine and matched nothing. It was the single largest tool-failure
      class on the platform, and the wording conflated "index missing" with "index empty", so the
      agent could not tell them apart and retried against other indices.

  15  elasticsearch_check_cluster_health / elasticsearch_node_stats returned HTTP 500 +
      operational_error wrapping a fully SUCCESSFUL report (yellow cluster; _nodes.failed == 0).
      The report text was placed in the error field, so the agent discarded data it already had.

The consumer side already treats failure_kind == empty_query_result as no_data, so stamping the
right kind here is what makes the distinction real end to end.

Run: ./testing/bin/python -m pytest tests/test_elasticsearch_empty_and_health_emit.py -v
"""
import os
import re
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "drdroid_debug_toolkit"))

import django
from django.conf import settings

if not settings.configured:
    settings.configure(IS_PROD_ENV=False, SECRET_KEY="test", USE_TZ=True, INSTALLED_APPS=[])

from core.integrations.utils.task_result_utils import (
    FAILURE_KIND_EMPTY_QUERY,
    FAILURE_KIND_OPERATIONAL,
    build_task_empty_result,
    playbook_task_result_indicates_failure,
)
from core.protos.base_pb2 import Source
from core.protos.playbooks.playbook_commons_pb2 import PlaybookTaskResultType
from core.utils.proto_utils import proto_to_dict

SOURCE_FILE = os.path.join(
    os.path.dirname(__file__), "..", "drdroid_debug_toolkit", "core", "integrations",
    "source_managers", "elastic_search_source_manager.py",
)


def _source():
    with open(SOURCE_FILE) as fh:
        return fh.read()


def _body_after(marker, span=1400):
    """Return the source slice following a marker, comments stripped.

    Comments are removed because the explanatory notes on these fixes name the very helper
    the assertions check for absence of; matching them would be a false positive."""
    s = _source()
    i = s.index(marker)
    chunk = s[i:i + span]
    return "\n".join(re.sub(r"#.*$", "", line) for line in chunk.splitlines())


# ---------------------------------------------------------------------------
# no-data must be EMPTY, not a hard failure
# ---------------------------------------------------------------------------


def test_zero_hits_emits_empty_not_failure():
    """The 74-failure case: hits envelope present, total == 0 -> index responded, matched nothing."""
    body = _body_after("if 'hits' not in result or not result['hits']['hits']:")
    assert "build_task_empty_result" in body
    assert "build_task_failure_result" not in body, "zero-hit paths must not emit a hard failure"


def test_zero_hits_message_no_longer_claims_the_index_is_missing():
    """Old wording forced the agent to guess between 'missing index' and 'no match'."""
    body = _body_after("if 'hits' not in result or not result['hits']['hits']:")
    assert "not found or no data returned" not in body
    assert "No documents matched query" in body


@pytest.mark.parametrize(
    "message",
    [
        "No transaction names found for service: {service_name}",
        "No traces found for service: {service_name}",
        "No traces found for transaction: {transaction_name}",
        "No thread pool search data found",
        "Not enough monitoring data to plot",
    ],
)
def test_other_nothing_matched_paths_emit_empty(message):
    """Every 'nothing matched' path must carry the empty kind so it is counted as no_data."""
    s = _source()
    assert f'build_task_failure_result(self.source, f"{message}")' not in s
    assert f'build_task_failure_result(self.source, "{message}")' not in s
    assert message in s, "the message itself should still exist, just emitted as empty"


def test_empty_result_helper_stamps_the_kind_the_consumer_reads():
    """Guards the contract the aiops-side classifier depends on."""
    result = build_task_empty_result(Source.ELASTIC_SEARCH, "No documents matched query: x on index: y")
    body = proto_to_dict(result.api_response.response_body)
    assert body["failure_kind"] == FAILURE_KIND_EMPTY_QUERY
    assert body["failure_kind"] != FAILURE_KIND_OPERATIONAL


# ---------------------------------------------------------------------------
# health / node stats: a successful fetch is a SUCCESS
# ---------------------------------------------------------------------------


def test_cluster_health_emits_success_not_failure():
    body = _body_after("### Full Cluster Health Data:")
    assert "PlaybookTaskResultType.TEXT" in body
    assert "build_task_failure_result" not in body, (
        "a yellow/red cluster is the ANSWER; only an exception is a task failure"
    )


def test_node_stats_emits_success_not_failure():
    body = _body_after("### Full Node Stats Data:")
    assert "PlaybookTaskResultType.TEXT" in body
    assert "build_task_failure_result" not in body


def test_health_and_stats_still_raise_on_real_errors():
    """Regression guard: the except branches must keep raising, so genuine breakage is not
    silently reported as a healthy cluster."""
    s = _source()
    assert 'raise Exception(f"Error while checking ElasticSearch cluster health: {e}")' in s
    assert 'raise Exception(f"Error while fetching ElasticSearch node stats: {e}")' in s


# ---------------------------------------------------------------------------
# declared result_type must match what the executor actually emits
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("task_type", ["NODE_STATS", "CHECK_CLUSTER_HEALTH"])
def test_markdown_report_tasks_declare_text_result_type(task_type):
    """The declared result_type is the contract consumers render against. NODE_STATS emitted a
    TextResult while declaring LOGS, so consumers were told to expect the wrong shape."""
    s = _source()
    i = s.index(f"ElasticSearchProto.TaskType.{task_type}: {{")
    block = s[i:i + 400]
    assert "'result_type': PlaybookTaskResultType.TEXT" in block


def test_partial_node_stats_response_is_flagged_in_the_report():
    """_nodes.failed > 0 means the stats cover only the nodes that answered. The data is still
    real, so it is surfaced with a warning rather than discarded as a task failure."""
    body = _body_after("def execute_node_stats", span=1800)
    assert "_nodes" in body and "failed" in body
    assert "Partial response" in body
    assert "build_task_failure_result" not in body, "partial data must not be thrown away"
