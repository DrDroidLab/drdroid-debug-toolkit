"""Inventory guard: source managers that on-prem converted must emit helpers.

Slack/documentation keep intentional TEXT on both trees (no helpers on-prem).
Argo/Gardener are on-prem-only product connectors (proto + managers) — out of
this emit-parity PR.

Run: python -m pytest tests/test_source_manager_emit_inventory.py -v
"""
from pathlib import Path

import pytest

MANAGERS_DIR = Path(__file__).resolve().parents[1] / "drdroid_debug_toolkit" / "core" / "integrations" / "source_managers"

# Max remaining PlaybookTaskResultType.TEXT occurrences after parity port.
# Non-zero values match intentional leftovers that also remain on on-prem.
TEXT_BUDGET = {
    "datadog_source_manager.py": 0,
    "grafana_source_manager.py": 0,
    "cloudwatch_source_manager.py": 3,
    "coralogix_source_manager.py": 6,
    "metabase_source_manager.py": 0,
    "jira_source_manager.py": 0,
    # 3 parity leftovers + 2 added deliberately: check_cluster_health and node_stats used to emit
    # their markdown report through build_task_failure_result, so a successful fetch surfaced as
    # HTTP 500. They are now success TEXT emits. Mirror on drdroid-on-prem when porting.
    "elastic_search_source_manager.py": 5,
    "newrelic_source_manager.py": 0,
    "sentry_source_manager.py": 0,
    "posthog_source_manager.py": 0,
    "signoz_source_manager.py": 3,
    "github_source_manager.py": 0,
    "open_search_source_manager.py": 1,
    "victoria_logs_source_manager.py": 0,
    "azure_source_manager.py": 0,
    "big_query_source_manager.py": 0,
    "grafana_loki_source_manager.py": 0,
    "mimir_source_manager.py": 0,
    "gcm_source_manager.py": 0,
}


@pytest.mark.parametrize("filename,max_text", sorted(TEXT_BUDGET.items()))
def test_manager_uses_helpers_and_respects_text_budget(filename, max_text):
    path = MANAGERS_DIR / filename
    assert path.exists(), f"missing {filename}"
    src = path.read_text()
    assert "build_task_empty_result" in src or "build_task_failure_result" in src, (
        f"{filename} must import/use task_result_utils helpers"
    )
    text_count = src.count("PlaybookTaskResultType.TEXT")
    assert text_count <= max_text, (
        f"{filename} has {text_count} TEXT result sites; budget is {max_text}"
    )
