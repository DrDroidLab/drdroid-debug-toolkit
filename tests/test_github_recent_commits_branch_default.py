"""Tests for the GitHub recent-commits/merges empty-branch + severity fix.

Bug: the agent calls github_fetch_recent_commits/merges WITHOUT a branch, so
task.branch.value == "". The api processor sent `sha=` / `base=` (empty) to GitHub,
which returns zero commits — and the source manager then reported that empty result via
build_task_failure_result (operational_error / HTTP 500). Result: benign "no commits on
the default branch" surfaced as a server error (the 40720 github 500s).

Fixes locked in here:
  1. get_branch_commits / get_recent_merges OMIT the sha/base filter when branch is empty,
     so GitHub uses the repo's default branch.
  2. fetch_recent_commits reports an empty commit list as build_task_empty_result
     (empty_query_result / 404), not build_task_failure_result (operational_error / 500).
  3. A real fetch failure (processor returns None) STILL reports as an operational failure.

Run: python -m pytest tests/test_github_recent_commits_branch_default.py -v
"""
import os
import sys
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "drdroid_debug_toolkit"))

from core.integrations.source_api_processors.github_api_processor import GithubAPIProcessor  # noqa: E402
from core.integrations.source_managers.github_source_manager import GithubSourceManager  # noqa: E402
from core.integrations.utils.task_result_utils import (  # noqa: E402
    FAILURE_KIND_EMPTY_QUERY,
    FAILURE_KIND_OPERATIONAL,
)
from core.protos.base_pb2 import TimeRange  # noqa: E402
from core.protos.playbooks.playbook_commons_pb2 import PlaybookTaskResultType  # noqa: E402
from core.protos.playbooks.source_task_definitions.github_task_pb2 import Github  # noqa: E402
from core.utils.proto_utils import proto_to_dict  # noqa: E402
from google.protobuf.wrappers_pb2 import StringValue  # noqa: E402


def _mock_response(status=200, json_body=None):
    resp = MagicMock()
    resp.status_code = status
    resp.json.return_value = json_body if json_body is not None else []
    resp.__bool__ = lambda self: True
    return resp


# ---------- 1. branch defaulting in the API processor ----------

def test_get_branch_commits_omits_sha_when_branch_empty():
    proc = GithubAPIProcessor(api_key="x", org="acme")
    with patch("core.integrations.source_api_processors.github_api_processor.requests.request",
               return_value=_mock_response(200, [])) as req:
        proc.get_branch_commits("Alfred", branch="")  # agent omitted the branch
    url = req.call_args[0][1]
    # empty branch must NOT produce `sha=` (that yields zero commits); GitHub defaults instead
    assert "sha=" not in url, url
    assert "/repos/acme/Alfred/commits?" in url


def test_get_branch_commits_includes_sha_when_branch_given():
    proc = GithubAPIProcessor(api_key="x", org="acme")
    with patch("core.integrations.source_api_processors.github_api_processor.requests.request",
               return_value=_mock_response(200, [])) as req:
        proc.get_branch_commits("Alfred", branch="develop")
    url = req.call_args[0][1]
    assert "sha=develop" in url, url


def test_get_recent_merges_omits_base_when_branch_empty():
    proc = GithubAPIProcessor(api_key="x", org="acme")
    with patch("core.integrations.source_api_processors.github_api_processor.requests.request",
               return_value=_mock_response(200, [])) as req:
        proc.get_recent_merges("Alfred", branch="")
    url = req.call_args[0][1]
    assert "base=" not in url, url


def test_get_recent_merges_includes_base_when_branch_given():
    proc = GithubAPIProcessor(api_key="x", org="acme")
    with patch("core.integrations.source_api_processors.github_api_processor.requests.request",
               return_value=_mock_response(200, [])) as req:
        proc.get_recent_merges("Alfred", branch="develop")
    url = req.call_args[0][1]
    assert "base=develop" in url, url


# ---------- 2. severity: empty commits != operational failure ----------

def _commits_task(repo="Alfred", branch=""):
    return Github(
        type=Github.TaskType.FETCH_RECENT_COMMITS,
        fetch_recent_commits=Github.FetchRecentCommits(
            repo=StringValue(value=repo),
            branch=StringValue(value=branch),
        ),
    )


def test_no_commits_reports_empty_not_operational():
    mgr = GithubSourceManager()
    mgr.get_connector_processor = MagicMock(return_value=MagicMock(
        get_branch_commits=MagicMock(return_value=[])))  # query ran, zero commits
    result = mgr.fetch_recent_commits(TimeRange(), _commits_task(), MagicMock())
    body = proto_to_dict(result.api_response.response_body)
    assert body["failure_kind"] == FAILURE_KIND_EMPTY_QUERY, body
    assert result.api_response.response_status.value == 404


def test_real_fetch_failure_still_operational():
    mgr = GithubSourceManager()
    mgr.get_connector_processor = MagicMock(return_value=MagicMock(
        get_branch_commits=MagicMock(return_value=None)))  # repo/branch missing or auth error
    result = mgr.fetch_recent_commits(TimeRange(), _commits_task(), MagicMock())
    body = proto_to_dict(result.api_response.response_body)
    assert body["failure_kind"] == FAILURE_KIND_OPERATIONAL, body
    assert result.api_response.response_status.value == 500


def test_commits_present_returns_success_payload():
    mgr = GithubSourceManager()
    mgr.get_connector_processor = MagicMock(return_value=MagicMock(
        get_branch_commits=MagicMock(return_value=[{"sha": "abc", "commit": {"message": "fix"}}])))
    result = mgr.fetch_recent_commits(TimeRange(), _commits_task(), MagicMock())
    assert result.type == PlaybookTaskResultType.API_RESPONSE
    body = proto_to_dict(result.api_response.response_body)
    assert body.get("failure_kind") is None
    assert "recent_commits" in body
