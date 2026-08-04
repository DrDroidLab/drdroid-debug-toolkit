"""Tests for the Jenkins multibranch default-branch fix (mirrors the github default-branch fix).

Bug: fetch_last_build_details is almost always called WITHOUT a branch, and the source manager
never even forwarded the proto's branch_name field. For a multibranch pipeline job the api
processor then raised "Branch name must be specified..." — the single most common Jenkins call
became the dominant tool failure ("error while executing jenkins task: branch name ...").

Fixes locked in here:
  1. JenkinsAPIProcessor._default_multibranch_branch: prefer main/master, else the most recently
     built branch; None only when there are no branches at all.
  2. get_last_build defaults to that branch when none is supplied (no more hard failure).
  3. The source manager forwards fetch_last_build_details.branch_name so an explicitly requested
     branch is no longer silently dropped.

Run: python -m pytest tests/test_jenkins_default_branch.py -v
"""
import os
import sys
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'drdroid_debug_toolkit'))
from core.integrations.source_api_processors.jenkins_api_processor import JenkinsAPIProcessor  # noqa: E402
from core.integrations.source_managers.jenkins_source_manager import JenkinsSourceManager  # noqa: E402
from core.protos.playbooks.source_task_definitions.jenkins_task_pb2 import Jenkins  # noqa: E402
from google.protobuf.wrappers_pb2 import StringValue  # noqa: E402


# --------------------------------------------------------------------------------------
# 1. _default_multibranch_branch selection policy
# --------------------------------------------------------------------------------------
def test_default_prefers_main_over_others():
    subs = [{'name': 'feature-x', 'lastBuild': {'timestamp': 9999}},
            {'name': 'main', 'lastBuild': {'timestamp': 1}}]
    # main wins even though feature-x built more recently
    assert JenkinsAPIProcessor._default_multibranch_branch(subs) == 'main'


def test_default_prefers_master_when_no_main():
    subs = [{'name': 'develop', 'lastBuild': {'timestamp': 9999}},
            {'name': 'master', 'lastBuild': {'timestamp': 1}}]
    assert JenkinsAPIProcessor._default_multibranch_branch(subs) == 'master'


def test_default_falls_back_to_most_recent_build():
    subs = [{'name': 'develop', 'lastBuild': {'timestamp': 100}},
            {'name': 'release', 'lastBuild': {'timestamp': 500}},
            {'name': 'stale'}]  # no build at all -> timestamp 0
    assert JenkinsAPIProcessor._default_multibranch_branch(subs) == 'release'


def test_default_none_when_no_branches():
    assert JenkinsAPIProcessor._default_multibranch_branch([]) is None
    assert JenkinsAPIProcessor._default_multibranch_branch([{'lastBuild': {}}]) is None


# --------------------------------------------------------------------------------------
# 2. get_last_build no longer hard-fails a branch-less multibranch call
# --------------------------------------------------------------------------------------
def _proc():
    return JenkinsAPIProcessor(url="https://jenkins.example.com", username="u", api_token="t")


def _resp(status=200, payload=None):
    r = MagicMock()
    r.status_code = status
    r.json.return_value = payload or {}
    r.text = ""
    return r


def test_multibranch_without_branch_defaults_instead_of_raising():
    p = _proc()
    job_info = {'_class': 'org.jenkinsci.plugins.workflow.multibranch.WorkflowMultiBranchProject',
                'jobs': [{'name': 'main', 'lastBuild': {'timestamp': 5}},
                         {'name': 'dev', 'lastBuild': {'timestamp': 9}}]}
    build = {'number': 42, 'result': 'SUCCESS', 'timestamp': 5, 'actions': []}

    # first _make_request = job-type probe; second = the branch build fetch
    with patch.object(p, '_make_request', side_effect=[_resp(200, job_info), _resp(200, build)]):
        out = p.get_last_build("folder/my-multibranch")  # NO branch supplied

    # picked 'main' (preferred default) and returned a real build, not an exception
    assert out and out[0]['Branch'] == 'main'
    assert out[0]['Job Type'] == 'Multibranch Pipeline'


def test_multibranch_still_raises_when_no_branches_exist():
    p = _proc()
    job_info = {'_class': 'WorkflowMultiBranchProject', 'jobs': []}  # nothing to default to
    with patch.object(p, '_make_request', side_effect=[_resp(200, job_info)]):
        try:
            p.get_last_build("folder/empty-multibranch")
            assert False, "expected branch-required error when no branches exist"
        except Exception as e:
            assert "Branch name must be specified" in str(e)


def test_regular_job_unaffected():
    p = _proc()
    job_info = {'_class': 'hudson.model.FreeStyleProject'}  # not multibranch
    build = {'number': 7, 'result': 'FAILURE', 'timestamp': 1, 'actions': []}
    with patch.object(p, '_make_request', side_effect=[_resp(200, job_info), _resp(200, build)]):
        out = p.get_last_build("regular-job")
    assert out and out[0].get('Branch') is None  # regular jobs carry no Branch key


# --------------------------------------------------------------------------------------
# 3. source manager forwards the proto branch_name (was silently dropped before)
# --------------------------------------------------------------------------------------
def test_source_manager_forwards_branch_name():
    mgr = JenkinsSourceManager()
    fake_proc = MagicMock(get_last_build=MagicMock(return_value=[{'Branch': 'release', 'number': 1}]))
    mgr.get_connector_processor = MagicMock(return_value=fake_proc)
    task = Jenkins(type=Jenkins.TaskType.FETCH_LAST_BUILD_DETAILS,
                   fetch_last_build_details=Jenkins.LastBuildDetails(
                       job_name=StringValue(value="folder/mb"),
                       branch_name=StringValue(value="release")))
    mgr.fetch_last_build_details(None, task, MagicMock())
    # the branch the agent asked for must reach the processor
    _, kwargs = fake_proc.get_last_build.call_args
    assert kwargs.get('branch_name') == 'release'


def test_source_manager_passes_none_when_branch_absent():
    mgr = JenkinsSourceManager()
    fake_proc = MagicMock(get_last_build=MagicMock(return_value=[{'Branch': 'main', 'number': 1}]))
    mgr.get_connector_processor = MagicMock(return_value=fake_proc)
    task = Jenkins(type=Jenkins.TaskType.FETCH_LAST_BUILD_DETAILS,
                   fetch_last_build_details=Jenkins.LastBuildDetails(
                       job_name=StringValue(value="folder/mb")))  # no branch
    mgr.fetch_last_build_details(None, task, MagicMock())
    _, kwargs = fake_proc.get_last_build.call_args
    assert kwargs.get('branch_name') is None  # empty proto value -> None -> processor defaults
