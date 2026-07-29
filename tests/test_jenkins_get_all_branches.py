"""Tests for wiring Jenkins GET_ALL_BRANCHES (task type 3) into the source manager.

Bug: the proto (Jenkins.TaskType.GET_ALL_BRANCHES = 3) and the api processor
(JenkinsAPIProcessor.get_all_branches) both existed, but the source manager's
task_type_callable_map never registered it — so `jenkins_get_all_branches` failed with
"PlaybookSourceManager.get_source_task:: Task type 3 not supported for source: jenkins".
That dead-ended the multibranch flow (fetch_last_build_details tells the agent to "Use Get
All Branches", which then errored). These lock in the dispatch entry + executor behavior.

Run: python -m pytest tests/test_jenkins_get_all_branches.py -v
"""
import os
import sys
from unittest.mock import MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'drdroid_debug_toolkit'))
from core.integrations.source_managers.jenkins_source_manager import JenkinsSourceManager  # noqa: E402
from core.protos.playbooks.source_task_definitions.jenkins_task_pb2 import Jenkins  # noqa: E402
from core.protos.playbooks.playbook_commons_pb2 import PlaybookTaskResultType  # noqa: E402
from google.protobuf.wrappers_pb2 import StringValue  # noqa: E402


def test_get_all_branches_registered_in_dispatch_map():
    mgr = JenkinsSourceManager()
    assert Jenkins.TaskType.GET_ALL_BRANCHES in mgr.task_type_callable_map
    entry = mgr.task_type_callable_map[Jenkins.TaskType.GET_ALL_BRANCHES]
    assert entry['executor'] == mgr.get_all_branches
    assert entry['result_type'] == PlaybookTaskResultType.TABLE


def test_get_all_branches_builds_table_from_processor():
    mgr = JenkinsSourceManager()
    # mock the api processor so no real Jenkins call happens
    mgr.get_connector_processor = MagicMock(return_value=MagicMock(
        get_all_branches=MagicMock(return_value=[
            {'Branch Name': 'main', 'Has Builds': 'Yes', 'Last Build Status': 'SUCCESS'},
            {'Branch Name': 'develop', 'Has Builds': 'No', 'Last Build Status': 'N/A'},
        ])))
    task = Jenkins(type=Jenkins.TaskType.GET_ALL_BRANCHES,
                   get_all_branches=Jenkins.GetAllBranches(job_name=StringValue(value="gh-org-repo")))
    connector = MagicMock()
    result = mgr.get_all_branches(None, task, connector)
    assert result.type == PlaybookTaskResultType.TABLE
    assert result.table.total_count.value == 2
    # first row carries the branch name
    first_row_vals = {c.name.value: c.value.value for c in result.table.rows[0].columns}
    assert first_row_vals['Branch Name'] == 'main'


def test_get_all_branches_requires_job_name():
    mgr = JenkinsSourceManager()
    task = Jenkins(type=Jenkins.TaskType.GET_ALL_BRANCHES,
                   get_all_branches=Jenkins.GetAllBranches())  # no job_name
    try:
        mgr.get_all_branches(None, task, MagicMock())
        assert False, "expected job-name-required error"
    except Exception as e:
        assert "Job Name is required" in str(e)
