"""Tests for Jenkins LIST_JOBS (task type 4).

Why this task exists: every other Jenkins task requires an exact job path, but nothing
exposed the available paths to the agent. In prod (account 39992) it guessed from service
names -- `wi-candidate-profile-ms`, then `gh-wi-candidate-profile-ms-wi-candidate-profile-ms`
-- and Jenkins answered 404 each time. LIST_JOBS returns the real paths.

Run: python -m pytest tests/test_jenkins_list_jobs.py -v
"""
import os
import sys
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'drdroid_debug_toolkit'))
from core.integrations.source_managers.jenkins_source_manager import JenkinsSourceManager  # noqa: E402
from core.protos.playbooks.source_task_definitions.jenkins_task_pb2 import Jenkins  # noqa: E402
from core.protos.playbooks.playbook_commons_pb2 import PlaybookTaskResultType  # noqa: E402
from google.protobuf.wrappers_pb2 import StringValue  # noqa: E402


JOBS_AND_FOLDERS = [
    {'name': 'team-a', 'class': 'com.cloudbees.hudson.plugins.folder.Folder', 'full_path': 'team-a'},
    {'name': 'api-service', 'class': 'hudson.model.FreeStyleProject', 'full_path': 'team-a/api-service'},
    {'name': 'wi-candidate-profile-ms',
     'class': 'org.jenkinsci.plugins.workflow.multibranch.WorkflowMultiBranchProject',
     'full_path': 'gh-wi-candidate-profile-ms/wi-candidate-profile-ms'},
]


def _manager_with_jobs(jobs):
    mgr = JenkinsSourceManager()
    mgr.get_connector_processor = MagicMock(return_value=MagicMock(
        get_all_jobs_recursive=MagicMock(return_value=jobs)))
    return mgr


def _rows_as_dicts(result):
    return [{c.name.value: c.value.value for c in row.columns} for row in result.table.rows]


# ---------------------------------------------------------------------------
# Dispatch wiring -- the thing whose absence caused "Task type N not supported"
# ---------------------------------------------------------------------------


def test_list_jobs_registered_in_dispatch_map():
    mgr = JenkinsSourceManager()
    assert Jenkins.TaskType.LIST_JOBS in mgr.task_type_callable_map
    entry = mgr.task_type_callable_map[Jenkins.TaskType.LIST_JOBS]
    assert entry['executor'] == mgr.list_jobs
    assert entry['result_type'] == PlaybookTaskResultType.TABLE


def test_every_declared_task_type_has_a_dispatch_entry():
    """Guards the whole class of bug: a proto task type with no executor."""
    mgr = JenkinsSourceManager()
    declared = {
        value for name, value in Jenkins.TaskType.items()
        if value != Jenkins.TaskType.UNKNOWN
    }
    missing = declared - set(mgr.task_type_callable_map)
    assert not missing, f"task types declared in proto but not dispatchable: {missing}"


def test_list_jobs_proto_roundtrip():
    task = Jenkins(type=Jenkins.TaskType.LIST_JOBS,
                   list_jobs=Jenkins.ListJobs(folder_path=StringValue(value="team-a")))
    assert task.WhichOneof('task') == 'list_jobs'
    assert task.list_jobs.folder_path.value == "team-a"


# ---------------------------------------------------------------------------
# Executor behavior
# ---------------------------------------------------------------------------


def test_list_jobs_returns_full_paths():
    """The full_path is what the other Jenkins tasks consume."""
    mgr = _manager_with_jobs(JOBS_AND_FOLDERS)
    task = Jenkins(type=Jenkins.TaskType.LIST_JOBS, list_jobs=Jenkins.ListJobs())

    result = mgr.list_jobs(None, task, MagicMock())

    assert result.type == PlaybookTaskResultType.TABLE
    paths = [r['full_path'] for r in _rows_as_dicts(result)]
    assert paths == ['team-a/api-service', 'gh-wi-candidate-profile-ms/wi-candidate-profile-ms']


def test_folders_are_excluded():
    """Folders aren't runnable and their children already appear as their own rows."""
    mgr = _manager_with_jobs(JOBS_AND_FOLDERS)
    task = Jenkins(type=Jenkins.TaskType.LIST_JOBS, list_jobs=Jenkins.ListJobs())

    result = mgr.list_jobs(None, task, MagicMock())

    names = [r['name'] for r in _rows_as_dicts(result)]
    assert 'team-a' not in names
    assert result.table.total_count.value == 2


def test_multibranch_jobs_are_flagged():
    """Lets the agent go straight to Get All Branches instead of learning via a failure."""
    mgr = _manager_with_jobs(JOBS_AND_FOLDERS)
    task = Jenkins(type=Jenkins.TaskType.LIST_JOBS, list_jobs=Jenkins.ListJobs())

    rows = {r['name']: r for r in _rows_as_dicts(mgr.list_jobs(None, task, MagicMock()))}

    assert rows['wi-candidate-profile-ms']['is_multibranch'] == 'True'
    assert rows['api-service']['is_multibranch'] == 'False'


def test_folder_path_is_passed_through_to_the_processor():
    mgr = _manager_with_jobs([])
    processor = mgr.get_connector_processor.return_value
    task = Jenkins(type=Jenkins.TaskType.LIST_JOBS,
                   list_jobs=Jenkins.ListJobs(folder_path=StringValue(value="team-a")))

    mgr.list_jobs(None, task, MagicMock())

    processor.get_all_jobs_recursive.assert_called_once_with("team-a")


def test_folder_path_is_optional_and_defaults_to_whole_instance():
    """Unlike the other tasks, LIST_JOBS must work with no arguments -- it is the
    entry point the agent reaches for when it knows nothing."""
    mgr = _manager_with_jobs(JOBS_AND_FOLDERS)
    processor = mgr.get_connector_processor.return_value
    task = Jenkins(type=Jenkins.TaskType.LIST_JOBS, list_jobs=Jenkins.ListJobs())

    result = mgr.list_jobs(None, task, MagicMock())

    processor.get_all_jobs_recursive.assert_called_once_with("")
    assert result.table.total_count.value == 2


# ---------------------------------------------------------------------------
# Edge and failure cases
# ---------------------------------------------------------------------------


def test_empty_instance_returns_empty_table_not_an_error():
    """A Jenkins with no jobs is a valid answer, not a failure."""
    mgr = _manager_with_jobs([])
    task = Jenkins(type=Jenkins.TaskType.LIST_JOBS, list_jobs=Jenkins.ListJobs())

    result = mgr.list_jobs(None, task, MagicMock())

    assert result.type == PlaybookTaskResultType.TABLE
    assert result.table.total_count.value == 0


def test_folder_only_instance_returns_empty_table():
    mgr = _manager_with_jobs([JOBS_AND_FOLDERS[0]])
    task = Jenkins(type=Jenkins.TaskType.LIST_JOBS, list_jobs=Jenkins.ListJobs())

    assert mgr.list_jobs(None, task, MagicMock()).table.total_count.value == 0


def test_missing_connector_raises():
    mgr = JenkinsSourceManager()
    task = Jenkins(type=Jenkins.TaskType.LIST_JOBS, list_jobs=Jenkins.ListJobs())

    with pytest.raises(Exception, match="No Jenkins source found"):
        mgr.list_jobs(None, task, None)


def test_processor_failure_is_surfaced_as_jenkins_task_error():
    mgr = JenkinsSourceManager()
    mgr.get_connector_processor = MagicMock(return_value=MagicMock(
        get_all_jobs_recursive=MagicMock(side_effect=Exception("connection refused"))))
    task = Jenkins(type=Jenkins.TaskType.LIST_JOBS, list_jobs=Jenkins.ListJobs())

    with pytest.raises(Exception, match="Error while executing Jenkins task"):
        mgr.list_jobs(None, task, MagicMock())


def test_entries_missing_class_are_treated_as_jobs():
    """The api processor returns '' for class when Jenkins omits _class; such an entry
    must still be listed rather than silently dropped."""
    mgr = _manager_with_jobs([{'name': 'legacy', 'class': '', 'full_path': 'legacy'}])
    task = Jenkins(type=Jenkins.TaskType.LIST_JOBS, list_jobs=Jenkins.ListJobs())

    rows = _rows_as_dicts(mgr.list_jobs(None, task, MagicMock()))

    assert len(rows) == 1
    assert rows[0]['full_path'] == 'legacy'
    assert rows[0]['is_multibranch'] == 'False'
