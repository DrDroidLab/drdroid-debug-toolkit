"""Tests for the Jenkins metadata extractor's asset persistence.

Two production defects, both confirmed against the prod DB before fixing:

1. `super().__init__(..., Source.ARGOCD)` -- the active base class persists rows with
   `connector_type=self.source`, so every Jenkins job was filed under ArgoCD. In prod,
   all 537 of account 40720's jobs sat on connector_type=65 while other accounts used 62.

2. Folders were persisted as jobs. The `if not is_folder:` branch built a local
   `metadata` dict that was never used, so the guard had no effect and folder rows
   (e.g. "Prod-EKS" / com.cloudbees.hudson.plugins.folder.Folder) reached the DB and
   were indexed as runnable build targets.

Run: python -m pytest tests/test_jenkins_metadata_extractor.py -v
"""
import os
import sys
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'drdroid_debug_toolkit'))
from core.protos.base_pb2 import Source, SourceModelType  # noqa: E402


JOBS_AND_FOLDERS = [
    {'name': 'Prod-EKS', 'class': 'com.cloudbees.hudson.plugins.folder.Folder', 'full_path': 'Prod-EKS'},
    {'name': 'api-service', 'class': 'hudson.model.FreeStyleProject', 'full_path': 'Prod-EKS/api-service'},
    {'name': 'wi-candidate-events-ms',
     'class': 'org.jenkinsci.plugins.workflow.multibranch.WorkflowMultiBranchProject',
     'full_path': 'gh-wi-candidateevents-ms-wi-candidate-events-ms'},
]


@pytest.fixture
def extractor():
    """Build the extractor with its API processor mocked out."""
    with patch('core.integrations.source_metadata_extractors.jenkins_metadata_extractor.JenkinsAPIProcessor') as proc:
        from core.integrations.source_metadata_extractors.jenkins_metadata_extractor import (
            JenkinsSourceMetadataExtractor,
        )
        instance = proc.return_value
        instance.test_connection.return_value = True
        instance.get_all_jobs_recursive.return_value = JOBS_AND_FOLDERS
        instance.get_job_parameters.return_value = []

        ex = JenkinsSourceMetadataExtractor('req-1', 'K8_Jenkins', 'http://jenkins', 'user', 'token')
        ex.create_or_update_model_metadata = MagicMock()
        yield ex


def _saved_models(extractor):
    """The {model_uid: metadata} dict handed to the persistence layer."""
    assert extractor.create_or_update_model_metadata.called, "nothing was persisted"
    model_type, model_data = extractor.create_or_update_model_metadata.call_args[0]
    assert model_type == SourceModelType.JENKINS_JOBS
    return model_data


# ---------------------------------------------------------------------------
# Defect 1: wrong Source
# ---------------------------------------------------------------------------


def test_extractor_registers_itself_as_jenkins(extractor):
    """connector_type is derived from this; ARGOCD mis-filed every Jenkins job."""
    assert extractor.source == Source.JENKINS
    assert extractor.source != Source.ARGOCD


# ---------------------------------------------------------------------------
# Defect 2: folders persisted as jobs
# ---------------------------------------------------------------------------


def test_folders_are_not_persisted(extractor):
    extractor.extract_jobs()
    saved = _saved_models(extractor)

    assert 'Prod-EKS' not in saved
    assert set(saved) == {'Prod-EKS/api-service', 'gh-wi-candidateevents-ms-wi-candidate-events-ms'}


def test_jobs_inside_a_folder_are_still_persisted(extractor):
    """Dropping the folder must not drop its children."""
    extractor.extract_jobs()
    assert 'Prod-EKS/api-service' in _saved_models(extractor)


def test_folder_only_instance_persists_nothing(extractor):
    extractor.jenkins_processor.get_all_jobs_recursive.return_value = [JOBS_AND_FOLDERS[0]]
    extractor.extract_jobs()
    assert not extractor.create_or_update_model_metadata.called


def test_parameters_are_not_fetched_for_folders(extractor):
    """Avoids a pointless API call per folder."""
    extractor.extract_jobs()
    queried = [c[0][0] for c in extractor.jenkins_processor.get_job_parameters.call_args_list]
    assert 'Prod-EKS' not in queried


# ---------------------------------------------------------------------------
# Metadata enrichment
# ---------------------------------------------------------------------------


def test_multibranch_flag_is_stored(extractor):
    extractor.extract_jobs()
    saved = _saved_models(extractor)

    assert saved['gh-wi-candidateevents-ms-wi-candidate-events-ms']['is_multibranch'] is True
    assert saved['Prod-EKS/api-service']['is_multibranch'] is False


def test_parent_folder_is_stored_for_nested_jobs(extractor):
    extractor.extract_jobs()
    saved = _saved_models(extractor)

    assert saved['Prod-EKS/api-service']['parent_folder'] == 'Prod-EKS'
    # a top-level job has no parent folder key at all
    assert 'parent_folder' not in saved['gh-wi-candidateevents-ms-wi-candidate-events-ms']


def test_short_name_and_full_path_are_both_stored(extractor):
    extractor.extract_jobs()
    entry = _saved_models(extractor)['Prod-EKS/api-service']

    assert entry['name'] == 'api-service'
    assert entry['full_path'] == 'Prod-EKS/api-service'


# ---------------------------------------------------------------------------
# Failure handling
# ---------------------------------------------------------------------------


def test_failed_connection_persists_nothing(extractor):
    extractor.jenkins_processor.test_connection.return_value = False
    extractor.extract_jobs()
    assert not extractor.create_or_update_model_metadata.called


def test_no_jobs_persists_nothing(extractor):
    extractor.jenkins_processor.get_all_jobs_recursive.return_value = []
    extractor.extract_jobs()
    assert not extractor.create_or_update_model_metadata.called
