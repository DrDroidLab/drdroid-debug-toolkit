"""Smoke: Coralogix manager must boot against packaged TaskType enum.

Regression for FETCH_DASHBOARD_LIST missing from cloud protos.
"""
import os
import sys

from django.conf import settings

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "drdroid_debug_toolkit"))
if not settings.configured:
    settings.configure(IS_PROD_ENV=False, SECRET_KEY="test", USE_TZ=True, INSTALLED_APPS=[])

from core.integrations.source_managers.coralogix_source_manager import CoralogixSourceManager
from core.protos.playbooks.source_task_definitions.coralogix_task_pb2 import Coralogix


def test_fetch_dashboard_list_enum_exists():
    assert "FETCH_DASHBOARD_LIST" in Coralogix.TaskType.keys()


def test_coralogix_source_manager_instantiates():
    mgr = CoralogixSourceManager()
    # When packaged protos include the enum, it must be registered.
    if "FETCH_DASHBOARD_LIST" in Coralogix.TaskType.keys():
        assert Coralogix.TaskType.FETCH_DASHBOARD_LIST in mgr.task_type_callable_map
    # Core tasks must always be present.
    assert Coralogix.TaskType.FETCH_LOGS in mgr.task_type_callable_map


def test_coralogix_manager_boots_without_optional_enum(monkeypatch):
    """Simulate older protobuf missing FETCH_DASHBOARD_LIST — manager must still construct."""
    from core.integrations.source_managers import coralogix_source_manager as mod

    real_value = Coralogix.TaskType.Value

    def _value(name):
        if name == "FETCH_DASHBOARD_LIST":
            raise ValueError(name)
        return real_value(name)

    monkeypatch.setattr(Coralogix.TaskType, "Value", staticmethod(_value))
    mgr = mod.CoralogixSourceManager()
    assert Coralogix.TaskType.FETCH_LOGS in mgr.task_type_callable_map
