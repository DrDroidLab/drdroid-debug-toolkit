"""SourceManager.execute_task must not double-wrap error messages.

Bug: the inner handler prefixes task-execution errors ("Error while executing task
for source: X with error: <real>"), then the outer handler re-wrapped the SAME
exception again with "Error while executing task: ...". The real reason ended up
nested two layers deep and got truncated in UIs/probes (e.g. the render 404 and
"column does not exist" errors seen on the dogfood account). The outer handler now
passes already-prefixed messages through unchanged, while still wrapping genuinely
unwrapped setup errors.

Run: python -m pytest tests/test_execute_task_error_not_double_wrapped.py -v
"""
import os
import sys
from unittest.mock import MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "drdroid_debug_toolkit"))

from core.integrations.source_manager import SourceManager  # noqa: E402
from core.protos.base_pb2 import Source  # noqa: E402


class _StubManager(SourceManager):
    # Bypass the real __init__ (connector registries etc.); execute_task only needs
    # task_type_callable_map + the two methods we mock per-test.
    def __init__(self, executor):
        self.task_type_callable_map = {1: {"executor": executor, "result_type": None}}


def _mgr_and_task(executor):
    mgr = _StubManager(executor)
    mgr.get_resolved_task = MagicMock(return_value=(
        MagicMock(source=Source.GITHUB), MagicMock(type=1), {}))
    task = MagicMock()
    task.task_connector_sources = []  # skip the connector-resolution branch
    return mgr, task


def test_task_execution_error_is_prefixed_once():
    def boom(*a, **k):
        raise Exception("boom detail")
    mgr, task = _mgr_and_task(boom)
    try:
        mgr.execute_task(None, {}, task)
        assert False, "expected an exception"
    except Exception as e:
        msg = str(e)
    # single prefix, real reason preserved and up front — no doubled wrapper
    assert msg == "Error while executing task for source: github with error: boom detail", msg
    assert "Error while executing task: Error while executing task" not in msg
    assert "boom detail" in msg


def test_setup_error_is_still_wrapped_once():
    # An error raised BEFORE the inner try (unwrapped) must still get the outer prefix,
    # exactly once — the outer handler still does its job for setup failures.
    mgr = _StubManager(lambda *a, **k: None)
    mgr.get_resolved_task = MagicMock(side_effect=Exception("setup failed"))
    task = MagicMock()
    task.task_connector_sources = []
    try:
        mgr.execute_task(None, {}, task)
        assert False, "expected an exception"
    except Exception as e:
        msg = str(e)
    assert msg == "Error while executing task: setup failed", msg
