"""Tests for the actionable datasource-miss error (GrafanaSourceManager).

Bug: when the model passes a datasource that can't be resolved to a real UID of the tool's type
(a cross-type name, garbage, or a connector with no such datasource), the query hit a bare
`/api/ds/query` 404 "Data source not found" — the top Grafana failure on account 39992, with no
recovery signal. `_resolve_datasource_uid_or_error` now returns an actionable error listing the
connector's datasources of the expected type so the agent can retry with a valid one.

The tempo handlers still use the passthrough `_resolve_datasource_uid` (unchanged), and the pure
matcher `_match_datasource_uid` is shared by both.

Run: python -m pytest tests/test_grafana_datasource_actionable_error.py -v
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'drdroid_debug_toolkit'))
from core.integrations.source_managers.grafana_source_manager import GrafanaSourceManager  # noqa: E402
from core.utils.proto_utils import proto_to_dict  # noqa: E402


class _FakeProc:
    def __init__(self, datasources, raise_=False):
        self._ds, self._raise = datasources, raise_

    def fetch_data_sources(self):
        if self._raise:
            raise RuntimeError("grafana unreachable")
        return self._ds


DS = [
    {"uid": "TLOx7hq7z", "name": "loki-k8-employer", "type": "loki", "isDefault": False},
    {"uid": "promDefault1", "name": "Prometheus", "type": "prometheus", "isDefault": True},
    {"uid": "tempo1", "name": "Tempo", "type": "tempo", "isDefault": False},
]

match = GrafanaSourceManager._match_datasource_uid


# --- pure matcher ---
def test_match_uid_passthrough():
    assert match(DS, "promDefault1", "prometheus") == ("promDefault1", True)


def test_match_name_resolves():
    assert match(DS, "Prometheus", "prometheus") == ("promDefault1", True)


def test_match_type_alias_resolves_to_default():
    assert match(DS, "prometheus", "prometheus") == ("promDefault1", True)


def test_match_cross_type_is_a_miss():
    # a loki datasource name must NOT resolve for a prometheus query
    assert match(DS, "loki-k8-employer", "prometheus") == ("loki-k8-employer", False)


def test_match_cross_type_uid_is_a_miss():
    # a real loki UID passed to a prometheus query must NOT resolve as-is (never misroute across types)
    assert match(DS, "TLOx7hq7z", "prometheus") == ("TLOx7hq7z", False)


def test_match_garbage_is_a_miss():
    assert match(DS, "nope-xyz", "prometheus")[1] is False


def test_match_type_alias_multiple_defaults_is_a_miss():
    # two prometheus datasources both marked default -> ambiguous, must not route arbitrarily
    two_defaults = [
        {"uid": "promA", "name": "Prom A", "type": "prometheus", "isDefault": True},
        {"uid": "promB", "name": "Prom B", "type": "prometheus", "isDefault": True},
    ]
    assert match(two_defaults, "prometheus", "prometheus") == ("prometheus", False)


# --- actionable error on miss ---
def _err_body(result):
    return proto_to_dict(result.api_response.response_body)


def test_resolve_or_error_success_returns_uid_and_none():
    mgr = GrafanaSourceManager()
    uid, err = mgr._resolve_datasource_uid_or_error(_FakeProc(DS), "Prometheus", "prometheus")
    assert uid == "promDefault1" and err is None


def test_resolve_or_error_miss_returns_actionable_error_with_available():
    mgr = GrafanaSourceManager()
    uid, err = mgr._resolve_datasource_uid_or_error(_FakeProc(DS), "loki-k8-employer", "prometheus")
    assert uid is None and err is not None
    body = _err_body(err)
    msg = body.get("error", "")
    assert "could not be resolved to a prometheus datasource" in msg
    assert "Prometheus (promDefault1)" in msg  # lists the valid one to retry with


def test_resolve_or_error_no_datasource_of_type():
    mgr = GrafanaSourceManager()
    only_loki = [DS[0]]
    uid, err = mgr._resolve_datasource_uid_or_error(_FakeProc(only_loki), "prometheus", "prometheus")
    assert uid is None and err is not None
    assert "No prometheus datasource is configured" in _err_body(err).get("error", "")


def test_resolve_or_error_list_failure_preserves_old_behavior():
    mgr = GrafanaSourceManager()
    uid, err = mgr._resolve_datasource_uid_or_error(_FakeProc(DS, raise_=True), "whatever", "prometheus")
    assert err is None and uid == "whatever"  # can't list -> attempt the query as before


def test_passthrough_resolver_unchanged_on_miss():
    # tempo handlers rely on this: a miss returns the provided value unchanged (no error object)
    assert GrafanaSourceManager._resolve_datasource_uid(_FakeProc(DS), "nope-xyz", "tempo") == "nope-xyz"
