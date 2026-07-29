"""Tests for GrafanaSourceManager._resolve_datasource_uid.

Bug: agents pass a datasource NAME ('loki-k8-employer') or a bare TYPE ('loki','prometheus',
'tempo') in datasource_uid, so Grafana's /api/ds/query returns 404 (top Grafana failure on
account 39992). The resolver maps these to a real UID against the connector's datasources.
Invariants pinned here:
  - a real UID passes through untouched (happy path),
  - an exact datasource NAME (of the tool's own type) resolves to its UID,
  - a bare TYPE resolves to the default datasource of that type, else the only one,
  - NEVER misroute: no cross-type resolution; ambiguous type (2+, no default) does NOT guess;
    a name that only exists under a different type does NOT resolve,
  - any lookup error / miss returns the provided value unchanged (query then 4xx-unmasks).

Run: python -m pytest tests/test_grafana_datasource_uid_resolve.py -v
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'drdroid_debug_toolkit'))
from core.integrations.source_managers.grafana_source_manager import GrafanaSourceManager  # noqa: E402

resolve = GrafanaSourceManager._resolve_datasource_uid


class _FakeProc:
    def __init__(self, datasources, raise_=False):
        self._ds, self._raise = datasources, raise_

    def fetch_data_sources(self):
        if self._raise:
            raise RuntimeError("grafana unreachable")
        return self._ds


# two loki datasources (one default), one prom, one tempo
DS = [
    {"uid": "TLOx7hq7z", "name": "loki-k8-employer", "type": "loki", "isDefault": False},
    {"uid": "lokiDefault9", "name": "loki-k8-infra", "type": "loki", "isDefault": True},
    {"uid": "promDefault1", "name": "Prometheus", "type": "prometheus", "isDefault": True},
    {"uid": "tempo1", "name": "Tempo", "type": "tempo", "isDefault": False},
]


def test_valid_uid_passthrough():
    assert resolve(_FakeProc(DS), "TLOx7hq7z", "loki") == "TLOx7hq7z"


def test_exact_name_resolves_to_uid():
    assert resolve(_FakeProc(DS), "loki-k8-employer", "loki") == "TLOx7hq7z"


def test_name_case_insensitive():
    assert resolve(_FakeProc(DS), "LOKI-K8-EMPLOYER", "loki") == "TLOx7hq7z"


def test_type_alias_resolves_to_default():
    # 'loki' -> the default loki datasource, not just any
    assert resolve(_FakeProc(DS), "loki", "loki") == "lokiDefault9"
    assert resolve(_FakeProc(DS), "prometheus", "prometheus") == "promDefault1"


def test_type_alias_single_of_type_resolves():
    one_loki = [{"uid": "onlyLoki", "name": "logs", "type": "loki", "isDefault": False}]
    assert resolve(_FakeProc(one_loki), "loki", "loki") == "onlyLoki"


def test_ambiguous_type_does_not_guess():
    # two loki, NEITHER default -> ambiguous -> return provided unchanged (never coin-flip a source)
    two_loki_no_default = [
        {"uid": "a", "name": "loki-a", "type": "loki", "isDefault": False},
        {"uid": "b", "name": "loki-b", "type": "loki", "isDefault": False},
    ]
    assert resolve(_FakeProc(two_loki_no_default), "loki", "loki") == "loki"


def test_no_cross_type_misroute():
    # 'prometheus' passed to a LOKI tool must NOT route to the prometheus datasource
    assert resolve(_FakeProc(DS), "prometheus", "loki") == "prometheus"


def test_name_of_other_type_not_resolved():
    # 'Prometheus' is a real datasource NAME but of the wrong type for a loki tool -> unchanged
    assert resolve(_FakeProc(DS), "Prometheus", "loki") == "Prometheus"


def test_unknown_identifier_passthrough():
    # a genuinely stale/wrong UID that is neither a known uid nor a name/type -> unchanged (4xx-unmasks)
    assert resolve(_FakeProc(DS), "p8Mz_msnz", "loki") == "p8Mz_msnz"


def test_lookup_error_returns_provided():
    assert resolve(_FakeProc(DS, raise_=True), "loki", "loki") == "loki"


def test_empty_provided():
    assert resolve(_FakeProc(DS), "", "loki") == ""


def test_tempo_name_resolves():
    assert resolve(_FakeProc(DS), "Tempo", "tempo") == "tempo1"
