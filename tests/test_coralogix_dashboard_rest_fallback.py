"""Coralogix dashboard catalog: gRPC preferred, REST when grpcurl missing."""
import os
import sys
from unittest.mock import MagicMock, patch

from django.conf import settings

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "drdroid_debug_toolkit"))
if not settings.configured:
    settings.configure(IS_PROD_ENV=False, SECRET_KEY="test", USE_TZ=True, INSTALLED_APPS=[])

from core.integrations.source_api_processors.coralogix_api_processor import CoralogixApiProcessor


def test_fetch_dashboards_falls_back_to_rest_without_grpcurl():
    proc = CoralogixApiProcessor(api_key="k", endpoint="https://api.coralogix.com")
    with patch.object(proc, "_grpcurl", side_effect=FileNotFoundError("grpcurl")):
        with patch.object(proc, "_fetch_dashboards_rest", return_value={"items": [{"id": "1"}]}) as rest:
            assert proc.fetch_dashboards() == {"items": [{"id": "1"}]}
            rest.assert_called_once()


def test_fetch_dashboard_details_falls_back_to_rest_without_grpcurl():
    proc = CoralogixApiProcessor(api_key="k", endpoint="https://api.coralogix.com")
    with patch.object(proc, "_grpcurl", side_effect=FileNotFoundError("grpcurl")):
        with patch.object(
            proc, "_fetch_dashboard_details_rest", return_value={"dashboard": {"id": "d1"}}
        ) as rest:
            assert proc.fetch_dashboard_details("d1") == {"dashboard": {"id": "d1"}}
            rest.assert_called_once_with("d1")


def test_grpcurl_reraises_file_not_found():
    proc = CoralogixApiProcessor(api_key="k", endpoint="https://api.coralogix.com")
    with patch(
        "core.integrations.source_api_processors.coralogix_api_processor.subprocess.run",
        side_effect=FileNotFoundError("grpcurl"),
    ):
        try:
            proc._grpcurl("svc/Method")
            assert False, "expected FileNotFoundError"
        except FileNotFoundError:
            pass
