import logging
import time

import requests

from core.integrations.processor import Processor
from core.settings import EXTERNAL_CALL_TIMEOUT

logger = logging.getLogger(__name__)


# A Site24x7 account lives in exactly one Zoho data center. The OAuth token must be
# minted from that region's accounts host, and API calls must go to that region's
# API host. The two hostnames don't follow a single pattern (e.g. AU uses .com.au for
# the token but .net.au for the API; JP uses the app. subdomain), so they are mapped
# explicitly. Key is the user-selected region code (case-insensitive).
DATA_CENTERS = {
    "US": ("https://accounts.zoho.com", "https://www.site24x7.com/api"),
    "EU": ("https://accounts.zoho.eu", "https://www.site24x7.eu/api"),
    "IN": ("https://accounts.zoho.in", "https://www.site24x7.in/api"),
    "AU": ("https://accounts.zoho.com.au", "https://www.site24x7.net.au/api"),
    "JP": ("https://accounts.zoho.jp", "https://app.site24x7.jp/api"),
    "CA": ("https://accounts.zohocloud.ca", "https://www.site24x7.ca/api"),
    "UK": ("https://accounts.zoho.uk", "https://app.site24x7.uk/api"),
    "CN": ("https://accounts.zoho.com.cn", "https://www.site24x7.cn/api"),
}


class Site24x7ApiProcessor(Processor):

    def __init__(self, client_id: str, client_secret: str, refresh_token: str, data_center: str = "US"):
        self.__client_id = client_id
        self.__client_secret = client_secret
        self.__refresh_token = refresh_token

        dc = (data_center or "US").strip().upper()
        if dc not in DATA_CENTERS:
            raise Exception(
                f"Unsupported Site24x7 data_center '{data_center}'. "
                f"Expected one of: {', '.join(DATA_CENTERS.keys())}"
            )
        self.data_center = dc
        self.__accounts_host, self.base_url = DATA_CENTERS[dc]

        self._access_token = None
        self._token_expires_at = 0  # epoch seconds

    def _get_access_token(self) -> str:
        """Exchange the permanent refresh token for a short-lived access token, cached
        until ~1 minute before expiry."""
        if self._access_token and time.time() < self._token_expires_at - 60:
            return self._access_token

        url = f"{self.__accounts_host}/oauth/v2/token"
        params = {
            "grant_type": "refresh_token",
            "client_id": self.__client_id,
            "client_secret": self.__client_secret,
            "refresh_token": self.__refresh_token,
        }
        response = requests.post(url, params=params, timeout=EXTERNAL_CALL_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        if "access_token" not in data:
            # Zoho returns HTTP 200 with an "error" field on auth failures.
            raise Exception(f"Site24x7 token refresh failed: {data.get('error', data)}")

        self._access_token = data["access_token"]
        self._token_expires_at = time.time() + int(data.get("expires_in", 3600))
        return self._access_token

    def _headers(self) -> dict:
        return {
            "Authorization": f"Zoho-oauthtoken {self._get_access_token()}",
            "Accept": "application/json; version=2.0",
        }

    def _get(self, path: str, params: dict = None) -> dict:
        url = f"{self.base_url}{path}"
        response = requests.get(url, headers=self._headers(), params=params, timeout=EXTERNAL_CALL_TIMEOUT)
        # A stale/expired token surfaces as 401; refresh once and retry.
        if response.status_code == 401:
            self._access_token = None
            response = requests.get(url, headers=self._headers(), params=params, timeout=EXTERNAL_CALL_TIMEOUT)
        response.raise_for_status()
        body = response.json()
        # Site24x7 wraps successful responses as {"code": 0, "message": "success", "data": {...}}.
        # A non-zero code indicates an API-level error even on HTTP 200.
        if isinstance(body, dict) and body.get("code", 0) not in (0, None):
            raise Exception(f"Site24x7 API error on {path}: {body.get('message', body)}")
        return body

    def test_connection(self) -> bool:
        try:
            self._get("/monitors")
            return True
        except requests.exceptions.HTTPError as e:
            logger.error(f"Site24x7ApiProcessor.test_connection:: HTTP error: {e}")
        except Exception as e:
            logger.error(f"Site24x7ApiProcessor.test_connection:: Error: {e}")
        return False

    def get_current_status(self, params: dict = None) -> dict:
        try:
            return self._get("/current_status", params=params)
        except Exception as e:
            logger.error(f"Site24x7ApiProcessor.get_current_status:: Error: {e}")
            raise

    def list_monitors(self) -> dict:
        try:
            return self._get("/monitors")
        except Exception as e:
            logger.error(f"Site24x7ApiProcessor.list_monitors:: Error: {e}")
            raise

    def get_monitor_details(self, monitor_id: str) -> dict:
        try:
            return self._get(f"/monitors/{monitor_id}")
        except Exception as e:
            logger.error(f"Site24x7ApiProcessor.get_monitor_details:: Error for monitor {monitor_id}: {e}")
            raise

    def get_availability_report(self, monitor_id: str, params: dict = None) -> dict:
        try:
            return self._get(f"/reports/availability/{monitor_id}", params=params)
        except Exception as e:
            logger.error(f"Site24x7ApiProcessor.get_availability_report:: Error for monitor {monitor_id}: {e}")
            raise

    def get_performance_report(self, monitor_id: str, params: dict = None) -> dict:
        try:
            return self._get(f"/reports/performance/{monitor_id}", params=params)
        except Exception as e:
            logger.error(f"Site24x7ApiProcessor.get_performance_report:: Error for monitor {monitor_id}: {e}")
            raise
