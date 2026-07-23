import logging
import requests

from core.integrations.processor import Processor
from core.settings import EXTERNAL_CALL_TIMEOUT

logger = logging.getLogger(__name__)


class ChecklyApiProcessor(Processor):

    BASE_URL = "https://api.checklyhq.com/v1"

    def __init__(self, api_key: str, account_id: str):
        self.__api_key = api_key
        self.__account_id = account_id

    def _headers(self):
        return {
            "Authorization": f"Bearer {self.__api_key}",
            "X-Checkly-Account": self.__account_id,
            "Accept": "application/json",
        }

    def _get(self, path: str, params: dict = None) -> dict:
        url = f"{self.BASE_URL}{path}"
        response = requests.get(url, headers=self._headers(), params=params, timeout=EXTERNAL_CALL_TIMEOUT)
        response.raise_for_status()
        return response.json()

    def test_connection(self) -> bool:
        try:
            self._get("/checks", params={"limit": 1})
            return True
        except requests.exceptions.HTTPError as e:
            logger.error(f"ChecklyApiProcessor.test_connection:: HTTP error: {e}")
        except Exception as e:
            logger.error(f"ChecklyApiProcessor.test_connection:: Error: {e}")
        return False

    def list_checks(self) -> dict:
        try:
            return self._get("/checks")
        except Exception as e:
            logger.error(f"ChecklyApiProcessor.list_checks:: Error: {e}")
            raise

    def get_check_statuses(self) -> dict:
        try:
            return self._get("/check-statuses")
        except Exception as e:
            logger.error(f"ChecklyApiProcessor.get_check_statuses:: Error: {e}")
            raise

    def get_check_results(self, check_id: str, limit: int = 10) -> dict:
        try:
            return self._get(f"/check-results/{check_id}", params={"limit": limit})
        except Exception as e:
            logger.error(f"ChecklyApiProcessor.get_check_results:: Error for check {check_id}: {e}")
            raise

    def get_check_analytics(self, check_id: str, check_type: str, from_ts: int = None, to_ts: int = None) -> dict:
        try:
            check_type = check_type.lower() if check_type else "api"
            type_map = {
                "api": "api-checks",
                "browser": "browser-checks",
                "multistep": "multistep-checks",
                "tcp": "tcp-checks",
            }
            endpoint_segment = type_map.get(check_type, "api-checks")
            params = {}
            if from_ts:
                params["from"] = from_ts
            if to_ts:
                params["to"] = to_ts
            return self._get(f"/analytics/{endpoint_segment}/{check_id}", params=params or None)
        except Exception as e:
            logger.error(f"ChecklyApiProcessor.get_check_analytics:: Error for check {check_id}: {e}")
            raise

    def get_check_alerts(self, check_id: str = None) -> dict:
        try:
            if check_id:
                return self._get(f"/check-alerts/{check_id}")
            return self._get("/check-alerts")
        except Exception as e:
            logger.error(f"ChecklyApiProcessor.get_check_alerts:: Error: {e}")
            raise

    def get_check_groups(self) -> dict:
        try:
            return self._get("/check-groups")
        except Exception as e:
            logger.error(f"ChecklyApiProcessor.get_check_groups:: Error: {e}")
            raise

    def get_error_groups(self, check_id: str = None, limit: int = None, page: int = None) -> dict:
        try:
            params = {}
            if limit:
                params["limit"] = limit
            if page:
                params["page"] = page
            path = f"/error-groups/checks/{check_id}" if check_id else "/error-groups"
            return self._get(path, params=params or None)
        except Exception as e:
            logger.error(f"ChecklyApiProcessor.get_error_groups:: Error: {e}")
            raise

    def get_reporting(self, from_ts: str = None, to_ts: str = None, quick_range: str = None, granularity: str = None) -> dict:
        try:
            params = {}
            if from_ts and to_ts:
                params["from"] = from_ts
                params["to"] = to_ts
            elif quick_range:
                params["quickRange"] = quick_range
            if granularity:
                params["granularity"] = granularity
            return self._get("/reporting", params=params or None)
        except Exception as e:
            logger.error(f"ChecklyApiProcessor.get_reporting:: Error: {e}")
            raise

    def get_check_result_detail(self, check_id: str, check_result_id: str) -> dict:
        try:
            return self._get(f"/check-results/{check_id}/{check_result_id}")
        except Exception as e:
            logger.error(f"ChecklyApiProcessor.get_check_result_detail:: Error for check {check_id}: {e}")
            raise

    def get_check_result_assets(self, check_id: str, check_result_id: str) -> dict:
        try:
            return self._get(f"/check-results/{check_id}/{check_result_id}/assets")
        except Exception as e:
            logger.error(f"ChecklyApiProcessor.get_check_result_assets:: Error for check {check_id}: {e}")
            raise

    def get_alert_channels(self, limit: int = None, page: int = None) -> dict:
        try:
            params = {}
            if limit:
                params["limit"] = limit
            if page:
                params["page"] = page
            return self._get("/alert-channels", params=params or None)
        except Exception as e:
            logger.error(f"ChecklyApiProcessor.get_alert_channels:: Error: {e}")
            raise
