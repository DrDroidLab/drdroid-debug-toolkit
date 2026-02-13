import logging

import requests

from core.integrations.processor import Processor
from core.settings import EXTERNAL_CALL_TIMEOUT

logger = logging.getLogger(__name__)


class MetabaseApiProcessor(Processor):
    def __init__(self, metabase_url, metabase_api_key):
        self.__host = metabase_url.rstrip('/')
        self.__api_key = metabase_api_key
        self.headers = {
            "x-api-key": self.__api_key,
            "Content-Type": "application/json"
        }

    def test_connection(self):
        """
        Test connection to Metabase by calling the /api/user/current endpoint.
        This endpoint returns the current user info and validates API key authentication.

        Returns:
            bool: True if connection is successful
        Raises:
            Exception: If connection fails
        """
        try:
            url = f"{self.__host}/api/user/current"
            response = requests.get(url, headers=self.headers, timeout=EXTERNAL_CALL_TIMEOUT)
            response.raise_for_status()
            return True
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error while testing Metabase connection: {e}")
            raise Exception(f"Metabase connection failed: {e}")
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Connection error while testing Metabase connection: {e}")
            raise Exception(f"Could not connect to Metabase at {self.__host}: {e}")
        except requests.exceptions.Timeout as e:
            logger.error(f"Timeout while testing Metabase connection: {e}")
            raise Exception(f"Metabase connection timed out: {e}")
        except Exception as e:
            logger.error(f"Exception occurred while testing Metabase connection: {e}")
            raise e

    # Alert endpoints

    def list_alerts(self):
        try:
            url = f"{self.__host}/api/alert"
            response = requests.get(url, headers=self.headers, timeout=EXTERNAL_CALL_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"MetabaseApiProcessor.list_alerts:: Error listing alerts: {e}")
            raise

    def get_alert(self, alert_id):
        try:
            url = f"{self.__host}/api/alert/{alert_id}"
            response = requests.get(url, headers=self.headers, timeout=EXTERNAL_CALL_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"MetabaseApiProcessor.get_alert:: Error getting alert {alert_id}: {e}")
            raise

    def create_alert(self, payload):
        try:
            url = f"{self.__host}/api/alert"
            response = requests.post(url, headers=self.headers, json=payload, timeout=EXTERNAL_CALL_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"MetabaseApiProcessor.create_alert:: Error creating alert: {e}")
            raise

    def update_alert(self, alert_id, payload):
        try:
            url = f"{self.__host}/api/alert/{alert_id}"
            response = requests.put(url, headers=self.headers, json=payload, timeout=EXTERNAL_CALL_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"MetabaseApiProcessor.update_alert:: Error updating alert {alert_id}: {e}")
            raise

    def delete_alert(self, alert_id):
        try:
            url = f"{self.__host}/api/alert/{alert_id}"
            response = requests.delete(url, headers=self.headers, timeout=EXTERNAL_CALL_TIMEOUT)
            response.raise_for_status()
            return True
        except Exception as e:
            logger.error(f"MetabaseApiProcessor.delete_alert:: Error deleting alert {alert_id}: {e}")
            raise

    # Pulse endpoints

    def list_pulses(self):
        try:
            url = f"{self.__host}/api/pulse"
            response = requests.get(url, headers=self.headers, timeout=EXTERNAL_CALL_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"MetabaseApiProcessor.list_pulses:: Error listing pulses: {e}")
            raise

    def get_pulse(self, pulse_id):
        try:
            url = f"{self.__host}/api/pulse/{pulse_id}"
            response = requests.get(url, headers=self.headers, timeout=EXTERNAL_CALL_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"MetabaseApiProcessor.get_pulse:: Error getting pulse {pulse_id}: {e}")
            raise

    def create_pulse(self, payload):
        try:
            url = f"{self.__host}/api/pulse"
            response = requests.post(url, headers=self.headers, json=payload, timeout=EXTERNAL_CALL_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"MetabaseApiProcessor.create_pulse:: Error creating pulse: {e}")
            raise

    def update_pulse(self, pulse_id, payload):
        try:
            url = f"{self.__host}/api/pulse/{pulse_id}"
            response = requests.put(url, headers=self.headers, json=payload, timeout=EXTERNAL_CALL_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"MetabaseApiProcessor.update_pulse:: Error updating pulse {pulse_id}: {e}")
            raise

    def delete_pulse(self, pulse_id):
        try:
            url = f"{self.__host}/api/pulse/{pulse_id}"
            response = requests.delete(url, headers=self.headers, timeout=EXTERNAL_CALL_TIMEOUT)
            response.raise_for_status()
            return True
        except Exception as e:
            logger.error(f"MetabaseApiProcessor.delete_pulse:: Error deleting pulse {pulse_id}: {e}")
            raise
