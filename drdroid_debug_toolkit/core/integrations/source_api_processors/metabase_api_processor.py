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
