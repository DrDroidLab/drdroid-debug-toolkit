import logging
import requests

from core.integrations.processor import Processor

logger = logging.getLogger(__name__)


class DatabricksApiProcessor(Processor):

    def __init__(self, databricks_host: str, databricks_token: str):
        self.databricks_host = databricks_host.rstrip('/')
        self.databricks_token = databricks_token

    def test_connection(self):
        try:
            url = f"{self.databricks_host}/api/2.0/clusters/list"
            headers = {
                "Authorization": f"Bearer {self.databricks_token}",
                "Content-Type": "application/json",
            }
            response = requests.get(url, headers=headers, timeout=30)
            if response.status_code == 200:
                return True
            elif response.status_code == 403:
                logger.error(f"Databricks authentication failed: {response.text}")
                raise Exception(f"Authentication failed: Insufficient permissions or invalid token")
            elif response.status_code == 401:
                logger.error(f"Databricks authentication failed: {response.text}")
                raise Exception(f"Authentication failed: Invalid token")
            else:
                logger.error(f"Databricks connection test failed with status {response.status_code}: {response.text}")
                raise Exception(f"Connection test failed with status {response.status_code}")
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Databricks connection error: {e}")
            raise Exception(f"Could not connect to Databricks workspace: {self.databricks_host}")
        except requests.exceptions.Timeout as e:
            logger.error(f"Databricks connection timeout: {e}")
            raise Exception(f"Connection to Databricks workspace timed out")

    def _get(self, path: str, params: dict = None) -> dict:
        url = f"{self.databricks_host}{path}"
        headers = {
            "Authorization": f"Bearer {self.databricks_token}",
            "Content-Type": "application/json",
        }
        response = requests.get(url, headers=headers, params=params, timeout=60)
        response.raise_for_status()
        return response.json()

    def list_jobs(self) -> list:
        result = self._get("/api/2.1/jobs/list", params={"limit": 100})
        return result.get("jobs", [])

    def list_clusters(self) -> list:
        result = self._get("/api/2.0/clusters/list")
        return result.get("clusters", [])

    def list_sql_warehouses(self) -> list:
        result = self._get("/api/2.0/sql/warehouses")
        return result.get("warehouses", [])
