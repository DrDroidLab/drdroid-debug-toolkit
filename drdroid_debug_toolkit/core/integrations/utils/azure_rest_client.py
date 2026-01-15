"""
Azure REST API Client for direct API access without CLI dependency.

Uses OAuth2 client credentials flow for authentication and provides
methods for common Azure Resource Manager operations.
"""
import logging
import time
from typing import Dict, Any, Optional, List
from urllib.parse import urlencode

import requests

from core.settings import EXTERNAL_CALL_TIMEOUT

logger = logging.getLogger(__name__)

# Azure endpoints
AZURE_LOGIN_URL = "https://login.microsoftonline.com"
AZURE_MANAGEMENT_URL = "https://management.azure.com"
AZURE_LOG_ANALYTICS_URL = "https://api.loganalytics.io/v1"

# API versions for different services
API_VERSIONS = {
    "resources": "2021-04-01",
    "resource_groups": "2021-04-01",
    "aks": "2024-01-01",
    "compute": "2024-03-01",
    "storage": "2023-05-01",
    "sql": "2021-11-01",
    "cosmos": "2023-11-15",
    "monitor_alerts": "2018-03-01",
    "action_groups": "2023-01-01",
    "log_analytics": "2022-10-01",
    "postgres_flexible": "2022-12-01",
    "redis": "2023-08-01",
    "metrics": "2023-10-01",
}


class AzureAuthError(Exception):
    """Raised when Azure authentication fails."""
    pass


class AzureAPIError(Exception):
    """Raised when Azure API call fails."""
    def __init__(self, status_code: int, message: str, response: dict = None):
        self.status_code = status_code
        self.message = message
        self.response = response
        super().__init__(f"Azure API Error ({status_code}): {message}")


class AzureRESTClient:
    """
    Azure REST API client using OAuth2 client credentials flow.

    Handles token acquisition, caching, and renewal automatically.
    """

    def __init__(
        self,
        subscription_id: str,
        tenant_id: str,
        client_id: str,
        client_secret: str,
        timeout: int = EXTERNAL_CALL_TIMEOUT
    ):
        self._subscription_id = subscription_id
        self._tenant_id = tenant_id
        self._client_id = client_id
        self._client_secret = client_secret
        self._timeout = timeout

        # Token cache
        self._access_token: Optional[str] = None
        self._token_expiry: float = 0

        # Session for connection pooling
        self._session = requests.Session()

    def _get_access_token(self) -> str:
        """
        Get a valid access token, refreshing if necessary.

        Uses OAuth2 client credentials flow to obtain token from Azure AD.
        """
        # Return cached token if still valid (with 5 min buffer)
        if self._access_token and time.time() < (self._token_expiry - 300):
            return self._access_token

        token_url = f"{AZURE_LOGIN_URL}/{self._tenant_id}/oauth2/v2.0/token"

        data = {
            "grant_type": "client_credentials",
            "client_id": self._client_id,
            "client_secret": self._client_secret,
            "scope": f"{AZURE_MANAGEMENT_URL}/.default"
        }

        try:
            response = self._session.post(
                token_url,
                data=data,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=self._timeout
            )

            if response.status_code != 200:
                error_data = response.json() if response.text else {}
                error_msg = error_data.get("error_description", response.text)
                raise AzureAuthError(f"Failed to obtain access token: {error_msg}")

            token_data = response.json()
            self._access_token = token_data["access_token"]
            # Set expiry time (expires_in is in seconds)
            self._token_expiry = time.time() + token_data.get("expires_in", 3600)

            logger.debug("Successfully obtained Azure access token")
            return self._access_token

        except requests.RequestException as e:
            raise AzureAuthError(f"Network error during token acquisition: {e}")

    def _make_request(
        self,
        method: str,
        url: str,
        params: Dict[str, str] = None,
        json_body: Dict[str, Any] = None,
        timeout: int = None
    ) -> Dict[str, Any]:
        """
        Make an authenticated request to Azure REST API.

        Handles authentication, error responses, and JSON parsing.
        """
        token = self._get_access_token()

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

        try:
            response = self._session.request(
                method=method,
                url=url,
                params=params,
                json=json_body,
                headers=headers,
                timeout=timeout or self._timeout
            )

            # Handle successful responses
            if response.status_code in (200, 201, 202):
                if response.text:
                    return response.json()
                return {}

            # Handle errors
            error_data = response.json() if response.text else {}
            error_msg = error_data.get("error", {}).get("message", response.text)
            raise AzureAPIError(response.status_code, error_msg, error_data)

        except requests.RequestException as e:
            raise AzureAPIError(0, f"Network error: {e}")

    def _get_paginated_results(
        self,
        url: str,
        params: Dict[str, str] = None,
        value_key: str = "value"
    ) -> List[Dict[str, Any]]:
        """
        Fetch all pages of a paginated API response.

        Azure REST APIs return 'nextLink' for pagination.
        """
        results = []

        while url:
            response = self._make_request("GET", url, params=params)
            results.extend(response.get(value_key, []))

            # Get next page URL (params already included in nextLink)
            url = response.get("nextLink")
            params = None  # nextLink already includes query params

        return results

    # ==================== Resource Management APIs ====================

    def list_resource_groups(self) -> List[Dict[str, Any]]:
        """List all resource groups in the subscription."""
        url = f"{AZURE_MANAGEMENT_URL}/subscriptions/{self._subscription_id}/resourcegroups"
        params = {"api-version": API_VERSIONS["resource_groups"]}
        return self._get_paginated_results(url, params)

    def list_resources(self, filter_expr: str = None) -> List[Dict[str, Any]]:
        """List all resources in the subscription."""
        url = f"{AZURE_MANAGEMENT_URL}/subscriptions/{self._subscription_id}/resources"
        params = {"api-version": API_VERSIONS["resources"]}
        if filter_expr:
            params["$filter"] = filter_expr
        return self._get_paginated_results(url, params)

    def list_workspaces(self) -> List[Dict[str, Any]]:
        """List all Log Analytics workspaces."""
        url = f"{AZURE_MANAGEMENT_URL}/subscriptions/{self._subscription_id}/providers/Microsoft.OperationalInsights/workspaces"
        params = {"api-version": API_VERSIONS["log_analytics"]}
        return self._get_paginated_results(url, params)

    # ==================== AKS APIs ====================

    def aks_list_clusters(self) -> List[Dict[str, Any]]:
        """List all AKS clusters in the subscription."""
        url = f"{AZURE_MANAGEMENT_URL}/subscriptions/{self._subscription_id}/providers/Microsoft.ContainerService/managedClusters"
        params = {"api-version": API_VERSIONS["aks"]}
        return self._get_paginated_results(url, params)

    def aks_get_cluster(self, resource_group: str, cluster_name: str) -> Optional[Dict[str, Any]]:
        """Get details of a specific AKS cluster."""
        url = (
            f"{AZURE_MANAGEMENT_URL}/subscriptions/{self._subscription_id}"
            f"/resourceGroups/{resource_group}"
            f"/providers/Microsoft.ContainerService/managedClusters/{cluster_name}"
        )
        params = {"api-version": API_VERSIONS["aks"]}
        return self._make_request("GET", url, params)

    def aks_get_admin_credentials(self, resource_group: str, cluster_name: str) -> Optional[Dict[str, Any]]:
        """
        Get admin kubeconfig credentials for an AKS cluster.

        Returns the kubeconfig content that can be used to connect to the cluster.
        """
        url = (
            f"{AZURE_MANAGEMENT_URL}/subscriptions/{self._subscription_id}"
            f"/resourceGroups/{resource_group}"
            f"/providers/Microsoft.ContainerService/managedClusters/{cluster_name}"
            f"/listClusterAdminCredential"
        )
        params = {"api-version": API_VERSIONS["aks"]}
        return self._make_request("POST", url, params)

    # ==================== Compute APIs ====================

    def compute_list_vms(self) -> List[Dict[str, Any]]:
        """List all virtual machines in the subscription."""
        url = f"{AZURE_MANAGEMENT_URL}/subscriptions/{self._subscription_id}/providers/Microsoft.Compute/virtualMachines"
        params = {"api-version": API_VERSIONS["compute"]}
        return self._get_paginated_results(url, params)

    def compute_list_vmss(self) -> List[Dict[str, Any]]:
        """List all VM scale sets in the subscription."""
        url = f"{AZURE_MANAGEMENT_URL}/subscriptions/{self._subscription_id}/providers/Microsoft.Compute/virtualMachineScaleSets"
        params = {"api-version": API_VERSIONS["compute"]}
        return self._get_paginated_results(url, params)

    # ==================== Storage APIs ====================

    def storage_list_accounts(self) -> List[Dict[str, Any]]:
        """List all storage accounts in the subscription."""
        url = f"{AZURE_MANAGEMENT_URL}/subscriptions/{self._subscription_id}/providers/Microsoft.Storage/storageAccounts"
        params = {"api-version": API_VERSIONS["storage"]}
        return self._get_paginated_results(url, params)

    def storage_list_blob_containers(self, resource_group: str, account_name: str) -> List[Dict[str, Any]]:
        """List blob containers in a storage account."""
        url = (
            f"{AZURE_MANAGEMENT_URL}/subscriptions/{self._subscription_id}"
            f"/resourceGroups/{resource_group}"
            f"/providers/Microsoft.Storage/storageAccounts/{account_name}"
            f"/blobServices/default/containers"
        )
        params = {"api-version": API_VERSIONS["storage"]}
        return self._get_paginated_results(url, params)

    # ==================== SQL APIs ====================

    def sql_list_servers(self) -> List[Dict[str, Any]]:
        """List all SQL servers in the subscription."""
        url = f"{AZURE_MANAGEMENT_URL}/subscriptions/{self._subscription_id}/providers/Microsoft.Sql/servers"
        params = {"api-version": API_VERSIONS["sql"]}
        return self._get_paginated_results(url, params)

    def sql_list_databases(self, resource_group: str, server_name: str) -> List[Dict[str, Any]]:
        """List all databases in a SQL server."""
        url = (
            f"{AZURE_MANAGEMENT_URL}/subscriptions/{self._subscription_id}"
            f"/resourceGroups/{resource_group}"
            f"/providers/Microsoft.Sql/servers/{server_name}/databases"
        )
        params = {"api-version": API_VERSIONS["sql"]}
        return self._get_paginated_results(url, params)

    # ==================== Cosmos DB APIs ====================

    def cosmos_list_accounts(self) -> List[Dict[str, Any]]:
        """List all Cosmos DB accounts in the subscription."""
        url = f"{AZURE_MANAGEMENT_URL}/subscriptions/{self._subscription_id}/providers/Microsoft.DocumentDB/databaseAccounts"
        params = {"api-version": API_VERSIONS["cosmos"]}
        return self._get_paginated_results(url, params)

    # ==================== PostgreSQL Flexible Server APIs ====================

    def postgres_flexible_list_servers(self) -> List[Dict[str, Any]]:
        """List all PostgreSQL Flexible Servers in the subscription."""
        url = f"{AZURE_MANAGEMENT_URL}/subscriptions/{self._subscription_id}/providers/Microsoft.DBforPostgreSQL/flexibleServers"
        params = {"api-version": API_VERSIONS["postgres_flexible"]}
        return self._get_paginated_results(url, params)

    def postgres_flexible_list_databases(self, resource_group: str, server_name: str) -> List[Dict[str, Any]]:
        """List all databases in a PostgreSQL Flexible Server."""
        url = (
            f"{AZURE_MANAGEMENT_URL}/subscriptions/{self._subscription_id}"
            f"/resourceGroups/{resource_group}"
            f"/providers/Microsoft.DBforPostgreSQL/flexibleServers/{server_name}/databases"
        )
        params = {"api-version": API_VERSIONS["postgres_flexible"]}
        return self._get_paginated_results(url, params)

    # ==================== Redis Cache APIs ====================

    def redis_list_caches(self) -> List[Dict[str, Any]]:
        """List all Redis Caches in the subscription."""
        url = f"{AZURE_MANAGEMENT_URL}/subscriptions/{self._subscription_id}/providers/Microsoft.Cache/redis"
        params = {"api-version": API_VERSIONS["redis"]}
        return self._get_paginated_results(url, params)

    # ==================== Monitor APIs ====================

    def monitor_list_metric_alerts(self) -> List[Dict[str, Any]]:
        """List all metric alerts in the subscription."""
        url = f"{AZURE_MANAGEMENT_URL}/subscriptions/{self._subscription_id}/providers/Microsoft.Insights/metricAlerts"
        params = {"api-version": API_VERSIONS["monitor_alerts"]}
        return self._get_paginated_results(url, params)

    def monitor_list_action_groups(self) -> List[Dict[str, Any]]:
        """List all action groups in the subscription."""
        url = f"{AZURE_MANAGEMENT_URL}/subscriptions/{self._subscription_id}/providers/microsoft.insights/actionGroups"
        params = {"api-version": API_VERSIONS["action_groups"]}
        return self._get_paginated_results(url, params)

    def monitor_list_metric_definitions(self, resource_id: str) -> List[Dict[str, Any]]:
        """List available metrics for a resource."""
        url = f"{AZURE_MANAGEMENT_URL}{resource_id}/providers/microsoft.insights/metricDefinitions"
        params = {"api-version": API_VERSIONS["metrics"]}
        return self._get_paginated_results(url, params)

    def monitor_query_metrics(
        self,
        resource_id: str,
        metric_names: List[str],
        start_time: str,
        end_time: str,
        aggregation: str = "Average",
        interval: str = "PT5M"
    ) -> Dict[str, Any]:
        """
        Query metrics for a resource.

        Args:
            resource_id: Full Azure resource ID
            metric_names: List of metric names to query
            start_time: ISO 8601 formatted start time
            end_time: ISO 8601 formatted end time
            aggregation: Aggregation type (Average, Total, Maximum, Minimum, Count)
            interval: Time granularity in ISO 8601 duration format (e.g., PT5M, PT1H)
        """
        url = f"{AZURE_MANAGEMENT_URL}{resource_id}/providers/microsoft.insights/metrics"
        params = {
            "api-version": API_VERSIONS["metrics"],
            "metricnames": ",".join(metric_names),
            "timespan": f"{start_time}/{end_time}",
            "aggregation": aggregation,
            "interval": interval
        }
        return self._make_request("GET", url, params)

    # ==================== Log Analytics APIs ====================

    def query_log_analytics(self, workspace_id: str, query: str, timespan: str = "PT4H") -> Dict[str, Any]:
        """
        Query a Log Analytics workspace.

        Args:
            workspace_id: The workspace's customer ID (GUID)
            query: KQL query string
            timespan: ISO 8601 duration format (e.g., PT4H for 4 hours)

        Note: Uses a different endpoint (api.loganalytics.io) and requires
        a separate scope for authentication.
        """
        # Log Analytics uses a different endpoint and scope
        # We need to get a separate token for Log Analytics
        token = self._get_log_analytics_token()

        url = f"{AZURE_LOG_ANALYTICS_URL}/workspaces/{workspace_id}/query"

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

        body = {
            "query": query,
            "timespan": timespan
        }

        try:
            response = self._session.post(
                url,
                json=body,
                headers=headers,
                timeout=self._timeout
            )

            if response.status_code == 200:
                return response.json()

            error_data = response.json() if response.text else {}
            error_msg = error_data.get("error", {}).get("message", response.text)
            raise AzureAPIError(response.status_code, error_msg, error_data)

        except requests.RequestException as e:
            raise AzureAPIError(0, f"Network error: {e}")

    def _get_log_analytics_token(self) -> str:
        """Get access token for Log Analytics API (different scope)."""
        token_url = f"{AZURE_LOGIN_URL}/{self._tenant_id}/oauth2/v2.0/token"

        data = {
            "grant_type": "client_credentials",
            "client_id": self._client_id,
            "client_secret": self._client_secret,
            "scope": "https://api.loganalytics.io/.default"
        }

        try:
            response = self._session.post(
                token_url,
                data=data,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=self._timeout
            )

            if response.status_code != 200:
                error_data = response.json() if response.text else {}
                error_msg = error_data.get("error_description", response.text)
                raise AzureAuthError(f"Failed to obtain Log Analytics token: {error_msg}")

            return response.json()["access_token"]

        except requests.RequestException as e:
            raise AzureAuthError(f"Network error during Log Analytics token acquisition: {e}")

    def close(self):
        """Close the HTTP session."""
        self._session.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
