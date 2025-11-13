from typing import Dict, Any, Optional
import requests
from requests.exceptions import RequestException
from django.conf import settings
from core.protos.base_pb2 import SourceModelType
from core.protos.assets.asset_pb2 import AccountConnectorAssets
from core.utils.proto_utils import dict_to_proto

IS_PROD_ENV = False

try:
    IS_PROD_ENV = settings.IS_PROD_ENV
except Exception as e:
    pass

try:
    MASTER_ASSETS_TOKEN = settings.MASTER_ASSETS_TOKEN
except Exception as e:
    MASTER_ASSETS_TOKEN = None


if IS_PROD_ENV:
    API_HOST = "http://localhost:8080"
    API_TOKEN = MASTER_ASSETS_TOKEN
else:
    API_HOST = "https://agent-api.drdroid.io"
    API_TOKEN = settings.DRD_CLOUD_API_TOKEN

class PrototypeClient:
    """
    Client for interacting with the DrDroid Platform.
    
    This client provides methods to interact with various DrDroid Platform APIs
    in a clean and type-safe manner.
    """

    def __init__(self, api_token: str = None, api_host: str = None):
        """
        Initialize the client.
        
        Args:
            api_token: The API token for authentication
            api_host: The API host URL
        """
        self.auth_token = api_token or API_TOKEN
        self.base_url = api_host or API_HOST
        
        if not self.auth_token or not self.base_url:
            raise ValueError("API token and API host must be provided")

    def _get_headers(self) -> Dict[str, str]:
        """Get the default headers for API requests."""
        return {
            'content-type': 'application/json',
            'Authorization': f'Bearer {self.auth_token}',
        }
    
    def _get_asset_url(self) -> str:
        """Get the asset URL for the API request."""
        if IS_PROD_ENV:
            return f"{self.base_url}/connectors/master/assets/models/get"
        else:
            return f"{self.base_url}/connectors/proxy/assets/models/get"

    def get_connector_assets(
        self,
        connector_type: str,
        connector_id: str,
        asset_type: SourceModelType,
        filters: Optional[Dict[str, Any]] = None,
        page_size: Optional[int] = None
    ) -> AccountConnectorAssets:
        """
        Retrieve connector assets based on specified parameters.

        Args:
            connector_type (str): Type of the connector (e.g., 'CLOUDWATCH')
            connector_id (str): ID of the connector

        Returns:
            Dict[str, Any]: Response data from the API

        Raises:
            Exception: If the API request fails
        """
        payload = {
            "connector_type": connector_type,
            "connector_id": connector_id,
            "type": asset_type,
        }

        if filters:
            payload["filters"] = filters

        # Handle pagination: loop through pages and merge assets
        current_page = 1
        total_pages = 1
        merged_assets_proto: Optional[AccountConnectorAssets] = None

        try:
            while current_page <= total_pages:
                page_payload = dict(payload)
                page_payload["current_page"] = current_page
                if page_size:
                    page_payload["page_size"] = page_size

                response = requests.post(
                    self._get_asset_url(),
                    json=page_payload,
                    headers=self._get_headers()
                )
                response.raise_for_status()
                resp_json = response.json()

                # Update total pages (default to 1 if not present)
                total_pages = int(resp_json.get("total_pages", total_pages)) or 1

                # Extract assets list from this page
                assets_list = resp_json.get("assets", []) or []
                if not assets_list:
                    # No assets on this page; continue to next page
                    current_page += 1
                    continue

                # Convert first (and expected only) AccountConnectorAssets entry to proto
                page_assets_proto = self.post_process_assets({"assets": assets_list})

                if merged_assets_proto is None:
                    merged_assets_proto = page_assets_proto
                else:
                    self._merge_account_connector_assets(merged_assets_proto, page_assets_proto)

                current_page += 1

            # If nothing was merged, return an empty structure consistent with proto
            if merged_assets_proto is None:
                # Return an empty container for the requested connector type
                merged_assets_proto = AccountConnectorAssets()

            return merged_assets_proto

        except RequestException as e:
            raise Exception(f"Failed to get connector assets: {str(e)}") from e
        except Exception as e:
            raise Exception(f"Failed to get connector assets: {str(e)}") from e
    
    def post_process_assets(self, assets: Dict[str, Any]) -> AccountConnectorAssets:
        """
        Post-process the assets to ensure they are in the correct format.
        """
        return dict_to_proto(assets['assets'][0], AccountConnectorAssets)

    def _merge_account_connector_assets(self, base: AccountConnectorAssets, other: AccountConnectorAssets) -> None:
        """Merge assets from 'other' into 'base' respecting the oneof type.

        This appends asset lists inside the active oneof field (e.g., new_relic.assets).
        """
        # Determine which oneof is set by checking attributes
        # Order matters less; check all known oneof names present in the proto
        oneof_fields = [
            "cloudwatch", "grafana", "clickhouse", "slack", "new_relic", "datadog",
            "postgres", "eks", "bash", "azure", "gke", "elastic_search", "gcm",
            "datadog_oauth", "open_search", "asana", "github", "jira_cloud", "argocd",
            "jenkins", "mongodb", "posthog", "sql", "signoz", "coralogix"
        ]

        base_field = None
        for name in oneof_fields:
            if getattr(base, name, None):
                base_field = name
                break

        other_field = None
        for name in oneof_fields:
            if getattr(other, name, None):
                other_field = name
                break

        if not base_field or not other_field or base_field != other_field:
            # Nothing to merge or mismatched types
            return

        base_assets_container = getattr(base, base_field)
        other_assets_container = getattr(other, other_field)

        # Many assets containers have a repeated 'assets' field
        if hasattr(base_assets_container, "assets") and hasattr(other_assets_container, "assets"):
            base_assets_container.assets.extend(other_assets_container.assets)
        else:
            # If no 'assets' repeated field, there may be repeated lists under specific names; best-effort no-op
            pass
