from typing import Dict, Any
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
    API_HOST = "http://localhost:8080/api"
    API_TOKEN = MASTER_ASSETS_TOKEN
else:
    API_HOST = "https://agent-api.drdroid.io/api"
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
        filters: Dict[str, Any] = None
    ) -> Dict[str, Any]:
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

        try:
            response = requests.post(
                self._get_asset_url(),
                json=payload,
                headers=self._get_headers()
            )
            response.raise_for_status()
            return self.post_process_assets(response.json())

        except RequestException as e:
            raise Exception(f"Failed to get connector assets: {str(e)}") from e
        except Exception as e:
            raise Exception(f"Failed to get connector assets: {str(e)}") from e
    
    def post_process_assets(self, assets: Dict[str, Any]) -> Dict[str, Any]:
        """
        Post-process the assets to ensure they are in the correct format.
        """
        return dict_to_proto(assets['assets'][0], AccountConnectorAssets)
