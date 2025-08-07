import logging
from datetime import datetime, date

import requests

from core.protos.base_pb2 import Source
from core.utils.logging_utils import log_function_call

logger = logging.getLogger(__name__)


class SourceMetadataExtractor:

    def __init__(self, request_id: str, connector_name: str, source: Source, api_host: str = None, api_token: str = None):
        self.request_id = request_id
        self.connector_name = connector_name
        self.source = source
        self.api_host = api_host
        self.api_token = api_token

    @log_function_call
    def create_or_update_model_metadata(self, model_type, collected_models):
        try:
            if not self.api_host or not self.api_token:
                logger.warning("API host or token not provided, skipping metadata update")
                return
                
            drd_cloud_host = self.api_host
            drd_cloud_api_token = self.api_token
            asset_metadata_models = []
            for model_uid, metadata in collected_models.items():
                for k, v in metadata.items():
                    if isinstance(v, (datetime, date)):
                        metadata[k] = v.isoformat()
                asset_metadata_models.append({
                    'model_uid': model_uid,
                    'model_type': model_type,
                    'metadata': metadata
                })
                if len(asset_metadata_models) >= 100:
                    requests.post(f'{drd_cloud_host}/connectors/proxy/connector/metadata/register',
                                  headers={'Authorization': f'Bearer {drd_cloud_api_token}'},
                                  json={'connector': {'name': self.connector_name}, 'assets': asset_metadata_models})
                    asset_metadata_models = []
            if len(asset_metadata_models) > 0:
                requests.post(f'{drd_cloud_host}/connectors/proxy/connector/metadata/register',
                              headers={'Authorization': f'Bearer {drd_cloud_api_token}'},
                              json={'connector': {'name': self.connector_name}, 'assets': asset_metadata_models})
        except Exception as e:
            logger.error(f'Error creating or updating model_type: {model_type} with error: {e}')
