import logging
import sys
from datetime import datetime, date

import requests

from core.protos.base_pb2 import Source
from core.utils.logging_utils import log_function_call

logger = logging.getLogger(__name__)

# Ensure a single module instance regardless of import path aliasing
_MODULE_ALIASES = [
    'core.integrations.source_metadata_extractor',
    'drdroid_debug_toolkit.core.integrations.source_metadata_extractor',
]
for _alias in _MODULE_ALIASES:
    if _alias not in sys.modules:
        sys.modules[_alias] = sys.modules[__name__]


# Dependency Injection registry for SourceMetadataExtractor base
_active_source_metadata_extractor_base_class = None


def set_source_metadata_extractor_base_class(cls):
    """
    Allows overriding the base class used for SourceMetadataExtractor.
    Must be called before importing modules that subclass SourceMetadataExtractor.
    """
    global _active_source_metadata_extractor_base_class
    _active_source_metadata_extractor_base_class = cls

    def _rebase_module(mod):
        try:
            sme_cls = getattr(mod, 'SourceMetadataExtractor', None)
            if isinstance(sme_cls, type) and sme_cls is not cls:
                sme_cls.__bases__ = (cls,)
        except Exception:
            # Best-effort: ignore if MRO update is not safe
            pass

    # Rebase in the current module
    _rebase_module(sys.modules.get(__name__))

    # Rebase in any known alias module instances (if both were imported separately)
    for _alias in _MODULE_ALIASES:
        mod = sys.modules.get(_alias)
        if mod is not None:
            _rebase_module(mod)


def get_source_metadata_extractor_base_class():
    """
    Returns the active base class for SourceMetadataExtractor. Falls back to the default
    implementation defined in this module when no override is provided.
    """
    return _active_source_metadata_extractor_base_class or _DefaultSourceMetadataExtractor


class _DefaultSourceMetadataExtractor:

    def __init__(self, request_id: str, connector_name: str, source: Source, api_host: str = None, api_token: str = None):
        self.request_id = request_id
        self.connector_name = connector_name
        self.source = source
        self.api_host = api_host
        self.api_token = api_token
        # Store collected data for real-time access
        self._collected_assets = {}

    @log_function_call
    def create_or_update_model_metadata(self, model_type, collected_models):
        try:
            # Store the collected models for real-time access
            self._collected_assets[model_type] = collected_models
            
            if not self.api_host or not self.api_token:
                logger.warning("API host or token not provided, skipping metadata update")
                return
                
            drd_cloud_host = self.api_host
            drd_cloud_api_token = self.api_token
            refresh_id = self.request_id  # Stable ID for this refresh; same for all batches of this run
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
                    requests.post(
                        f'{drd_cloud_host}/connectors/proxy/connector/metadata/register',
                        headers={'Authorization': f'Bearer {drd_cloud_api_token}'},
                        json={
                            'connector': {'name': self.connector_name},
                            'assets': asset_metadata_models,
                            'refresh_id': refresh_id,
                            'has_more': True,
                            'model_type': model_type,
                        },
                    )
                    asset_metadata_models = []
            if len(asset_metadata_models) > 0:
                requests.post(
                    f'{drd_cloud_host}/connectors/proxy/connector/metadata/register',
                    headers={'Authorization': f'Bearer {drd_cloud_api_token}'},
                    json={
                        'connector': {'name': self.connector_name},
                        'assets': asset_metadata_models,
                        'refresh_id': refresh_id,
                        'has_more': False,
                        'model_type': model_type,
                    },
                )
        except Exception as e:
            logger.error(f'Error creating or updating model_type: {model_type} with error: {e}')

    def get_collected_assets(self, model_type=None):
        """
        Get collected assets for real-time access.
        
        Args:
            model_type: Optional model type to filter by. If None, returns all collected assets.
            
        Returns:
            Dict containing the collected assets
        """
        if model_type is None:
            return self._collected_assets
        return self._collected_assets.get(model_type, {})

    def clear_collected_assets(self):
        """Clear all collected assets."""
        self._collected_assets = {}


# Exposed dynamic base for external subclasses
class SourceMetadataExtractor(get_source_metadata_extractor_base_class()):
    pass
