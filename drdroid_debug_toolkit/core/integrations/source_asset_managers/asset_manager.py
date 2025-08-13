# -*- coding: utf-8 -*-
"""Base Source Asset Manager class."""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional

from core.protos.base_pb2 import Source, SourceModelType
from core.protos.assets.asset_pb2 import AccountConnectorAssets, AccountConnectorAssetsModelFilters
from core.protos.connectors.connector_pb2 import Connector as ConnectorProto


class SourceAssetManager(ABC):
    """Base class for source-specific asset managers."""
    
    def __init__(self):
        self.source = None
        self.asset_type_callable_map = {}
    
    @abstractmethod
    def get_asset_options(self, model_type: SourceModelType, raw_data: Dict[str, Any]):
        """Get asset options for a specific model type."""
        pass
    
    @abstractmethod
    def get_asset_values(self, connector: ConnectorProto, 
                        filters: AccountConnectorAssetsModelFilters,
                        model_type: SourceModelType,
                        raw_data: Dict[str, Any]) -> AccountConnectorAssets:
        """Get asset values for a specific model type."""
        pass
    
    def get_asset_type_callable_map(self) -> Dict[SourceModelType, Dict[str, Any]]:
        """Get the asset type callable map."""
        return self.asset_type_callable_map 