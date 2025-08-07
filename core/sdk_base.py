"""
Base SDK class providing common functionality for all integrations
"""

import os
import yaml
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from abc import ABC, abstractmethod

from drdroid_sdk.exceptions import ConfigurationError, ConnectionError, ValidationError, TaskExecutionError
from drdroid_sdk.core.protos.base_pb2 import Source, TimeRange, SourceKeyType
from drdroid_sdk.core.protos.connectors.connector_pb2 import Connector, ConnectorKey
from drdroid_sdk.core.protos.playbooks.playbook_commons_pb2 import PlaybookTaskResult
from drdroid_sdk.core.integrations.source_manager import SourceManager
from drdroid_sdk.core.utils.credentilal_utils import generate_credentials_dict

logger = logging.getLogger(__name__)


class BaseSDK(ABC):
    """
    Abstract base class for SDK functionality
    Provides common methods and enforces interface for source-specific SDKs
    """
    
    def __init__(self, credentials_file_path: str):
        """
        Initialize the base SDK with credentials file path
        
        Args:
            credentials_file_path: Path to the YAML credentials file
        """
        self.credentials_file_path = credentials_file_path
        self.credentials = self._load_credentials()
        self.source_managers = self._initialize_source_managers()
        
    def _load_credentials(self) -> Dict[str, Any]:
        """Load credentials from YAML file"""
        try:
            if not os.path.exists(self.credentials_file_path):
                raise ConfigurationError(f"Credentials file not found: {self.credentials_file_path}")
                
            with open(self.credentials_file_path, 'r') as f:
                credentials = yaml.safe_load(f)
                
            if not credentials:
                raise ConfigurationError("Credentials file is empty or invalid")
                
            return credentials
        except yaml.YAMLError as e:
            raise ConfigurationError(f"Invalid YAML in credentials file: {e}")
        except Exception as e:
            raise ConfigurationError(f"Error loading credentials: {e}")
    
    @abstractmethod
    def _initialize_source_managers(self) -> Dict[str, SourceManager]:
        """Initialize source managers for supported integrations"""
        pass
    
    def _create_connector_proto(self, source_name: str, connector_config: Dict[str, Any]) -> Connector:
        """Create a Connector proto from configuration"""
        connector = Connector()
        connector.id.value = connector_config.get('id', 1)
        connector.type = getattr(Source, source_name.upper())
        
        # Add connector keys
        for key_name, key_value in connector_config.items():
            if key_name in ['id', 'name']:
                continue
                
            # Map configuration keys to SourceKeyType
            key_type_map = self._get_key_type_mapping()
            
            if key_name in key_type_map:
                connector_key = ConnectorKey()
                connector_key.key_type = key_type_map[key_name]
                connector_key.key.value = str(key_value)
                connector.keys.append(connector_key)
        
        return connector
    
    @abstractmethod
    def _get_key_type_mapping(self) -> Dict[str, SourceKeyType]:
        """Get mapping of configuration keys to SourceKeyType"""
        pass
    
    def _create_time_range(self, start_time: Optional[datetime] = None, 
                          end_time: Optional[datetime] = None,
                          duration_minutes: Optional[int] = None) -> TimeRange:
        """Create a TimeRange proto"""
        time_range = TimeRange()
        
        if end_time is None:
            end_time = datetime.now()
        
        if start_time is None:
            if duration_minutes:
                start_time = end_time - timedelta(minutes=duration_minutes)
            else:
                start_time = end_time - timedelta(hours=1)  # Default to 1 hour
        
        time_range.time_geq = int(start_time.timestamp())
        time_range.time_lt = int(end_time.timestamp())
        
        return time_range
    
    def _get_source_manager(self, source_name: str) -> SourceManager:
        """Get source manager for the specified source"""
        source_name = source_name.lower()
        if source_name not in self.source_managers:
            raise ValidationError(f"Source '{source_name}' not supported or not configured")
        return self.source_managers[source_name]
    
    def _get_connector(self, source_name: str) -> Connector:
        """Get connector for the specified source"""
        source_name = source_name.lower()
        if source_name not in self.credentials:
            raise ConfigurationError(f"No credentials found for source '{source_name}'")
        
        connector_config = self.credentials[source_name]
        return self._create_connector_proto(source_name, connector_config)
    
    def test_connection(self, source_name: str) -> bool:
        """
        Test connection to the specified source
        
        Args:
            source_name: Name of the source (e.g., 'grafana', 'signoz')
            
        Returns:
            True if connection successful
            
        Raises:
            ConnectionError: If connection fails
        """
        try:
            source_manager = self._get_source_manager(source_name)
            connector = self._get_connector(source_name)
            
            return source_manager.test_connector_processor(connector)
        except Exception as e:
            raise ConnectionError(f"Connection test failed for {source_name}: {e}")
    
    def get_supported_sources(self) -> List[str]:
        """Get list of supported sources"""
        return list(self.source_managers.keys())
    
    def get_configured_sources(self) -> List[str]:
        """Get list of configured sources"""
        return list(self.credentials.keys()) 