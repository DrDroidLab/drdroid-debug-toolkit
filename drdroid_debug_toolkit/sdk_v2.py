"""
DroidSpace SDK v2 - Improved modular implementation
"""

import logging
from typing import Dict, Any, List, Union

from drdroid_debug_toolkit.exceptions import ConfigurationError, ConnectionError, ValidationError, TaskExecutionError
from drdroid_debug_toolkit.core.sdk_factory import SDKFactory
from drdroid_debug_toolkit.core.sources.grafana_sdk import GrafanaSDK
from drdroid_debug_toolkit.core.sources.signoz_sdk import SignozSDK

logger = logging.getLogger(__name__)


class DroidSDK:
    """
    Main SDK class for DroidSpace integrations
    Uses factory pattern to manage multiple source-specific SDKs
    Provides dynamic access to source-specific SDKs
    """
    
    def __init__(self, credentials_file_path: str):
        """
        Initialize the SDK with credentials file path
        
        Args:
            credentials_file_path: Path to the YAML credentials file
        """
        self.factory = SDKFactory(credentials_file_path)
    
    # ============================================================================
    # Connection Management
    # ============================================================================
    
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
            sdk = self.factory.get_sdk(source_name)
            return sdk.test_connection(source_name)
        except Exception as e:
            raise ConnectionError(f"Connection test failed for {source_name}: {e}")
    
    def test_all_connections(self) -> Dict[str, bool]:
        """
        Test connections for all configured sources
        
        Returns:
            Dictionary mapping source names to connection status
        """
        return self.factory.test_all_connections()
    
    # ============================================================================
    # Source Information
    # ============================================================================
    
    def get_supported_sources(self) -> List[str]:
        """Get list of supported sources"""
        return self.factory.get_available_sources()
    
    def get_configured_sources(self) -> List[str]:
        """Get list of configured sources"""
        # Get from any SDK instance since they all have the same credentials
        if self.factory._sdk_instances:
            first_sdk = next(iter(self.factory._sdk_instances.values()))
            return first_sdk.get_configured_sources()
        return []
    
    def get_sdk_info(self) -> Dict[str, Dict[str, Any]]:
        """
        Get information about all available SDKs
        
        Returns:
            Dictionary with SDK information
        """
        return self.factory.get_sdk_info()
    
    # ============================================================================
    # Dynamic SDK Access
    # ============================================================================
    
    def get_sdk(self, source_name: str) -> Union[GrafanaSDK, SignozSDK]:
        """
        Get source-specific SDK by name
        
        Args:
            source_name: Name of the source (e.g., 'grafana', 'signoz')
            
        Returns:
            Source-specific SDK instance
            
        Raises:
            ValidationError: If source is not supported or not configured
        """
        return self.factory.get_sdk(source_name)
    
    def __getattr__(self, name: str):
        """
        Dynamic attribute access for source-specific SDKs
        Allows accessing SDKs as attributes (e.g., sdk.grafana, sdk.signoz)
        
        Args:
            name: Attribute name (should be a source name)
            
        Returns:
            Source-specific SDK instance
            
        Raises:
            AttributeError: If source is not supported or not configured
        """
        try:
            return self.get_sdk(name)
        except ValidationError as e:
            raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}': {e}")
    
    # ============================================================================
    # Convenience Methods (Optional - for backward compatibility)
    # ============================================================================
    
    def get_grafana_sdk(self) -> GrafanaSDK:
        """Get Grafana SDK instance (convenience method)"""
        return self.get_sdk('grafana')
    
    def get_signoz_sdk(self) -> SignozSDK:
        """Get Signoz SDK instance (convenience method)"""
        return self.get_sdk('signoz')
    
    # ============================================================================
    # Generic Task Execution (Advanced Usage)
    # ============================================================================
    
    def execute_task(self, source_name: str, task_name: str, **kwargs) -> Any:
        """
        Execute a task on a specific source dynamically
        
        Args:
            source_name: Name of the source (e.g., 'grafana', 'signoz')
            task_name: Name of the task method to execute
            **kwargs: Arguments to pass to the task method
            
        Returns:
            Task execution result
            
        Raises:
            ValidationError: If source or task is not found
            TaskExecutionError: If task execution fails
        """
        try:
            sdk = self.get_sdk(source_name)
            
            if not hasattr(sdk, task_name):
                available_methods = [method for method in dir(sdk) 
                                   if not method.startswith('_') and callable(getattr(sdk, method))]
                raise ValidationError(
                    f"Task '{task_name}' not found in {source_name} SDK. "
                    f"Available methods: {available_methods}"
                )
            
            method = getattr(sdk, task_name)
            return method(**kwargs)
            
        except ValidationError:
            raise
        except Exception as e:
            raise TaskExecutionError(f"Task execution failed: {e}")
    
    def list_available_tasks(self, source_name: str) -> List[str]:
        """
        List available tasks for a specific source
        
        Args:
            source_name: Name of the source
            
        Returns:
            List of available task method names
        """
        try:
            sdk = self.get_sdk(source_name)
            return [method for method in dir(sdk) 
                   if not method.startswith('_') and callable(getattr(sdk, method))]
        except Exception as e:
            logger.error(f"Failed to list tasks for {source_name}: {e}")
            return [] 