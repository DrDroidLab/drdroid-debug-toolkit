"""
SDK Factory for managing multiple source-specific SDKs
"""

import logging
from typing import Dict, Any, Optional, List, Type
from datetime import datetime

from .sdk_base import BaseSDK
from .sources.grafana_sdk import GrafanaSDK
from .sources.signoz_sdk import SignozSDK
from .sources.bash_sdk import BashSDK
from .sources.kubernetes_sdk import KubernetesSDK
from .sources.cloudwatch_sdk import CloudWatchSDK
from .sources.sentry_sdk import SentrySDK
from .sources.datadog_sdk import DatadogSDK
from .sources.newrelic_sdk import NewRelicSDK
from .sources.postgres_sdk import PostgresSDK
from .sources.posthog_sdk import PostHogSDK
from .sources.sql_database_connection_sdk import SqlDatabaseConnectionSDK
from .sources.clickhouse_sdk import ClickHouseSDK
from ..exceptions import ConfigurationError, ValidationError

logger = logging.getLogger(__name__)


class SDKFactory:
    """
    Factory class for creating and managing source-specific SDKs
    Provides a unified interface for accessing different integrations
    """
    
    # Registry of available SDKs
    _SDK_REGISTRY = {
        'grafana': GrafanaSDK,
        'signoz': SignozSDK,
        'bash': BashSDK,
        'kubernetes': KubernetesSDK,
        'cloudwatch': CloudWatchSDK,
        'sentry': SentrySDK,
        'datadog': DatadogSDK,
        'newrelic': NewRelicSDK,
        'postgres': PostgresSDK,
        'posthog': PostHogSDK,
        'sql_database_connection': SqlDatabaseConnectionSDK,
        'clickhouse': ClickHouseSDK,
    }
    
    def __init__(self, credentials_file_path: str):
        """
        Initialize the SDK factory
        
        Args:
            credentials_file_path: Path to the YAML credentials file
        """
        self.credentials_file_path = credentials_file_path
        self._sdk_instances: Dict[str, BaseSDK] = {}
        self._initialize_sdks()
    
    def _initialize_sdks(self):
        """Initialize SDK instances for configured sources"""
        try:
            # Load credentials to determine which SDKs to initialize
            import yaml
            with open(self.credentials_file_path, 'r') as f:
                credentials = yaml.safe_load(f)
            
            if not credentials:
                raise ConfigurationError("Credentials file is empty or invalid")
            
            # Initialize SDKs for each configured source
            for source_name in credentials.keys():
                if source_name in self._SDK_REGISTRY:
                    sdk_class = self._SDK_REGISTRY[source_name]
                    self._sdk_instances[source_name] = sdk_class(self.credentials_file_path)
                    logger.info(f"Initialized {source_name} SDK")
                else:
                    logger.warning(f"No SDK available for source: {source_name}")
                    
        except Exception as e:
            raise ConfigurationError(f"Failed to initialize SDKs: {e}")
    
    def get_sdk(self, source_name: str) -> BaseSDK:
        """
        Get SDK instance for the specified source
        
        Args:
            source_name: Name of the source (e.g., 'grafana', 'signoz')
            
        Returns:
            SDK instance for the specified source
            
        Raises:
            ValidationError: If source is not supported or not configured
        """
        source_name = source_name.lower()
        if source_name not in self._sdk_instances:
            available_sources = list(self._sdk_instances.keys())
            raise ValidationError(
                f"Source '{source_name}' not available. Available sources: {available_sources}"
            )
        return self._sdk_instances[source_name]
    
    def get_grafana_sdk(self) -> GrafanaSDK:
        """Get Grafana SDK instance"""
        return self.get_sdk('grafana')
    
    def get_signoz_sdk(self) -> SignozSDK:
        """Get Signoz SDK instance"""
        return self.get_sdk('signoz')
    
    def get_bash_sdk(self) -> BashSDK:
        """Get Bash SDK instance"""
        return self.get_sdk('bash')
    
    def get_kubernetes_sdk(self) -> KubernetesSDK:
        """Get Kubernetes SDK instance"""
        return self.get_sdk('kubernetes')
    
    def get_cloudwatch_sdk(self) -> CloudWatchSDK:
        """Get CloudWatch SDK instance"""
        return self.get_sdk('cloudwatch')
    
    def get_sentry_sdk(self) -> SentrySDK:
        """Get Sentry SDK instance"""
        return self.get_sdk('sentry')
    
    def get_datadog_sdk(self) -> DatadogSDK:
        """Get Datadog SDK instance"""
        return self.get_sdk('datadog')
    
    def get_newrelic_sdk(self) -> NewRelicSDK:
        """Get NewRelic SDK instance"""
        return self.get_sdk('newrelic')
    
    def get_postgres_sdk(self) -> PostgresSDK:
        """Get Postgres SDK instance"""
        return self.get_sdk('postgres')
    
    def get_posthog_sdk(self) -> PostHogSDK:
        """Get PostHog SDK instance"""
        return self.get_sdk('posthog')
    
    def get_sql_database_connection_sdk(self) -> SqlDatabaseConnectionSDK:
        """Get SQL Database Connection SDK instance"""
        return self.get_sdk('sql_database_connection')
    
    def get_clickhouse_sdk(self) -> ClickHouseSDK:
        """Get ClickHouse SDK instance"""
        return self.get_sdk('clickhouse')
    
    def get_available_sources(self) -> List[str]:
        """Get list of available source SDKs"""
        return list(self._sdk_instances.keys())
    
    def test_all_connections(self) -> Dict[str, bool]:
        """
        Test connections for all configured sources
        
        Returns:
            Dictionary mapping source names to connection status
        """
        results = {}
        for source_name, sdk in self._sdk_instances.items():
            try:
                results[source_name] = sdk.test_connection(source_name)
            except Exception as e:
                logger.error(f"Connection test failed for {source_name}: {e}")
                results[source_name] = False
        return results
    
    def register_sdk(self, source_name: str, sdk_class: Type[BaseSDK]):
        """
        Register a new SDK class for a source
        
        Args:
            source_name: Name of the source
            sdk_class: SDK class that extends BaseSDK
        """
        if not issubclass(sdk_class, BaseSDK):
            raise ValidationError(f"SDK class must extend BaseSDK")
        
        self._SDK_REGISTRY[source_name.lower()] = sdk_class
        logger.info(f"Registered SDK for source: {source_name}")
    
    def get_sdk_info(self) -> Dict[str, Dict[str, Any]]:
        """
        Get information about all available SDKs
        
        Returns:
            Dictionary with SDK information
        """
        info = {}
        for source_name, sdk in self._sdk_instances.items():
            info[source_name] = {
                'class': sdk.__class__.__name__,
                'supported_sources': sdk.get_supported_sources(),
                'configured_sources': sdk.get_configured_sources(),
            }
        return info 