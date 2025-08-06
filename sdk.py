"""
DroidSpace SDK - Main SDK implementation
"""

import os
import yaml
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta

from exceptions import ConfigurationError, ConnectionError, ValidationError, TaskExecutionError
from core.protos.base_pb2 import Source, TimeRange, SourceKeyType
from core.protos.connectors.connector_pb2 import Connector, ConnectorKey
from core.protos.playbooks.playbook_commons_pb2 import PlaybookTaskResult
from core.integrations.source_manager import SourceManager
from core.integrations.source_manangers.grafana_source_manager import GrafanaSourceManager
from core.integrations.source_manangers.signoz_source_manager import SignozSourceManager
from core.utils.credentilal_utils import generate_credentials_dict

logger = logging.getLogger(__name__)


class DroidSDK:
    """
    Main SDK class for DroidSpace integrations
    """
    
    def __init__(self, credentials_file_path: str):
        """
        Initialize the SDK with credentials file path
        
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
    
    def _initialize_source_managers(self) -> Dict[str, SourceManager]:
        """Initialize source managers for supported integrations"""
        managers = {}
        
        # Initialize Grafana source manager
        if 'grafana' in self.credentials:
            managers['grafana'] = GrafanaSourceManager()
            
        # Initialize Signoz source manager
        if 'signoz' in self.credentials:
            managers['signoz'] = SignozSourceManager()
            
        return managers
    
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
            key_type_map = {
                'grafana_host': SourceKeyType.GRAFANA_HOST,
                'grafana_api_key': SourceKeyType.GRAFANA_API_KEY,
                'ssl_verify': SourceKeyType.SSL_VERIFY,
                'signoz_api_url': SourceKeyType.SIGNOZ_API_URL,
                'signoz_api_token': SourceKeyType.SIGNOZ_API_TOKEN,
            }
            
            if key_name in key_type_map:
                connector_key = ConnectorKey()
                connector_key.key_type = key_type_map[key_name]
                connector_key.key.value = str(key_value)
                connector.keys.append(connector_key)
        
        return connector
    
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
    
    def grafana_query_prometheus(self, 
                               datasource_uid: str,
                               query: str,
                               start_time: Optional[datetime] = None,
                               end_time: Optional[datetime] = None,
                               duration_minutes: Optional[int] = None,
                               interval: Optional[int] = None,
                               query_type: str = "PromQL") -> Dict[str, Any]:
        """
        Execute a Prometheus query via Grafana
        
        Args:
            datasource_uid: Grafana datasource UID
            query: PromQL query expression
            start_time: Start time for the query
            end_time: End time for the query
            duration_minutes: Duration in minutes (used if start_time not provided)
            interval: Step interval in seconds
            query_type: Query type ('PromQL' or 'Flux')
            
        Returns:
            Query results as dictionary
        """
        try:
            source_manager = self._get_source_manager('grafana')
            connector = self._get_connector('grafana')
            time_range = self._create_time_range(start_time, end_time, duration_minutes)
            
            # Create task proto
            from drdroid_sdk.core.protos.playbooks.source_task_definitions.grafana_task_pb2 import Grafana
            from drdroid_sdk.core.protos.literal_pb2 import Literal, LiteralType
            from google.protobuf.wrappers_pb2 import StringValue, UInt64Value
            
            task = Grafana()
            task.type = Grafana.TaskType.PROMETHEUS_DATASOURCE_METRIC_EXECUTION
            
            prometheus_task = task.prometheus_datasource_metric_execution
            prometheus_task.datasource_uid.CopyFrom(StringValue(value=datasource_uid))
            prometheus_task.promql_expression.CopyFrom(StringValue(value=query))
            prometheus_task.query_type.CopyFrom(StringValue(value=query_type))
            
            if interval:
                prometheus_task.interval.CopyFrom(UInt64Value(value=interval))
            
            # Execute task
            result = source_manager.execute_prometheus_datasource_metric_execution(
                time_range, task, connector
            )
            # Convert result to dictionary
            from drdroid_sdk.core.utils.proto_utils import proto_to_dict
            return proto_to_dict(result)
            
        except Exception as e:
            raise TaskExecutionError(f"Grafana Prometheus query failed: {e}")
    
    def grafana_query_dashboard_panel(self,
                                    dashboard_id: str,
                                    panel_id: str,
                                    datasource_uid: str,
                                    queries: List[str],
                                    start_time: Optional[datetime] = None,
                                    end_time: Optional[datetime] = None,
                                    duration_minutes: Optional[int] = None) -> Dict[str, Any]:
        """
        Query a specific panel in a Grafana dashboard
        
        Args:
            dashboard_id: Dashboard ID
            panel_id: Panel ID
            datasource_uid: Datasource UID
            queries: List of query expressions
            start_time: Start time for the query
            end_time: End time for the query
            duration_minutes: Duration in minutes
            
        Returns:
            Query results as dictionary
        """
        try:
            source_manager = self._get_source_manager('grafana')
            connector = self._get_connector('grafana')
            time_range = self._create_time_range(start_time, end_time, duration_minutes)
            
            # Create task proto
            from drdroid_sdk.core.protos.playbooks.source_task_definitions.grafana_task_pb2 import Grafana
            from drdroid_sdk.core.protos.literal_pb2 import Literal, LiteralType
            from google.protobuf.wrappers_pb2 import StringValue
            
            task = Grafana()
            task.type = Grafana.TaskType.QUERY_DASHBOARD_PANEL_METRIC
            
            panel_task = task.query_dashboard_panel_metric
            panel_task.dashboard_id.CopyFrom(StringValue(value=dashboard_id))
            panel_task.panel_id.CopyFrom(StringValue(value=panel_id))
            panel_task.datasource_uid.CopyFrom(StringValue(value=datasource_uid))
            
            # Add queries
            for query in queries:
                query_obj = panel_task.queries.add()
                query_obj.expr.CopyFrom(StringValue(value=query))
            
            # Execute task
            result = source_manager.execute_query_dashboard_panel_metric_execution(
                time_range, task, connector
            )
            # Convert result to dictionary
            from drdroid_sdk.core.utils.proto_utils import proto_to_dict
            return proto_to_dict(result)
            
        except Exception as e:
            raise TaskExecutionError(f"Grafana dashboard panel query failed: {e}")
    
    def grafana_execute_all_dashboard_panels(self,
                                           dashboard_uid: str,
                                           start_time: Optional[datetime] = None,
                                           end_time: Optional[datetime] = None,
                                           duration_minutes: Optional[int] = None,
                                           interval: Optional[int] = None,
                                           panel_ids: Optional[List[str]] = None,
                                           template_variables: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Execute all panels in a Grafana dashboard
        
        Args:
            dashboard_uid: Dashboard UID
            start_time: Start time for the query
            end_time: End time for the query
            duration_minutes: Duration in minutes
            interval: Step interval in seconds
            panel_ids: Optional list of panel IDs to execute
            template_variables: Optional template variables
            
        Returns:
            List of query results as dictionaries
        """
        try:
            source_manager = self._get_source_manager('grafana')
            connector = self._get_connector('grafana')
            time_range = self._create_time_range(start_time, end_time, duration_minutes)
            
            # Create task proto
            from drdroid_sdk.core.protos.playbooks.source_task_definitions.grafana_task_pb2 import Grafana
            from drdroid_sdk.core.protos.literal_pb2 import Literal, LiteralType
            from google.protobuf.wrappers_pb2 import StringValue, UInt64Value
            import json
            
            task = Grafana()
            task.type = Grafana.TaskType.EXECUTE_ALL_DASHBOARD_PANELS
            
            dashboard_task = task.execute_all_dashboard_panels
            dashboard_task.dashboard_uid.CopyFrom(StringValue(value=dashboard_uid))
            
            if interval:
                dashboard_task.interval.CopyFrom(UInt64Value(value=interval))
            
            if panel_ids:
                panel_ids_str = ",".join(panel_ids)
                dashboard_task.panel_ids.CopyFrom(StringValue(value=panel_ids_str))
            
            if template_variables:
                template_vars_json = json.dumps(template_variables)
                dashboard_task.template_variables.CopyFrom(StringValue(value=template_vars_json))
            
            # Execute task
            results = source_manager.execute_all_dashboard_panels(
                time_range, task, connector
            )
            print(results)
            # Convert results to dictionaries
            from drdroid_sdk.core.utils.proto_utils import proto_to_dict
            return [proto_to_dict(result) for result in results]
            
        except Exception as e:
            raise TaskExecutionError(f"Grafana dashboard execution failed: {e}")
    
    def signoz_clickhouse_query(self,
                               query: str,
                               start_time: Optional[datetime] = None,
                               end_time: Optional[datetime] = None,
                               duration_minutes: Optional[int] = None,
                               step: Optional[int] = None,
                               fill_gaps: bool = False,
                               panel_type: str = "table") -> Dict[str, Any]:
        """
        Execute a Clickhouse query via Signoz
        
        Args:
            query: Clickhouse SQL query
            start_time: Start time for the query
            end_time: End time for the query
            duration_minutes: Duration in minutes
            step: Step interval in seconds
            fill_gaps: Whether to fill gaps in time series
            panel_type: Panel type ('table', 'graph', 'value')
            
        Returns:
            Query results as dictionary
        """
        try:
            source_manager = self._get_source_manager('signoz')
            connector = self._get_connector('signoz')
            time_range = self._create_time_range(start_time, end_time, duration_minutes)
            
            # Create task proto
            from drdroid_sdk.core.protos.playbooks.source_task_definitions.signoz_task_pb2 import Signoz
            from drdroid_sdk.core.protos.literal_pb2 import Literal, LiteralType
            from google.protobuf.wrappers_pb2 import StringValue, Int32Value, BoolValue
            
            task = Signoz()
            task.type = Signoz.TaskType.CLICKHOUSE_QUERY
            
            clickhouse_task = task.clickhouse_query
            clickhouse_task.query.CopyFrom(StringValue(value=query))
            clickhouse_task.fill_gaps.CopyFrom(BoolValue(value=fill_gaps))
            clickhouse_task.panel_type.CopyFrom(StringValue(value=panel_type))
            
            if step:
                clickhouse_task.step.CopyFrom(Int32Value(value=step))
            
            # Execute task
            result = source_manager.execute_clickhouse_query(
                time_range, task, connector
            )
            
            # Handle case where result might be None
            if result is None:
                return {"error": "No data returned from Signoz query"}
            
            # Convert result to dictionary
            from drdroid_sdk.core.utils.proto_utils import proto_to_dict
            return proto_to_dict(result)
            
        except Exception as e:
            raise TaskExecutionError(f"Signoz Clickhouse query failed: {e}")
    
    def signoz_builder_query(self,
                            builder_queries: Dict[str, Any],
                            start_time: Optional[datetime] = None,
                            end_time: Optional[datetime] = None,
                            duration_minutes: Optional[int] = None,
                            step: Optional[int] = None,
                            panel_type: str = "table") -> Dict[str, Any]:
        """
        Execute a builder query via Signoz
        
        Args:
            builder_queries: Builder queries configuration
            start_time: Start time for the query
            end_time: End time for the query
            duration_minutes: Duration in minutes
            step: Step interval in seconds
            panel_type: Panel type ('table', 'graph', 'value')
            
        Returns:
            Query results as dictionary
        """
        try:
            source_manager = self._get_source_manager('signoz')
            connector = self._get_connector('signoz')
            time_range = self._create_time_range(start_time, end_time, duration_minutes)
            
            # Create task proto
            from drdroid_sdk.core.protos.playbooks.source_task_definitions.signoz_task_pb2 import Signoz
            from drdroid_sdk.core.protos.literal_pb2 import Literal, LiteralType
            from google.protobuf.wrappers_pb2 import StringValue, Int32Value
            import json
            
            task = Signoz()
            task.type = Signoz.TaskType.BUILDER_QUERY
            
            builder_task = task.builder_query
            builder_task.builder_queries.CopyFrom(StringValue(value=json.dumps(builder_queries)))
            builder_task.panel_type.CopyFrom(StringValue(value=panel_type))
            
            if step:
                builder_task.step.CopyFrom(Int32Value(value=step))
            
            # Execute task
            result = source_manager.execute_builder_query(
                time_range, task, connector
            )
            
            # Handle case where result might be None
            if result is None:
                return {"error": "No data returned from Signoz builder query"}
            
            # Convert result to dictionary
            from drdroid_sdk.core.utils.proto_utils import proto_to_dict
            return proto_to_dict(result)
            
        except Exception as e:
            raise TaskExecutionError(f"Signoz builder query failed: {e}")
    
    def signoz_dashboard_data(self,
                             dashboard_name: str,
                             start_time: Optional[datetime] = None,
                             end_time: Optional[datetime] = None,
                             duration_minutes: Optional[int] = None,
                             step: Optional[int] = None,
                             variables: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Get data from a Signoz dashboard
        
        Args:
            dashboard_name: Name of the dashboard
            start_time: Start time for the query
            end_time: End time for the query
            duration_minutes: Duration in minutes
            step: Step interval in seconds
            variables: Optional variables for the dashboard
            
        Returns:
            List of query results as dictionaries
        """
        try:
            source_manager = self._get_source_manager('signoz')
            connector = self._get_connector('signoz')
            time_range = self._create_time_range(start_time, end_time, duration_minutes)
            
            # Create task proto
            from drdroid_sdk.core.protos.playbooks.source_task_definitions.signoz_task_pb2 import Signoz
            from drdroid_sdk.core.protos.literal_pb2 import Literal, LiteralType
            from google.protobuf.wrappers_pb2 import StringValue, Int32Value
            import json
            
            task = Signoz()
            task.type = Signoz.TaskType.DASHBOARD_DATA
            
            dashboard_task = task.dashboard_data
            dashboard_task.dashboard_name.CopyFrom(StringValue(value=dashboard_name))
            
            if step:
                dashboard_task.step.CopyFrom(Int32Value(value=step))
            
            if variables:
                variables_json = json.dumps(variables)
                dashboard_task.variables_json.CopyFrom(StringValue(value=variables_json))
            
            # Execute task
            results = source_manager.execute_dashboard_data(
                time_range, task, connector
            )
            
            # Handle case where results might be None or empty
            if not results:
                return [{"error": "No data returned from Signoz dashboard query"}]
            
            # Convert results to dictionaries
            from drdroid_sdk.core.utils.proto_utils import proto_to_dict
            return [proto_to_dict(result) for result in results]
            
        except Exception as e:
            raise TaskExecutionError(f"Signoz dashboard data query failed: {e}")
    
    def get_supported_sources(self) -> List[str]:
        """Get list of supported sources"""
        return list(self.source_managers.keys())
    
    def get_configured_sources(self) -> List[str]:
        """Get list of configured sources"""
        return list(self.credentials.keys()) 