"""
Signoz-specific SDK implementation
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime

from drdroid_sdk.exceptions import TaskExecutionError
from drdroid_sdk.core.protos.base_pb2 import Source, SourceKeyType
from drdroid_sdk.core.integrations.source_managers.signoz_source_manager import SignozSourceManager
from drdroid_sdk.core.utils.proto_utils import proto_to_dict
from drdroid_sdk.core.sdk_base import BaseSDK

logger = logging.getLogger(__name__)


class SignozSDK(BaseSDK):
    """
    Signoz-specific SDK implementation
    Provides high-level methods for Signoz operations
    """
    
    def _initialize_source_managers(self) -> Dict[str, Any]:
        """Initialize Signoz source manager"""
        managers = {}
        
        if 'signoz' in self.credentials:
            managers['signoz'] = SignozSourceManager()
            
        return managers
    
    def _get_key_type_mapping(self) -> Dict[str, SourceKeyType]:
        """Get Signoz-specific key type mapping"""
        return {
            'signoz_api_url': SourceKeyType.SIGNOZ_API_URL,
            'signoz_api_token': SourceKeyType.SIGNOZ_API_TOKEN,
        }
    
    def clickhouse_query(self,
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
            from ..protos.playbooks.source_task_definitions.signoz_task_pb2 import Signoz
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
            
            return proto_to_dict(result)
            
        except Exception as e:
            raise TaskExecutionError(f"Signoz Clickhouse query failed: {e}")
    
    def builder_query(self,
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
            from ..protos.playbooks.source_task_definitions.signoz_task_pb2 import Signoz
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
            
            return proto_to_dict(result)
            
        except Exception as e:
            raise TaskExecutionError(f"Signoz builder query failed: {e}")
    
    def dashboard_data(self,
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
            from ..protos.playbooks.source_task_definitions.signoz_task_pb2 import Signoz
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
            
            return [proto_to_dict(result) for result in results]
            
        except Exception as e:
            raise TaskExecutionError(f"Signoz dashboard data query failed: {e}")
    
    def fetch_services(self,
                      start_time: Optional[datetime] = None,
                      end_time: Optional[datetime] = None,
                      duration_minutes: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Fetch services from Signoz
        
        Args:
            start_time: Start time for the query
            end_time: End time for the query
            duration_minutes: Duration in minutes
            
        Returns:
            List of services as dictionaries
        """
        try:
            source_manager = self._get_source_manager('signoz')
            connector = self._get_connector('signoz')
            time_range = self._create_time_range(start_time, end_time, duration_minutes)
            
            # Create task proto
            from drdroid_sdk.core.protos.playbooks.source_task_definitions.signoz_task_pb2 import Signoz
            
            task = Signoz()
            task.type = Signoz.TaskType.FETCH_SERVICES
            
            # Execute task
            results = source_manager.execute_fetch_services(
                time_range, task, connector
            )
            
            return [proto_to_dict(result) for result in results]
            
        except Exception as e:
            raise TaskExecutionError(f"Signoz services fetch failed: {e}")
    
    def fetch_dashboards(self) -> List[Dict[str, Any]]:
        """
        Fetch all dashboards from Signoz
        
        Returns:
            List of dashboards as dictionaries
        """
        try:
            source_manager = self._get_source_manager('signoz')
            connector = self._get_connector('signoz')
            time_range = self._create_time_range()
            
            # Create task proto
            from drdroid_sdk.core.protos.playbooks.source_task_definitions.signoz_task_pb2 import Signoz
            
            task = Signoz()
            task.type = Signoz.TaskType.FETCH_DASHBOARDS
            
            # Execute task
            results = source_manager.execute_fetch_dashboards(
                time_range, task, connector
            )
            
            return [proto_to_dict(result) for result in results]
            
        except Exception as e:
            raise TaskExecutionError(f"Signoz dashboards fetch failed: {e}")
    
    def apm_metrics(self,
                   service_name: str,
                   start_time: Optional[datetime] = None,
                   end_time: Optional[datetime] = None,
                   duration_minutes: Optional[int] = None,
                   window: str = "5m",
                   operation_names: Optional[List[str]] = None,
                   metrics: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Fetch APM metrics for a service
        
        Args:
            service_name: Name of the service
            start_time: Start time for the query
            end_time: End time for the query
            duration_minutes: Duration in minutes
            window: Time window for aggregation
            operation_names: Optional list of operation names to filter by
            metrics: Optional list of metrics to fetch
            
        Returns:
            APM metrics as dictionary
        """
        try:
            source_manager = self._get_source_manager('signoz')
            connector = self._get_connector('signoz')
            time_range = self._create_time_range(start_time, end_time, duration_minutes)
            
            # Create task proto
            from drdroid_sdk.core.protos.playbooks.source_task_definitions.signoz_task_pb2 import Signoz
            from google.protobuf.wrappers_pb2 import StringValue
            import json
            
            task = Signoz()
            task.type = Signoz.TaskType.APM_METRICS
            
            apm_task = task.apm_metrics
            apm_task.service_name.CopyFrom(StringValue(value=service_name))
            apm_task.window.CopyFrom(StringValue(value=window))
            
            if operation_names:
                apm_task.operation_names.CopyFrom(StringValue(value=json.dumps(operation_names)))
            
            if metrics:
                apm_task.metrics.CopyFrom(StringValue(value=json.dumps(metrics)))
            
            # Execute task
            result = source_manager.execute_apm_metrics(
                time_range, task, connector
            )
            
            return proto_to_dict(result)
            
        except Exception as e:
            raise TaskExecutionError(f"Signoz APM metrics fetch failed: {e}") 