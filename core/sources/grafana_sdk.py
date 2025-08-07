"""
Grafana-specific SDK implementation
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime

from drdroid_debug_toolkit.exceptions import TaskExecutionError
from drdroid_debug_toolkit.core.protos.base_pb2 import Source, SourceKeyType
from drdroid_debug_toolkit.core.integrations.source_managers.grafana_source_manager import GrafanaSourceManager
from drdroid_debug_toolkit.core.utils.proto_utils import proto_to_dict
from drdroid_debug_toolkit.core.sdk_base import BaseSDK

logger = logging.getLogger(__name__)


class GrafanaSDK(BaseSDK):
    """
    Grafana-specific SDK implementation
    Provides high-level methods for Grafana operations
    """
    
    def _initialize_source_managers(self) -> Dict[str, Any]:
        """Initialize Grafana source manager"""
        managers = {}
        
        if 'grafana' in self.credentials:
            managers['grafana'] = GrafanaSourceManager()
            
        return managers
    
    def _get_key_type_mapping(self) -> Dict[str, SourceKeyType]:
        """Get Grafana-specific key type mapping"""
        return {
            'grafana_host': SourceKeyType.GRAFANA_HOST,
            'grafana_api_key': SourceKeyType.GRAFANA_API_KEY,
            'ssl_verify': SourceKeyType.SSL_VERIFY,
        }
    
    def query_prometheus(self, 
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
            from ..protos.playbooks.source_task_definitions.grafana_task_pb2 import Grafana
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
            
            return proto_to_dict(result)
            
        except Exception as e:
            raise TaskExecutionError(f"Grafana Prometheus query failed: {e}")
    
    def query_dashboard_panel(self,
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
            from ..protos.playbooks.source_task_definitions.grafana_task_pb2 import Grafana
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
            
            return proto_to_dict(result)
            
        except Exception as e:
            raise TaskExecutionError(f"Grafana dashboard panel query failed: {e}")
    
    def execute_all_dashboard_panels(self,
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
            from ..protos.playbooks.source_task_definitions.grafana_task_pb2 import Grafana
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
            
            return [proto_to_dict(result) for result in results]
            
        except Exception as e:
            raise TaskExecutionError(f"Grafana dashboard execution failed: {e}")
    
    def fetch_dashboard_variables(self, dashboard_uid: str) -> Dict[str, Any]:
        """
        Fetch variables from a Grafana dashboard
        
        Args:
            dashboard_uid: Dashboard UID
            
        Returns:
            Dashboard variables as dictionary
        """
        try:
            source_manager = self._get_source_manager('grafana')
            connector = self._get_connector('grafana')
            time_range = self._create_time_range()
            
            # Create task proto
            from ..protos.playbooks.source_task_definitions.grafana_task_pb2 import Grafana
            from google.protobuf.wrappers_pb2 import StringValue
            
            task = Grafana()
            task.type = Grafana.TaskType.FETCH_DASHBOARD_VARIABLES
            
            variables_task = task.fetch_dashboard_variables
            variables_task.dashboard_uid.CopyFrom(StringValue(value=dashboard_uid))
            
            # Execute task
            result = source_manager.execute_fetch_dashboard_variables(
                time_range, task, connector
            )
            
            return proto_to_dict(result)
            
        except Exception as e:
            raise TaskExecutionError(f"Grafana dashboard variables fetch failed: {e}")
