"""
Signoz-specific SDK implementation
"""

import json
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime

from ...exceptions import TaskExecutionError
from ..protos.base_pb2 import Source, SourceKeyType, TimeRange
from ..integrations.source_managers.signoz_source_manager import SignozSourceManager
from ..utils.proto_utils import proto_to_dict
from ..sdk_base import BaseSDK

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
            clickhouse_task.panel_type.CopyFrom(StringValue(value=panel_type))
            clickhouse_task.fill_gaps.CopyFrom(BoolValue(value=fill_gaps))
            
            if step:
                clickhouse_task.step.CopyFrom(Int32Value(value=step))
            
            # Execute task
            result = source_manager.execute_clickhouse_query(
                time_range, task, connector
            )
            
            if result is None:
                return {"error": "No data returned from Signoz Clickhouse query"}
            
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
                      variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
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
            Dashboard data as dictionary
        """
        try:
            source_manager = self._get_source_manager('signoz')
            connector = self._get_connector('signoz')
            time_range = self._create_time_range(start_time, end_time, duration_minutes)
            
            # Create task proto
            from ..protos.playbooks.source_task_definitions.signoz_task_pb2 import Signoz
            from google.protobuf.wrappers_pb2 import StringValue, Int32Value
            
            task = Signoz()
            task.type = Signoz.TaskType.DASHBOARD_DATA
            
            dashboard_task = task.dashboard_data
            dashboard_task.dashboard_name.CopyFrom(StringValue(value=dashboard_name))
            
            if step:
                dashboard_task.step.CopyFrom(Int32Value(value=step))
            
            if variables:
                dashboard_task.variables_json.CopyFrom(StringValue(value=json.dumps(variables)))
            
            # Execute task
            results = source_manager.execute_dashboard_data(
                time_range, task, connector
            )
            
            if not results:
                return {"error": "No data returned from Signoz dashboard query"}
            
            # Convert list of results to list of dictionaries
            return [proto_to_dict(result) for result in results]
            
        except Exception as e:
            raise TaskExecutionError(f"Signoz dashboard data query failed: {e}")
    
    def fetch_services(self,
                      start_time: Optional[datetime] = None,
                      end_time: Optional[datetime] = None,
                      duration_minutes: Optional[int] = None) -> Dict[str, Any]:
        """
        Fetch services from Signoz
        
        Args:
            start_time: Start time for the query
            end_time: End time for the query
            duration_minutes: Duration in minutes
            
        Returns:
            Services data as dictionary
        """
        try:
            source_manager = self._get_source_manager('signoz')
            connector = self._get_connector('signoz')
            time_range = self._create_time_range(start_time, end_time, duration_minutes)
            
            # Create task proto
            from ..protos.playbooks.source_task_definitions.signoz_task_pb2 import Signoz
            from google.protobuf.wrappers_pb2 import StringValue
            
            task = Signoz()
            task.type = Signoz.TaskType.FETCH_SERVICES
            
            services_task = task.fetch_services
            if start_time:
                services_task.start_time.CopyFrom(StringValue(value=start_time.isoformat()))
            if end_time:
                services_task.end_time.CopyFrom(StringValue(value=end_time.isoformat()))
            if duration_minutes:
                services_task.duration.CopyFrom(StringValue(value=f"{duration_minutes}m"))
            
            # Execute task
            result = source_manager.execute_fetch_services(
                time_range, task, connector
            )
            
            if result is None:
                return {"error": "No data returned from Signoz services query"}
            
            return proto_to_dict(result)
            
        except Exception as e:
            raise TaskExecutionError(f"Signoz services fetch failed: {e}")
    
    def fetch_dashboards(self) -> Dict[str, Any]:
        """
        Fetch all dashboards from Signoz
        
        Returns:
            Dashboards data as dictionary
        """
        try:
            source_manager = self._get_source_manager('signoz')
            connector = self._get_connector('signoz')
            time_range = self._create_time_range()
            
            # Create task proto
            from ..protos.playbooks.source_task_definitions.signoz_task_pb2 import Signoz
            
            task = Signoz()
            task.type = Signoz.TaskType.FETCH_DASHBOARDS
            
            # Execute task
            result = source_manager.execute_fetch_dashboards(
                time_range, task, connector
            )
            
            if result is None:
                return {"error": "No data returned from Signoz dashboards query"}
            
            return proto_to_dict(result)
            
        except Exception as e:
            raise TaskExecutionError(f"Signoz dashboards fetch failed: {e}")
    
    def fetch_dashboard_details(self, dashboard_id: str) -> Dict[str, Any]:
        """
        Fetch details of a specific dashboard
        
        Args:
            dashboard_id: ID of the dashboard
            
        Returns:
            Dashboard details as dictionary
        """
        try:
            source_manager = self._get_source_manager('signoz')
            connector = self._get_connector('signoz')
            time_range = self._create_time_range()
            
            # Create task proto
            from ..protos.playbooks.source_task_definitions.signoz_task_pb2 import Signoz
            from google.protobuf.wrappers_pb2 import StringValue
            
            task = Signoz()
            task.type = Signoz.TaskType.FETCH_DASHBOARD_DETAILS
            
            details_task = task.fetch_dashboard_details
            details_task.dashboard_id.CopyFrom(StringValue(value=dashboard_id))
            
            # Execute task
            result = source_manager.execute_fetch_dashboard_details(
                time_range, task, connector
            )
            
            if result is None:
                return {"error": f"No data returned for dashboard {dashboard_id}"}
            
            return proto_to_dict(result)
            
        except Exception as e:
            raise TaskExecutionError(f"Signoz dashboard details fetch failed: {e}")
    
    def fetch_apm_metrics(self,
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
            from ..protos.playbooks.source_task_definitions.signoz_task_pb2 import Signoz
            from google.protobuf.wrappers_pb2 import StringValue
            
            task = Signoz()
            task.type = Signoz.TaskType.FETCH_APM_METRICS
            
            apm_task = task.fetch_apm_metrics
            apm_task.service_name.CopyFrom(StringValue(value=service_name))
            apm_task.window.CopyFrom(StringValue(value=window))
            
            if start_time:
                apm_task.start_time.CopyFrom(StringValue(value=start_time.isoformat()))
            if end_time:
                apm_task.end_time.CopyFrom(StringValue(value=end_time.isoformat()))
            if duration_minutes:
                apm_task.duration.CopyFrom(StringValue(value=f"{duration_minutes}m"))
            if operation_names:
                apm_task.operation_names.CopyFrom(StringValue(value=json.dumps(operation_names)))
            if metrics:
                apm_task.metrics.CopyFrom(StringValue(value=json.dumps(metrics)))
            
            # Execute task
            result = source_manager.execute_fetch_apm_metrics(
                time_range, task, connector
            )
            
            if result is None:
                return {"error": "No data returned from Signoz APM metrics query"}
            
            return proto_to_dict(result)
            
        except Exception as e:
            raise TaskExecutionError(f"Signoz APM metrics fetch failed: {e}")
    
    def apm_metrics(self,
                   service_name: str,
                   start_time: Optional[datetime] = None,
                   end_time: Optional[datetime] = None,
                   duration_minutes: Optional[int] = None,
                   window: str = "5m",
                   operation_names: Optional[List[str]] = None,
                   metrics: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Fetch APM metrics for a service (alias for fetch_apm_metrics)
        
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
        return self.fetch_apm_metrics(
            service_name=service_name,
            start_time=start_time,
            end_time=end_time,
            duration_minutes=duration_minutes,
            window=window,
            operation_names=operation_names,
            metrics=metrics
        )
    
    def fetch_traces_or_logs(self,
                           data_type: str,
                           start_time: Optional[datetime] = None,
                           end_time: Optional[datetime] = None,
                           duration_minutes: Optional[int] = None,
                           service_name: Optional[str] = None,
                           limit: int = 100) -> Dict[str, Any]:
        """
        Fetch traces or logs from Signoz
        
        Args:
            data_type: Type of data to fetch ('traces' or 'logs')
            start_time: Start time for the query
            end_time: End time for the query
            duration_minutes: Duration in minutes
            service_name: Optional service name to filter by
            limit: Maximum number of records to return
            
        Returns:
            Traces or logs data as dictionary
        """
        try:
            source_manager = self._get_source_manager('signoz')
            connector = self._get_connector('signoz')
            time_range = self._create_time_range(start_time, end_time, duration_minutes)
            
            # Create task proto
            from ..protos.playbooks.source_task_definitions.signoz_task_pb2 import Signoz
            from google.protobuf.wrappers_pb2 import StringValue, Int32Value
            
            task = Signoz()
            task.type = Signoz.TaskType.FETCH_TRACES_OR_LOGS
            
            traces_task = task.fetch_traces_or_logs
            traces_task.data_type.CopyFrom(StringValue(value=data_type))
            traces_task.limit.CopyFrom(Int32Value(value=limit))
            
            if start_time:
                traces_task.start_time.CopyFrom(StringValue(value=start_time.isoformat()))
            if end_time:
                traces_task.end_time.CopyFrom(StringValue(value=end_time.isoformat()))
            if duration_minutes:
                traces_task.duration.CopyFrom(StringValue(value=f"{duration_minutes}m"))
            if service_name:
                traces_task.service_name.CopyFrom(StringValue(value=service_name))
            
            # Execute task
            result = source_manager.execute_fetch_traces_or_logs(
                time_range, task, connector
            )
            
            if result is None:
                return {"error": f"No data returned from Signoz {data_type} query"}
            
            return proto_to_dict(result)
            
        except Exception as e:
            raise TaskExecutionError(f"Signoz {data_type} fetch failed: {e}")
    
    def fetch_alerts(self) -> Dict[str, Any]:
        """
        Fetch all alerts from Signoz
        
        Returns:
            Alerts data as dictionary
        """
        try:
            source_manager = self._get_source_manager('signoz')
            connector = self._get_connector('signoz')
            
            # Execute task using source manager's direct API processor
            api_processor = source_manager.get_connector_processor(connector)
            result = api_processor.fetch_alerts()
            
            if result is None:
                return {"error": "No data returned from Signoz alerts query"}
            
            return result
            
        except Exception as e:
            raise TaskExecutionError(f"Signoz alerts fetch failed: {e}")
    
    def fetch_alert_details(self, alert_id: str) -> Dict[str, Any]:
        """
        Fetch details of a specific alert
        
        Args:
            alert_id: ID of the alert
            
        Returns:
            Alert details as dictionary
        """
        try:
            source_manager = self._get_source_manager('signoz')
            connector = self._get_connector('signoz')
            
            # Execute task using source manager's direct API processor
            api_processor = source_manager.get_connector_processor(connector)
            result = api_processor.fetch_alert_details(alert_id)
            
            if result is None:
                return {"error": f"No data returned for alert {alert_id}"}
            
            return result
            
        except Exception as e:
            raise TaskExecutionError(f"Signoz alert details fetch failed: {e}")
    
 