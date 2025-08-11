"""
Datadog-specific SDK implementation
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime

from ...exceptions import TaskExecutionError
from ..protos.base_pb2 import Source, SourceKeyType
from ..integrations.source_managers.datadog_source_manager import DatadogSourceManager
from ..utils.proto_utils import proto_to_dict
from ..sdk_base import BaseSDK

logger = logging.getLogger(__name__)


class DatadogSDK(BaseSDK):
    """
    Datadog-specific SDK implementation
    Provides high-level methods for Datadog operations
    """
    
    def _initialize_source_managers(self) -> Dict[str, Any]:
        """Initialize Datadog source manager"""
        managers = {}
        
        if 'datadog' in self.credentials:
            managers['datadog'] = DatadogSourceManager()
            
        return managers
    
    def _get_key_type_mapping(self) -> Dict[str, SourceKeyType]:
        """Get Datadog-specific key type mapping"""
        return {
            'datadog_api_key': SourceKeyType.DATADOG_API_KEY,
            'datadog_app_key': SourceKeyType.DATADOG_APP_KEY,
            'datadog_host': SourceKeyType.DATADOG_HOST,
        }
    
    def execute_service_metric(self,
                             service_name: str,
                             environment_name: str,
                             metric_family: str,
                             metrics: List[str],
                             interval: Optional[int] = None,
                             start_time: Optional[datetime] = None,
                             end_time: Optional[datetime] = None,
                             duration_minutes: Optional[int] = None) -> Dict[str, Any]:
        """
        Execute a Datadog service metric query
        
        Args:
            service_name: Name of the service
            environment_name: Name of the environment
            metric_family: Metric family to query
            metrics: List of metric names to fetch
            interval: Step interval in seconds
            start_time: Start time for the query
            end_time: End time for the query
            duration_minutes: Duration in minutes
            
        Returns:
            Service metric results as dictionary
        """
        try:
            source_manager = self._get_source_manager('datadog')
            connector = self._get_connector('datadog')
            time_range = self._create_time_range(start_time, end_time, duration_minutes)
            
            # Create task proto
            from ..protos.playbooks.source_task_definitions.datadog_task_pb2 import Datadog
            from google.protobuf.wrappers_pb2 import StringValue, UInt64Value
            
            task = Datadog()
            task.type = Datadog.TaskType.SERVICE_METRIC_EXECUTION
            
            metric_task = task.service_metric_execution
            metric_task.service_name.CopyFrom(StringValue(value=service_name))
            metric_task.environment_name.CopyFrom(StringValue(value=environment_name))
            metric_task.metric_family.CopyFrom(StringValue(value=metric_family))
            
            # Add metrics
            for metric in metrics:
                metric_obj = metric_task.metrics.add()
                metric_obj.CopyFrom(StringValue(value=metric))
            
            if interval:
                metric_task.interval.CopyFrom(UInt64Value(value=interval))
            
            # Execute task
            result = source_manager.execute_service_metric_execution(
                time_range, task, connector
            )
            
            return proto_to_dict(result)
            
        except Exception as e:
            raise TaskExecutionError(f"Datadog service metric execution failed: {e}")
    
    def execute_log_query(self,
                         query: str,
                         start_time: Optional[datetime] = None,
                         end_time: Optional[datetime] = None,
                         duration_minutes: Optional[int] = None) -> Dict[str, Any]:
        """
        Execute a Datadog log query
        
        Args:
            query: Log query expression
            start_time: Start time for the query
            end_time: End time for the query
            duration_minutes: Duration in minutes
            
        Returns:
            Log query results as dictionary
        """
        try:
            source_manager = self._get_source_manager('datadog')
            connector = self._get_connector('datadog')
            time_range = self._create_time_range(start_time, end_time, duration_minutes)
            
            # Create task proto
            from ..protos.playbooks.source_task_definitions.datadog_task_pb2 import Datadog
            from google.protobuf.wrappers_pb2 import StringValue
            
            task = Datadog()
            task.type = Datadog.TaskType.LOG_QUERY_EXECUTION
            
            log_task = task.log_query_execution
            log_task.query.CopyFrom(StringValue(value=query))
            
            # Execute task
            result = source_manager.execute_log_query_execution(
                time_range, task, connector
            )
            
            return proto_to_dict(result)
            
        except Exception as e:
            raise TaskExecutionError(f"Datadog log query execution failed: {e}")
    
    def execute_dashboard_widgets(self,
                                dashboard_name: str,
                                widget_id: Optional[str] = None,
                                start_time: Optional[datetime] = None,
                                end_time: Optional[datetime] = None,
                                duration_minutes: Optional[int] = None) -> Dict[str, Any]:
        """
        Execute Datadog dashboard widgets query
        
        Args:
            dashboard_name: Name of the dashboard
            widget_id: Optional widget ID to filter
            start_time: Start time for the query
            end_time: End time for the query
            duration_minutes: Duration in minutes
            
        Returns:
            Dashboard widgets results as dictionary
        """
        try:
            source_manager = self._get_source_manager('datadog')
            connector = self._get_connector('datadog')
            time_range = self._create_time_range(start_time, end_time, duration_minutes)
            
            # Create task proto
            from ..protos.playbooks.source_task_definitions.datadog_task_pb2 import Datadog
            from google.protobuf.wrappers_pb2 import StringValue
            
            task = Datadog()
            task.type = Datadog.TaskType.DASHBOARD_MULTIPLE_WIDGETS
            
            dashboard_task = task.dashboard_multiple_widgets
            dashboard_task.dashboard_name.CopyFrom(StringValue(value=dashboard_name))
            
            if widget_id:
                dashboard_task.widget_id.CopyFrom(StringValue(value=widget_id))
            
            # Execute task
            result = source_manager.execute_dashboard_multiple_widgets_task(
                time_range, task, connector
            )
            
            return proto_to_dict(result)
            
        except Exception as e:
            raise TaskExecutionError(f"Datadog dashboard widgets execution failed: {e}")
    
    def execute_apm_queries(self,
                           start_time: Optional[datetime] = None,
                           end_time: Optional[datetime] = None,
                           duration_minutes: Optional[int] = None) -> Dict[str, Any]:
        """
        Execute Datadog APM queries
        
        Args:
            start_time: Start time for the query
            end_time: End time for the query
            duration_minutes: Duration in minutes
            
        Returns:
            APM query results as dictionary
        """
        try:
            source_manager = self._get_source_manager('datadog')
            connector = self._get_connector('datadog')
            time_range = self._create_time_range(start_time, end_time, duration_minutes)
            
            # Create task proto
            from ..protos.playbooks.source_task_definitions.datadog_task_pb2 import Datadog
            
            task = Datadog()
            task.type = Datadog.TaskType.APM_QUERIES
            
            # Execute task
            result = source_manager.execute_apm_queries(
                time_range, task, connector
            )
            
            return proto_to_dict(result)
            
        except Exception as e:
            raise TaskExecutionError(f"Datadog APM queries execution failed: {e}") 