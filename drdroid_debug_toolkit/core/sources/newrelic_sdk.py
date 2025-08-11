"""
NewRelic-specific SDK implementation
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime

from ...exceptions import TaskExecutionError
from ..protos.base_pb2 import Source, SourceKeyType
from ..integrations.source_managers.newrelic_source_manager import NewRelicSourceManager
from ..utils.proto_utils import proto_to_dict
from ..sdk_base import BaseSDK

logger = logging.getLogger(__name__)


class NewRelicSDK(BaseSDK):
    """
    NewRelic-specific SDK implementation
    Provides high-level methods for NewRelic operations
    """
    
    def _initialize_source_managers(self) -> Dict[str, Any]:
        """Initialize NewRelic source manager"""
        managers = {}
        
        if 'newrelic' in self.credentials:
            managers['newrelic'] = NewRelicSourceManager()
            
        return managers
    
    def _get_key_type_mapping(self) -> Dict[str, SourceKeyType]:
        """Get NewRelic-specific key type mapping"""
        return {
            'newrelic_api_key': SourceKeyType.NEW_RELIC_API_KEY,
            'newrelic_account_id': SourceKeyType.NEW_RELIC_ACCOUNT_ID,
            'newrelic_region': SourceKeyType.NEW_RELIC_REGION,
        }
    
    def execute_entity_application_golden_metric(self,
                                               application_entity_name: str,
                                               golden_metric_name: str,
                                               golden_metric_unit: str,
                                               golden_metric_nrql_expression: str,
                                               start_time: Optional[datetime] = None,
                                               end_time: Optional[datetime] = None,
                                               duration_minutes: Optional[int] = None) -> Dict[str, Any]:
        """
        Execute a NewRelic golden metric query for an application entity
        
        Args:
            application_entity_name: Name of the application entity
            golden_metric_name: Name of the golden metric
            golden_metric_unit: Unit of the metric
            golden_metric_nrql_expression: NRQL expression for the metric
            start_time: Start time for the query
            end_time: End time for the query
            duration_minutes: Duration in minutes
            
        Returns:
            Golden metric results as dictionary
        """
        try:
            source_manager = self._get_source_manager('newrelic')
            connector = self._get_connector('newrelic')
            time_range = self._create_time_range(start_time, end_time, duration_minutes)
            
            # Create task proto
            from ..protos.playbooks.source_task_definitions.new_relic_task_pb2 import NewRelic
            from google.protobuf.wrappers_pb2 import StringValue
            
            task = NewRelic()
            task.type = NewRelic.TaskType.ENTITY_APPLICATION_GOLDEN_METRIC_EXECUTION
            
            metric_task = task.entity_application_golden_metric_execution
            metric_task.application_entity_name.CopyFrom(StringValue(value=application_entity_name))
            metric_task.golden_metric_name.CopyFrom(StringValue(value=golden_metric_name))
            metric_task.golden_metric_unit.CopyFrom(StringValue(value=golden_metric_unit))
            metric_task.golden_metric_nrql_expression.CopyFrom(StringValue(value=golden_metric_nrql_expression))
            
            # Execute task
            result = source_manager.execute_entity_application_golden_metric_execution(
                time_range, task, connector
            )
            
            return proto_to_dict(result)
            
        except Exception as e:
            raise TaskExecutionError(f"NewRelic golden metric execution failed: {e}")
    
    def execute_entity_application_apm_metric(self,
                                            application_entity_name: str,
                                            apm_metric_names: Optional[str] = None,
                                            start_time: Optional[datetime] = None,
                                            end_time: Optional[datetime] = None,
                                            duration_minutes: Optional[int] = None) -> Dict[str, Any]:
        """
        Execute a NewRelic APM metric query for an application entity
        
        Args:
            application_entity_name: Name of the application entity
            apm_metric_names: Optional comma-separated list of APM metric names
            start_time: Start time for the query
            end_time: End time for the query
            duration_minutes: Duration in minutes
            
        Returns:
            APM metric results as dictionary
        """
        try:
            source_manager = self._get_source_manager('newrelic')
            connector = self._get_connector('newrelic')
            time_range = self._create_time_range(start_time, end_time, duration_minutes)
            
            # Create task proto
            from ..protos.playbooks.source_task_definitions.new_relic_task_pb2 import NewRelic
            from google.protobuf.wrappers_pb2 import StringValue
            
            task = NewRelic()
            task.type = NewRelic.TaskType.ENTITY_APPLICATION_APM_METRIC_EXECUTION
            
            metric_task = task.entity_application_apm_metric_execution
            metric_task.application_entity_name.CopyFrom(StringValue(value=application_entity_name))
            
            if apm_metric_names:
                metric_task.apm_metric_names.CopyFrom(StringValue(value=apm_metric_names))
            
            # Execute task
            result = source_manager.execute_entity_application_apm_metric_execution(
                time_range, task, connector
            )
            
            return proto_to_dict(result)
            
        except Exception as e:
            raise TaskExecutionError(f"NewRelic APM metric execution failed: {e}")
    
    def execute_entity_dashboard_widget_nrql_metric(self,
                                                  dashboard_guid: str,
                                                  widget_id: str,
                                                  start_time: Optional[datetime] = None,
                                                  end_time: Optional[datetime] = None,
                                                  duration_minutes: Optional[int] = None) -> Dict[str, Any]:
        """
        Execute a NewRelic dashboard widget NRQL metric query
        
        Args:
            dashboard_guid: GUID of the dashboard
            widget_id: ID of the widget
            start_time: Start time for the query
            end_time: End time for the query
            duration_minutes: Duration in minutes
            
        Returns:
            Dashboard widget metric results as dictionary
        """
        try:
            source_manager = self._get_source_manager('newrelic')
            connector = self._get_connector('newrelic')
            time_range = self._create_time_range(start_time, end_time, duration_minutes)
            
            # Create task proto
            from ..protos.playbooks.source_task_definitions.new_relic_task_pb2 import NewRelic
            from google.protobuf.wrappers_pb2 import StringValue
            
            task = NewRelic()
            task.type = NewRelic.TaskType.ENTITY_DASHBOARD_WIDGET_NRQL_METRIC_EXECUTION
            
            widget_task = task.entity_dashboard_widget_nrql_metric_execution
            widget_task.dashboard_guid.CopyFrom(StringValue(value=dashboard_guid))
            widget_task.widget_id.CopyFrom(StringValue(value=widget_id))
            
            # Execute task
            result = source_manager.execute_entity_dashboard_widget_nrql_metric_execution(
                time_range, task, connector
            )
            
            return proto_to_dict(result)
            
        except Exception as e:
            raise TaskExecutionError(f"NewRelic dashboard widget metric execution failed: {e}")
    
    def execute_nrql_metric(self,
                           nrql_expression: str,
                           start_time: Optional[datetime] = None,
                           end_time: Optional[datetime] = None,
                           duration_minutes: Optional[int] = None) -> Dict[str, Any]:
        """
        Execute a NewRelic NRQL metric query
        
        Args:
            nrql_expression: NRQL expression to execute
            start_time: Start time for the query
            end_time: End time for the query
            duration_minutes: Duration in minutes
            
        Returns:
            NRQL metric results as dictionary
        """
        try:
            source_manager = self._get_source_manager('newrelic')
            connector = self._get_connector('newrelic')
            time_range = self._create_time_range(start_time, end_time, duration_minutes)
            
            # Create task proto
            from ..protos.playbooks.source_task_definitions.new_relic_task_pb2 import NewRelic
            from google.protobuf.wrappers_pb2 import StringValue
            
            task = NewRelic()
            task.type = NewRelic.TaskType.NRQL_METRIC_EXECUTION
            
            nrql_task = task.nrql_metric_execution
            nrql_task.nrql_expression.CopyFrom(StringValue(value=nrql_expression))
            
            # Execute task
            result = source_manager.execute_nrql_metric_execution(
                time_range, task, connector
            )
            
            return proto_to_dict(result)
            
        except Exception as e:
            raise TaskExecutionError(f"NewRelic NRQL metric execution failed: {e}")
    
    def fetch_dashboard_widgets(self,
                               dashboard_name: str,
                               page_name: Optional[str] = None,
                               widget_names: Optional[List[str]] = None,
                               start_time: Optional[datetime] = None,
                               end_time: Optional[datetime] = None,
                               duration_minutes: Optional[int] = None) -> Dict[str, Any]:
        """
        Fetch NewRelic dashboard widgets
        
        Args:
            dashboard_name: Name of the dashboard
            page_name: Optional page name to filter
            widget_names: Optional list of widget names to filter
            start_time: Start time for the query
            end_time: End time for the query
            duration_minutes: Duration in minutes
            
        Returns:
            Dashboard widgets results as dictionary
        """
        try:
            source_manager = self._get_source_manager('newrelic')
            connector = self._get_connector('newrelic')
            time_range = self._create_time_range(start_time, end_time, duration_minutes)
            
            # Create task proto
            from ..protos.playbooks.source_task_definitions.new_relic_task_pb2 import NewRelic
            from google.protobuf.wrappers_pb2 import StringValue
            
            task = NewRelic()
            task.type = NewRelic.TaskType.FETCH_DASHBOARD_WIDGETS
            
            dashboard_task = task.fetch_dashboard_widgets
            dashboard_task.dashboard_name.CopyFrom(StringValue(value=dashboard_name))
            
            if page_name:
                dashboard_task.page_name.CopyFrom(StringValue(value=page_name))
            
            if widget_names:
                widget_names_str = ",".join(widget_names)
                dashboard_task.widget_names.CopyFrom(StringValue(value=widget_names_str))
            
            # Execute task
            result = source_manager.execute_fetch_dashboard_widgets(
                time_range, task, connector
            )
            
            return proto_to_dict(result)
            
        except Exception as e:
            raise TaskExecutionError(f"NewRelic dashboard widgets fetch failed: {e}") 