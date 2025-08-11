"""
PostHog-specific SDK implementation
"""

import logging
from typing import Dict, Any, Optional
from datetime import datetime

from ...exceptions import TaskExecutionError
from ..protos.base_pb2 import Source, SourceKeyType
from ..integrations.source_managers.posthog_source_manager import PosthogSourceManager
from ..utils.proto_utils import proto_to_dict
from ..sdk_base import BaseSDK

logger = logging.getLogger(__name__)


class PostHogSDK(BaseSDK):
    """
    PostHog-specific SDK implementation
    Provides high-level methods for PostHog operations
    """
    
    def _initialize_source_managers(self) -> Dict[str, Any]:
        """Initialize PostHog source manager"""
        managers = {}
        
        if 'posthog' in self.credentials:
            managers['posthog'] = PosthogSourceManager()
            
        return managers
    
    def _get_key_type_mapping(self) -> Dict[str, SourceKeyType]:
        """Get PostHog-specific key type mapping"""
        return {
            'posthog_api_key': SourceKeyType.POSTHOG_API_KEY,
            'posthog_host': SourceKeyType.POSTHOG_HOST,
        }
    
    def execute_hogql_query(self,
                           query: str,
                           start_time: Optional[datetime] = None,
                           end_time: Optional[datetime] = None,
                           duration_minutes: Optional[int] = None) -> Dict[str, Any]:
        """
        Execute a HogQL query on PostHog
        
        Args:
            query: HogQL query to execute
            start_time: Start time for the query
            end_time: End time for the query
            duration_minutes: Duration in minutes
            
        Returns:
            HogQL query results as dictionary
        """
        try:
            source_manager = self._get_source_manager('posthog')
            connector = self._get_connector('posthog')
            time_range = self._create_time_range(start_time, end_time, duration_minutes)
            
            # Create task proto
            from ..protos.playbooks.source_task_definitions.posthog_task_pb2 import PostHog
            from google.protobuf.wrappers_pb2 import StringValue
            
            task = PostHog()
            task.type = PostHog.TaskType.HOGQL_QUERY
            
            hogql_task = task.hogql_query
            hogql_task.query.CopyFrom(StringValue(value=query))
            
            # Execute task
            result = source_manager.execute_hogql_query(
                time_range, task, connector
            )
            
            return proto_to_dict(result)
            
        except Exception as e:
            raise TaskExecutionError(f"PostHog HogQL query execution failed: {e}")
    
    def execute_event_query(self,
                           event_name: Optional[str] = None,
                           person_id: Optional[str] = None,
                           properties: Optional[Dict[str, Any]] = None,
                           limit: Optional[int] = None,
                           start_time: Optional[datetime] = None,
                           end_time: Optional[datetime] = None,
                           duration_minutes: Optional[int] = None) -> Dict[str, Any]:
        """
        Execute a PostHog event query
        
        Args:
            event_name: Optional event name to filter
            person_id: Optional person ID to filter
            properties: Optional properties to filter
            limit: Optional limit for results (default: 100)
            start_time: Start time for the query
            end_time: End time for the query
            duration_minutes: Duration in minutes
            
        Returns:
            Event query results as dictionary
        """
        try:
            source_manager = self._get_source_manager('posthog')
            connector = self._get_connector('posthog')
            time_range = self._create_time_range(start_time, end_time, duration_minutes)
            
            # Create task proto
            from ..protos.playbooks.source_task_definitions.posthog_task_pb2 import PostHog
            from google.protobuf.wrappers_pb2 import StringValue, UInt64Value
            from google.protobuf.struct_pb2 import Struct
            
            task = PostHog()
            task.type = PostHog.TaskType.EVENT_QUERY
            
            event_task = task.event_query
            
            if event_name:
                event_task.event_name.CopyFrom(StringValue(value=event_name))
            
            if person_id:
                event_task.person_id.CopyFrom(StringValue(value=person_id))
            
            if limit:
                event_task.limit.CopyFrom(UInt64Value(value=limit))
            
            if properties:
                # Convert properties to Struct
                struct = Struct()
                for key, value in properties.items():
                    if isinstance(value, str):
                        struct.fields[key].string_value = value
                    elif isinstance(value, int):
                        struct.fields[key].number_value = value
                    elif isinstance(value, bool):
                        struct.fields[key].bool_value = value
                    else:
                        struct.fields[key].string_value = str(value)
                
                event_task.properties.CopyFrom(struct)
            
            # Execute task
            result = source_manager.execute_event_query(
                time_range, task, connector
            )
            
            return proto_to_dict(result)
            
        except Exception as e:
            raise TaskExecutionError(f"PostHog event query execution failed: {e}") 