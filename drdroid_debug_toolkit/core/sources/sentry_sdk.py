"""
Sentry-specific SDK implementation
"""

import logging
from datetime import datetime
from typing import Dict, Any, Optional

from drdroid_debug_toolkit.drdroid_debug_toolkit.exceptions import TaskExecutionError
from drdroid_debug_toolkit.drdroid_debug_toolkit.core.protos.base_pb2 import Source, SourceKeyType
from drdroid_debug_toolkit.drdroid_debug_toolkit.core.integrations.source_managers.sentry_source_manager import SentrySourceManager
from drdroid_debug_toolkit.drdroid_debug_toolkit.core.utils.proto_utils import proto_to_dict
from drdroid_debug_toolkit.drdroid_debug_toolkit.core.sdk_base import BaseSDK

logger = logging.getLogger(__name__)


class SentrySDK(BaseSDK):
    """
    Sentry-specific SDK implementation
    Provides high-level methods for Sentry operations
    """
    
    def _initialize_source_managers(self) -> Dict[str, Any]:
        """Initialize Sentry source manager"""
        managers = {}
        
        if 'sentry' in self.credentials:
            managers['sentry'] = SentrySourceManager()
            
        return managers
    
    def _get_key_type_mapping(self) -> Dict[str, SourceKeyType]:
        """Get Sentry-specific key type mapping"""
        return {
            'sentry_api_url': SourceKeyType.SENTRY_API_URL,
            'sentry_api_token': SourceKeyType.SENTRY_API_TOKEN,
        }
    
    def fetch_issue_info_by_id(self,
                               issue_id: str,
                               start_time: Optional[datetime] = None,
                               end_time: Optional[datetime] = None,
                               duration_minutes: Optional[int] = None) -> Dict[str, Any]:
        """
        Fetch Sentry issue information by ID
        
        Args:
            issue_id: Sentry issue ID
            start_time: Start time for the query
            end_time: End time for the query
            duration_minutes: Duration in minutes
            
        Returns:
            Issue information as dictionary
        """
        try:
            source_manager = self._get_source_manager('sentry')
            connector = self._get_connector('sentry')
            time_range = self._create_time_range(start_time, end_time, duration_minutes)
            
            # Create task proto
            from drdroid_debug_toolkit.drdroid_debug_toolkit.core.protos.playbooks.source_task_definitions.sentry_task_pb2 import Sentry
            from google.protobuf.wrappers_pb2 import StringValue
            
            task = Sentry()
            task.type = Sentry.TaskType.FETCH_ISSUE_INFO_BY_ID
            
            issue_task = task.fetch_issue_info_by_id
            issue_task.issue_id.CopyFrom(StringValue(value=issue_id))
            
            # Execute task
            result = source_manager.fetch_issue_info_by_id(
                time_range, task, connector
            )
            
            return proto_to_dict(result)
            
        except Exception as e:
            raise TaskExecutionError(f"Sentry issue info fetch failed: {e}")
    
    def fetch_event_info_by_id(self,
                               event_id: str,
                               project_slug: str,
                               start_time: Optional[datetime] = None,
                               end_time: Optional[datetime] = None,
                               duration_minutes: Optional[int] = None) -> Dict[str, Any]:
        """
        Fetch Sentry event information by ID
        
        Args:
            event_id: Sentry event ID
            project_slug: Project slug
            start_time: Start time for the query
            end_time: End time for the query
            duration_minutes: Duration in minutes
            
        Returns:
            Event information as dictionary
        """
        try:
            source_manager = self._get_source_manager('sentry')
            connector = self._get_connector('sentry')
            time_range = self._create_time_range(start_time, end_time, duration_minutes)
            
            # Create task proto
            from drdroid_debug_toolkit.drdroid_debug_toolkit.core.protos.playbooks.source_task_definitions.sentry_task_pb2 import Sentry
            from google.protobuf.wrappers_pb2 import StringValue
            
            task = Sentry()
            task.type = Sentry.TaskType.FETCH_EVENT_INFO_BY_ID
            
            event_task = task.fetch_event_info_by_id
            event_task.event_id.CopyFrom(StringValue(value=event_id))
            event_task.project_slug.CopyFrom(StringValue(value=project_slug))
            
            # Execute task
            result = source_manager.fetch_event_info_by_id(
                time_range, task, connector
            )
            
            return proto_to_dict(result)
            
        except Exception as e:
            raise TaskExecutionError(f"Sentry event info fetch failed: {e}")
    
    def fetch_list_of_recent_events_with_search_query(self,
                                                     project_slug: str,
                                                     query: str = "is:unresolved",
                                                     max_events_to_analyse: int = 10,
                                                     start_time: Optional[datetime] = None,
                                                     end_time: Optional[datetime] = None,
                                                     duration_minutes: Optional[int] = None) -> Dict[str, Any]:
        """
        Fetch list of recent events with search query
        
        Args:
            project_slug: Project slug
            query: Search query (default: "is:unresolved")
            max_events_to_analyse: Maximum number of events to analyze
            start_time: Start time for the query
            end_time: End time for the query
            duration_minutes: Duration in minutes
            
        Returns:
            Recent events as dictionary
        """
        try:
            source_manager = self._get_source_manager('sentry')
            connector = self._get_connector('sentry')
            time_range = self._create_time_range(start_time, end_time, duration_minutes)
            
            # Create task proto
            from drdroid_debug_toolkit.drdroid_debug_toolkit.core.protos.playbooks.source_task_definitions.sentry_task_pb2 import Sentry
            from google.protobuf.wrappers_pb2 import StringValue, Int64Value
            
            task = Sentry()
            task.type = Sentry.TaskType.FETCH_LIST_OF_RECENT_EVENTS_WITH_SEARCH_QUERY
            
            events_task = task.fetch_list_of_recent_events_with_search_query
            events_task.project_slug.CopyFrom(StringValue(value=project_slug))
            events_task.query.CopyFrom(StringValue(value=query))
            events_task.max_events_to_analyse.CopyFrom(Int64Value(value=max_events_to_analyse))
            
            # Execute task
            result = source_manager.fetch_list_of_recent_events_with_search_query(
                time_range, task, connector
            )
            
            return proto_to_dict(result)
            
        except Exception as e:
            raise TaskExecutionError(f"Sentry recent events fetch failed: {e}") 