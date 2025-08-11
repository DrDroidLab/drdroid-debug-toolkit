"""
SQL Database Connection-specific SDK implementation
"""

import logging
from typing import Dict, Any, Optional
from datetime import datetime

from ...exceptions import TaskExecutionError
from ..protos.base_pb2 import Source, SourceKeyType
from ..integrations.source_managers.sql_database_connection_source_manager import SqlDatabaseConnectionSourceManager
from ..utils.proto_utils import proto_to_dict
from ..sdk_base import BaseSDK

logger = logging.getLogger(__name__)


class SqlDatabaseConnectionSDK(BaseSDK):
    """
    SQL Database Connection-specific SDK implementation
    Provides high-level methods for SQL database operations
    """
    
    def _initialize_source_managers(self) -> Dict[str, Any]:
        """Initialize SQL Database Connection source manager"""
        managers = {}
        
        if 'sql_database_connection' in self.credentials:
            managers['sql_database_connection'] = SqlDatabaseConnectionSourceManager()
            
        return managers
    
    def _get_key_type_mapping(self) -> Dict[str, SourceKeyType]:
        """Get SQL Database Connection-specific key type mapping"""
        return {
            'connection_string': SourceKeyType.SQL_DATABASE_CONNECTION_STRING,
        }
    
    def execute_sql_query(self,
                         query: str,
                         timeout: Optional[int] = None,
                         start_time: Optional[datetime] = None,
                         end_time: Optional[datetime] = None,
                         duration_minutes: Optional[int] = None) -> Dict[str, Any]:
        """
        Execute a SQL query on any SQL database
        
        Args:
            query: SQL query to execute
            timeout: Optional timeout in seconds (default: 120)
            start_time: Start time for the query (not used for SQL queries)
            end_time: End time for the query (not used for SQL queries)
            duration_minutes: Duration in minutes (not used for SQL queries)
            
        Returns:
            SQL query results as dictionary
        """
        try:
            source_manager = self._get_source_manager('sql_database_connection')
            connector = self._get_connector('sql_database_connection')
            time_range = self._create_time_range(start_time, end_time, duration_minutes)
            
            # Create task proto
            from ..protos.playbooks.source_task_definitions.sql_data_fetch_task_pb2 import SqlDataFetch
            from google.protobuf.wrappers_pb2 import StringValue, Int64Value
            
            task = SqlDataFetch()
            task.type = SqlDataFetch.TaskType.SQL_QUERY
            
            sql_task = task.sql_query
            sql_task.query.CopyFrom(StringValue(value=query))
            
            if timeout:
                sql_task.timeout.CopyFrom(Int64Value(value=timeout))
            
            # Execute task
            result = source_manager.execute_sql_query(
                time_range, task, connector
            )
            
            return proto_to_dict(result)
            
        except Exception as e:
            raise TaskExecutionError(f"SQL Database Connection query execution failed: {e}") 