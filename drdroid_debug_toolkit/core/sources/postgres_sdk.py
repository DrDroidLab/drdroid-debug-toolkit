"""
Postgres-specific SDK implementation
"""

import logging
from typing import Dict, Any, Optional
from datetime import datetime

from ...exceptions import TaskExecutionError
from ..protos.base_pb2 import Source, SourceKeyType
from ..integrations.source_managers.postgres_source_manager import PostgresSourceManager
from ..utils.proto_utils import proto_to_dict
from ..sdk_base import BaseSDK

logger = logging.getLogger(__name__)


class PostgresSDK(BaseSDK):
    """
    Postgres-specific SDK implementation
    Provides high-level methods for Postgres operations
    """
    
    def _initialize_source_managers(self) -> Dict[str, Any]:
        """Initialize Postgres source manager"""
        managers = {}
        
        if 'postgres' in self.credentials:
            managers['postgres'] = PostgresSourceManager()
            
        return managers
    
    def _get_key_type_mapping(self) -> Dict[str, SourceKeyType]:
        """Get Postgres-specific key type mapping"""
        return {
            'host': SourceKeyType.POSTGRES_HOST,
            'port': SourceKeyType.POSTGRES_PORT,
            'user': SourceKeyType.POSTGRES_USER,
            'password': SourceKeyType.POSTGRES_PASSWORD,
            'database': SourceKeyType.POSTGRES_DATABASE,
        }
    
    def execute_sql_query(self,
                         query: str,
                         database: Optional[str] = None,
                         timeout: Optional[int] = None,
                         start_time: Optional[datetime] = None,
                         end_time: Optional[datetime] = None,
                         duration_minutes: Optional[int] = None) -> Dict[str, Any]:
        """
        Execute a SQL query on Postgres database
        
        Args:
            query: SQL query to execute
            database: Optional database name (if not specified in credentials)
            timeout: Optional timeout in seconds (default: 120)
            start_time: Start time for the query (not used for SQL queries)
            end_time: End time for the query (not used for SQL queries)
            duration_minutes: Duration in minutes (not used for SQL queries)
            
        Returns:
            SQL query results as dictionary
        """
        try:
            source_manager = self._get_source_manager('postgres')
            connector = self._get_connector('postgres')
            time_range = self._create_time_range(start_time, end_time, duration_minutes)
            
            # Create task proto
            from ..protos.playbooks.source_task_definitions.sql_data_fetch_task_pb2 import SqlDataFetch
            from google.protobuf.wrappers_pb2 import StringValue, UInt64Value
            
            task = SqlDataFetch()
            task.type = SqlDataFetch.TaskType.SQL_QUERY
            
            sql_task = task.sql_query
            sql_task.query.CopyFrom(StringValue(value=query))
            
            if database:
                sql_task.database.CopyFrom(StringValue(value=database))
            
            if timeout:
                sql_task.timeout.CopyFrom(UInt64Value(value=timeout))
            
            # Execute task
            result = source_manager.execute_sql_query(
                time_range, task, connector
            )
            
            return proto_to_dict(result)
            
        except Exception as e:
            raise TaskExecutionError(f"Postgres SQL query execution failed: {e}") 