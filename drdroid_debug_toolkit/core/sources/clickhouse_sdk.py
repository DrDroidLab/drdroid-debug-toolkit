"""
ClickHouse-specific SDK implementation
"""

import logging
from typing import Dict, Any, Optional
from datetime import datetime

from ...exceptions import TaskExecutionError
from ..protos.base_pb2 import Source, SourceKeyType
from ..integrations.source_managers.clickhouse_source_manager import ClickhouseSourceManager
from ..utils.proto_utils import proto_to_dict
from ..sdk_base import BaseSDK

logger = logging.getLogger(__name__)


class ClickHouseSDK(BaseSDK):
    """
    ClickHouse-specific SDK implementation
    Provides high-level methods for ClickHouse operations
    """
    
    def _initialize_source_managers(self) -> Dict[str, Any]:
        """Initialize ClickHouse source manager"""
        managers = {}
        
        if 'clickhouse' in self.credentials:
            managers['clickhouse'] = ClickhouseSourceManager()
            
        return managers
    
    def _get_key_type_mapping(self) -> Dict[str, SourceKeyType]:
        """Get ClickHouse-specific key type mapping"""
        return {
            'clickhouse_host': SourceKeyType.CLICKHOUSE_HOST,
            'clickhouse_port': SourceKeyType.CLICKHOUSE_PORT,
            'clickhouse_username': SourceKeyType.CLICKHOUSE_USERNAME,
            'clickhouse_password': SourceKeyType.CLICKHOUSE_PASSWORD,
            'clickhouse_database': SourceKeyType.CLICKHOUSE_DATABASE,
        }
    
    def execute_sql_query(self,
                         query: str,
                         database: Optional[str] = None,
                         timeout: Optional[int] = None,
                         start_time: Optional[datetime] = None,
                         end_time: Optional[datetime] = None,
                         duration_minutes: Optional[int] = None) -> Dict[str, Any]:
        """
        Execute a SQL query on ClickHouse database
        
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
            source_manager = self._get_source_manager('clickhouse')
            connector = self._get_connector('clickhouse')
            time_range = self._create_time_range(start_time, end_time, duration_minutes)
            
            # Create task proto
            from ..protos.playbooks.source_task_definitions.sql_data_fetch_task_pb2 import SqlDataFetch
            from google.protobuf.wrappers_pb2 import StringValue, Int64Value
            
            task = SqlDataFetch()
            task.type = SqlDataFetch.TaskType.SQL_QUERY
            
            sql_task = task.sql_query
            sql_task.query.CopyFrom(StringValue(value=query))
            
            if database:
                sql_task.database.CopyFrom(StringValue(value=database))
            
            if timeout:
                sql_task.timeout.CopyFrom(Int64Value(value=timeout))
            
            # Execute task
            result = source_manager.execute_sql_query(
                time_range, task, connector
            )
            
            return proto_to_dict(result)
            
        except Exception as e:
            raise TaskExecutionError(f"ClickHouse SQL query execution failed: {e}") 