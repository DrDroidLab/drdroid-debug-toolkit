"""
CloudWatch-specific SDK implementation
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime

from drdroid_debug_toolkit.exceptions import TaskExecutionError
from drdroid_debug_toolkit.core.protos.base_pb2 import Source, SourceKeyType
from drdroid_debug_toolkit.core.integrations.source_managers.cloudwatch_source_manager import CloudwatchSourceManager
from drdroid_debug_toolkit.core.utils.proto_utils import proto_to_dict
from drdroid_debug_toolkit.core.sdk_base import BaseSDK

logger = logging.getLogger(__name__)


class CloudWatchSDK(BaseSDK):
    """
    CloudWatch-specific SDK implementation
    Provides high-level methods for CloudWatch operations
    """
    
    def _initialize_source_managers(self) -> Dict[str, Any]:
        """Initialize CloudWatch source manager"""
        managers = {}
        
        if 'cloudwatch' in self.credentials:
            managers['cloudwatch'] = CloudwatchSourceManager()
            
        return managers
    
    def _get_key_type_mapping(self) -> Dict[str, SourceKeyType]:
        """Get CloudWatch-specific key type mapping"""
        return {
            'aws_access_key_id': SourceKeyType.AWS_ACCESS_KEY_ID,
            'aws_secret_access_key': SourceKeyType.AWS_SECRET_ACCESS_KEY,
            'aws_region': SourceKeyType.AWS_REGION,
        }
    
    def execute_metric(self,
                      namespace: str,
                      metric_name: str,
                      region: str,
                      dimensions: Optional[List[Dict[str, str]]] = None,
                      statistic: str = "Average",
                      period: Optional[int] = None,
                      timeseries_offsets: Optional[List[int]] = None,
                      start_time: Optional[datetime] = None,
                      end_time: Optional[datetime] = None,
                      duration_minutes: Optional[int] = None) -> Dict[str, Any]:
        """
        Execute a CloudWatch metric query
        
        Args:
            namespace: CloudWatch namespace
            metric_name: Name of the metric
            region: AWS region
            dimensions: Optional list of dimension dictionaries with 'name' and 'value' keys
            statistic: Statistic to use (Average, Sum, Minimum, Maximum, etc.)
            period: Period in seconds
            timeseries_offsets: Optional list of time offsets
            start_time: Start time for the query
            end_time: End time for the query
            duration_minutes: Duration in minutes
            
        Returns:
            Metric query results as dictionary
        """
        try:
            source_manager = self._get_source_manager('cloudwatch')
            connector = self._get_connector('cloudwatch')
            time_range = self._create_time_range(start_time, end_time, duration_minutes)
            
            # Create task proto
            from drdroid_debug_toolkit.core.protos.playbooks.source_task_definitions.cloudwatch_task_pb2 import Cloudwatch
            from google.protobuf.wrappers_pb2 import StringValue, UInt64Value
            
            task = Cloudwatch()
            task.type = Cloudwatch.TaskType.METRIC_EXECUTION
            
            metric_task = task.metric_execution
            metric_task.namespace.CopyFrom(StringValue(value=namespace))
            metric_task.metric_name.CopyFrom(StringValue(value=metric_name))
            metric_task.region.CopyFrom(StringValue(value=region))
            metric_task.statistic.CopyFrom(StringValue(value=statistic))
            
            if period:
                metric_task.period.CopyFrom(UInt64Value(value=period))
            
            if dimensions:
                for dim in dimensions:
                    dimension = metric_task.dimensions.add()
                    dimension.name.CopyFrom(StringValue(value=dim['name']))
                    dimension.value.CopyFrom(StringValue(value=dim['value']))
            
            if timeseries_offsets:
                metric_task.timeseries_offsets.extend(timeseries_offsets)
            
            # Execute task
            result = source_manager.execute_metric_execution(
                time_range, task, connector
            )
            
            return proto_to_dict(result)
            
        except Exception as e:
            raise TaskExecutionError(f"CloudWatch metric execution failed: {e}")
    
    def filter_log_events(self,
                         log_group_name: str,
                         region: str,
                         filter_query: Optional[str] = None,
                         start_time: Optional[datetime] = None,
                         end_time: Optional[datetime] = None,
                         duration_minutes: Optional[int] = None) -> Dict[str, Any]:
        """
        Filter CloudWatch log events
        
        Args:
            log_group_name: Name of the log group
            region: AWS region
            filter_query: Optional filter query
            start_time: Start time for the query
            end_time: End time for the query
            duration_minutes: Duration in minutes
            
        Returns:
            Log events as dictionary
        """
        try:
            source_manager = self._get_source_manager('cloudwatch')
            connector = self._get_connector('cloudwatch')
            time_range = self._create_time_range(start_time, end_time, duration_minutes)
            
            # Create task proto
            from drdroid_debug_toolkit.core.protos.playbooks.source_task_definitions.cloudwatch_task_pb2 import Cloudwatch
            from google.protobuf.wrappers_pb2 import StringValue
            
            task = Cloudwatch()
            task.type = Cloudwatch.TaskType.FILTER_LOG_EVENTS
            
            log_task = task.filter_log_events
            log_task.log_group_name.CopyFrom(StringValue(value=log_group_name))
            log_task.region.CopyFrom(StringValue(value=region))
            
            if filter_query:
                log_task.filter_query.CopyFrom(StringValue(value=filter_query))
            
            # Execute task
            result = source_manager.execute_filter_log_events(
                time_range, task, connector
            )
            
            return proto_to_dict(result)
            
        except Exception as e:
            raise TaskExecutionError(f"CloudWatch log events filter failed: {e}")
    
    def ecs_list_clusters(self,
                          start_time: Optional[datetime] = None,
                          end_time: Optional[datetime] = None,
                          duration_minutes: Optional[int] = None) -> Dict[str, Any]:
        """
        List ECS clusters
        
        Args:
            start_time: Start time for the query
            end_time: End time for the query
            duration_minutes: Duration in minutes
            
        Returns:
            ECS clusters list as dictionary
        """
        try:
            source_manager = self._get_source_manager('cloudwatch')
            connector = self._get_connector('cloudwatch')
            time_range = self._create_time_range(start_time, end_time, duration_minutes)
            
            # Create task proto
            from drdroid_debug_toolkit.core.protos.playbooks.source_task_definitions.cloudwatch_task_pb2 import Cloudwatch
            
            task = Cloudwatch()
            task.type = Cloudwatch.TaskType.ECS_LIST_CLUSTERS
            
            # Execute task
            result = source_manager.ecs_list_clusters(
                time_range, task, connector
            )
            
            return proto_to_dict(result)
            
        except Exception as e:
            raise TaskExecutionError(f"CloudWatch ECS clusters list failed: {e}")
    
    def ecs_list_tasks(self,
                       cluster_name: str,
                       start_time: Optional[datetime] = None,
                       end_time: Optional[datetime] = None,
                       duration_minutes: Optional[int] = None) -> Dict[str, Any]:
        """
        List ECS tasks in a cluster
        
        Args:
            cluster_name: Name of the ECS cluster
            start_time: Start time for the query
            end_time: End time for the query
            duration_minutes: Duration in minutes
            
        Returns:
            ECS tasks list as dictionary
        """
        try:
            source_manager = self._get_source_manager('cloudwatch')
            connector = self._get_connector('cloudwatch')
            time_range = self._create_time_range(start_time, end_time, duration_minutes)
            
            # Create task proto
            from drdroid_debug_toolkit.core.protos.playbooks.source_task_definitions.cloudwatch_task_pb2 import Cloudwatch
            from google.protobuf.wrappers_pb2 import StringValue
            
            task = Cloudwatch()
            task.type = Cloudwatch.TaskType.ECS_LIST_TASKS
            
            ecs_task = task.ecs_list_tasks
            ecs_task.cluster_name.CopyFrom(StringValue(value=cluster_name))
            
            # Execute task
            result = source_manager.ecs_list_tasks(
                time_range, task, connector
            )
            
            return proto_to_dict(result)
            
        except Exception as e:
            raise TaskExecutionError(f"CloudWatch ECS tasks list failed: {e}")
    
    def ecs_get_task_logs(self,
                          cluster_name: str,
                          task_definition: Optional[str] = None,
                          max_lines: Optional[int] = None,
                          start_time: Optional[datetime] = None,
                          end_time: Optional[datetime] = None,
                          duration_minutes: Optional[int] = None) -> Dict[str, Any]:
        """
        Get ECS task logs
        
        Args:
            cluster_name: Name of the ECS cluster
            task_definition: Optional task definition filter
            max_lines: Maximum number of log lines to retrieve
            start_time: Start time for the query
            end_time: End time for the query
            duration_minutes: Duration in minutes
            
        Returns:
            ECS task logs as dictionary
        """
        try:
            source_manager = self._get_source_manager('cloudwatch')
            connector = self._get_connector('cloudwatch')
            time_range = self._create_time_range(start_time, end_time, duration_minutes)
            
            # Create task proto
            from drdroid_debug_toolkit.core.protos.playbooks.source_task_definitions.cloudwatch_task_pb2 import Cloudwatch
            from google.protobuf.wrappers_pb2 import StringValue, Int64Value
            
            task = Cloudwatch()
            task.type = Cloudwatch.TaskType.ECS_GET_TASK_LOGS
            
            logs_task = task.ecs_get_task_logs
            logs_task.cluster_name.CopyFrom(StringValue(value=cluster_name))
            
            if task_definition:
                logs_task.task_definition.CopyFrom(StringValue(value=task_definition))
            
            if max_lines:
                logs_task.max_lines.CopyFrom(Int64Value(value=max_lines))
            
            # Execute task
            result = source_manager.ecs_get_task_logs(
                time_range, task, connector
            )
            
            return proto_to_dict(result)
            
        except Exception as e:
            raise TaskExecutionError(f"CloudWatch ECS task logs failed: {e}")
    
    def fetch_dashboard(self,
                       dashboard_name: str,
                       step: Optional[int] = None,
                       start_time: Optional[datetime] = None,
                       end_time: Optional[datetime] = None,
                       duration_minutes: Optional[int] = None) -> Dict[str, Any]:
        """
        Fetch CloudWatch dashboard data
        
        Args:
            dashboard_name: Name of the dashboard
            step: Step interval in seconds
            start_time: Start time for the query
            end_time: End time for the query
            duration_minutes: Duration in minutes
            
        Returns:
            Dashboard data as dictionary
        """
        try:
            source_manager = self._get_source_manager('cloudwatch')
            connector = self._get_connector('cloudwatch')
            time_range = self._create_time_range(start_time, end_time, duration_minutes)
            
            # Create task proto
            from drdroid_debug_toolkit.core.protos.playbooks.source_task_definitions.cloudwatch_task_pb2 import Cloudwatch
            from google.protobuf.wrappers_pb2 import StringValue, Int32Value
            
            task = Cloudwatch()
            task.type = Cloudwatch.TaskType.FETCH_DASHBOARD
            
            dashboard_task = task.fetch_dashboard
            dashboard_task.dashboard_name.CopyFrom(StringValue(value=dashboard_name))
            
            if step:
                dashboard_task.step.CopyFrom(Int32Value(value=step))
            
            # Execute task
            result = source_manager.execute_fetch_dashboard(
                time_range, task, connector
            )
            
            return proto_to_dict(result)
            
        except Exception as e:
            raise TaskExecutionError(f"CloudWatch dashboard fetch failed: {e}")
    
    def fetch_s3_file(self,
                      bucket_name: str,
                      object_key: str,
                      start_time: Optional[datetime] = None,
                      end_time: Optional[datetime] = None,
                      duration_minutes: Optional[int] = None) -> Dict[str, Any]:
        """
        Fetch S3 file content
        
        Args:
            bucket_name: S3 bucket name
            object_key: S3 object key
            start_time: Start time for the query
            end_time: End time for the query
            duration_minutes: Duration in minutes
            
        Returns:
            S3 file content as dictionary
        """
        try:
            source_manager = self._get_source_manager('cloudwatch')
            connector = self._get_connector('cloudwatch')
            time_range = self._create_time_range(start_time, end_time, duration_minutes)
            
            # Create task proto
            from drdroid_debug_toolkit.core.protos.playbooks.source_task_definitions.cloudwatch_task_pb2 import Cloudwatch
            from google.protobuf.wrappers_pb2 import StringValue
            
            task = Cloudwatch()
            task.type = Cloudwatch.TaskType.FETCH_S3_FILE
            
            s3_task = task.fetch_s3_file
            s3_task.bucket_name.CopyFrom(StringValue(value=bucket_name))
            s3_task.object_key.CopyFrom(StringValue(value=object_key))
            
            # Execute task
            result = source_manager.execute_fetch_s3_file(
                time_range, task, connector
            )
            
            return proto_to_dict(result)
            
        except Exception as e:
            raise TaskExecutionError(f"CloudWatch S3 file fetch failed: {e}")
    
    def rds_get_sql_query_performance_stats(self,
                                           db_resource_uri: str,
                                           start_time: Optional[datetime] = None,
                                           end_time: Optional[datetime] = None,
                                           duration_minutes: Optional[int] = None) -> Dict[str, Any]:
        """
        Get RDS SQL query performance stats
        
        Args:
            db_resource_uri: Database resource URI
            start_time: Start time for the query
            end_time: End time for the query
            duration_minutes: Duration in minutes
            
        Returns:
            RDS performance stats as dictionary
        """
        try:
            source_manager = self._get_source_manager('cloudwatch')
            connector = self._get_connector('cloudwatch')
            time_range = self._create_time_range(start_time, end_time, duration_minutes)
            
            # Create task proto
            from drdroid_debug_toolkit.core.protos.playbooks.source_task_definitions.cloudwatch_task_pb2 import Cloudwatch
            from google.protobuf.wrappers_pb2 import StringValue
            
            task = Cloudwatch()
            task.type = Cloudwatch.TaskType.RDS_GET_SQL_QUERY_PERFORMANCE_STATS
            
            rds_task = task.rds_get_sql_query_performance_stats
            rds_task.db_resource_uri.CopyFrom(StringValue(value=db_resource_uri))
            
            # Execute task
            result = source_manager.execute_rds_get_sql_query_performance_stats(
                time_range, task, connector
            )
            
            return proto_to_dict(result)
            
        except Exception as e:
            raise TaskExecutionError(f"CloudWatch RDS performance stats failed: {e}") 