"""
Kubernetes-specific SDK implementation
"""

import logging
from typing import Dict, Any, Optional
from datetime import datetime

from drdroid_sdk.exceptions import TaskExecutionError
from drdroid_sdk.core.protos.base_pb2 import Source, SourceKeyType
from drdroid_sdk.core.integrations.source_managers.kubernetes_source_manager import KubernetesSourceManager
from drdroid_sdk.core.utils.proto_utils import proto_to_dict
from drdroid_sdk.core.sdk_base import BaseSDK

logger = logging.getLogger(__name__)


class KubernetesSDK(BaseSDK):
    """
    Kubernetes-specific SDK implementation
    Provides high-level methods for Kubernetes operations
    """
    
    def _initialize_source_managers(self) -> Dict[str, Any]:
        """Initialize Kubernetes source manager"""
        managers = {}
        
        if 'kubernetes' in self.credentials:
            managers['kubernetes'] = KubernetesSourceManager()
            
        return managers
    
    def _get_key_type_mapping(self) -> Dict[str, SourceKeyType]:
        """Get Kubernetes-specific key type mapping"""
        return {
            'kubeconfig': SourceKeyType.KUBECONFIG,
            'context': SourceKeyType.CONTEXT,
            'namespace': SourceKeyType.NAMESPACE,
        }
    
    def execute_command(self,
                       command: str,
                       start_time: Optional[datetime] = None,
                       end_time: Optional[datetime] = None,
                       duration_minutes: Optional[int] = None) -> Dict[str, Any]:
        """
        Execute a kubectl command
        
        Args:
            command: kubectl command to execute
            start_time: Start time for the query (not used for k8s but kept for consistency)
            end_time: End time for the query (not used for k8s but kept for consistency)
            duration_minutes: Duration in minutes (not used for k8s but kept for consistency)
            
        Returns:
            Command execution results as dictionary
        """
        try:
            source_manager = self._get_source_manager('kubernetes')
            connector = self._get_connector('kubernetes')
            time_range = self._create_time_range(start_time, end_time, duration_minutes)
            
            # Create task proto
            from drdroid_sdk.core.protos.playbooks.source_task_definitions.kubectl_task_pb2 import Kubectl
            from google.protobuf.wrappers_pb2 import StringValue
            
            task = Kubectl()
            task.type = Kubectl.TaskType.COMMAND
            
            command_task = task.command
            command_task.command.CopyFrom(StringValue(value=command))
            
            # Execute task
            result = source_manager.execute_command(
                time_range, task, connector
            )
            
            return proto_to_dict(result)
            
        except Exception as e:
            raise TaskExecutionError(f"Kubernetes command execution failed: {e}")
    
    def execute_write_command(self,
                             command: str,
                             start_time: Optional[datetime] = None,
                             end_time: Optional[datetime] = None,
                             duration_minutes: Optional[int] = None) -> Dict[str, Any]:
        """
        Execute a kubectl write command (apply, scale, delete, etc.)
        
        Args:
            command: kubectl write command to execute
            start_time: Start time for the query (not used for k8s but kept for consistency)
            end_time: End time for the query (not used for k8s but kept for consistency)
            duration_minutes: Duration in minutes (not used for k8s but kept for consistency)
            
        Returns:
            Command execution results as dictionary
        """
        try:
            source_manager = self._get_source_manager('kubernetes')
            connector = self._get_connector('kubernetes')
            time_range = self._create_time_range(start_time, end_time, duration_minutes)
            
            # Create task proto
            from drdroid_sdk.core.protos.playbooks.source_task_definitions.kubectl_task_pb2 import Kubectl
            from google.protobuf.wrappers_pb2 import StringValue
            
            task = Kubectl()
            task.type = Kubectl.TaskType.K8S_WRITE_COMMAND
            
            write_command_task = task.k8s_write_command
            write_command_task.command.CopyFrom(StringValue(value=command))
            
            # Execute task
            result = source_manager.execute_write_command(
                time_range, task, connector
            )
            
            return proto_to_dict(result)
            
        except Exception as e:
            raise TaskExecutionError(f"Kubernetes write command execution failed: {e}") 