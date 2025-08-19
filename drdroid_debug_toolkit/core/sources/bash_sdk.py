"""
Bash-specific SDK implementation
"""

import logging
from typing import Dict, Any, Optional
from datetime import datetime

from ...exceptions import TaskExecutionError
from ..protos.base_pb2 import Source, SourceKeyType
from ..integrations.source_managers.bash_source_manager import BashSourceManager
from ..utils.proto_utils import proto_to_dict
from ..sdk_base import BaseSDK

logger = logging.getLogger(__name__)


class BashSDK(BaseSDK):
    """
    Bash-specific SDK implementation
    Provides high-level methods for Bash operations
    """
    
    def _initialize_source_managers(self) -> Dict[str, Any]:
        """Initialize Bash source manager"""
        managers = {}
        
        if 'bash' in self.credentials:
            managers['bash'] = BashSourceManager()
            
        return managers
    
    def _get_key_type_mapping(self) -> Dict[str, SourceKeyType]:
        """Get Bash-specific key type mapping"""
        return {
            'remote_host': SourceKeyType.REMOTE_SERVER_HOST,
            'remote_user': SourceKeyType.REMOTE_SERVER_USER,
            'remote_password': SourceKeyType.REMOTE_SERVER_PASSWORD,
            'remote_pem': SourceKeyType.REMOTE_SERVER_PEM,
            'port': SourceKeyType.REMOTE_SERVER_PORT,
        }
    
    def execute_command(self,
                       command: str,
                       remote_server: Optional[str] = None,
                       start_time: Optional[datetime] = None,
                       end_time: Optional[datetime] = None,
                       duration_minutes: Optional[int] = None) -> Dict[str, Any]:
        """
        Execute a Bash command
        
        Args:
            command: Bash command to execute
            remote_server: Optional remote server to execute on
            start_time: Start time for the query (not used for bash but kept for consistency)
            end_time: End time for the query (not used for bash but kept for consistency)
            duration_minutes: Duration in minutes (not used for bash but kept for consistency)
            
        Returns:
            Command execution results as dictionary
        """
        try:
            source_manager = self._get_source_manager('bash')
            connector = self._get_connector('bash')
            time_range = self._create_time_range(start_time, end_time, duration_minutes)
            
            # Create task proto
            from core.protos.playbooks.source_task_definitions.bash_task_pb2 import Bash
            from google.protobuf.wrappers_pb2 import StringValue
            
            task = Bash()
            task.type = Bash.TaskType.COMMAND
            
            command_task = task.command
            command_task.command.CopyFrom(StringValue(value=command))
            
            if remote_server:
                command_task.remote_server.CopyFrom(StringValue(value=remote_server))
            
            # Execute task
            result = source_manager.execute_command(
                time_range, task, connector
            )
            
            return proto_to_dict(result)
            
        except Exception as e:
            raise TaskExecutionError(f"Bash command execution failed: {e}") 