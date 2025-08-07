"""
Template for creating new source-specific SDK implementations
Copy this file and modify it for your specific source
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime

from drdroid_sdk.exceptions import TaskExecutionError
from drdroid_sdk.core.protos.base_pb2 import Source, SourceKeyType
# Import your source manager here
# from drdroid_sdk.core.integrations.source_managers.your_source_manager import YourSourceManager
from drdroid_sdk.core.utils.proto_utils import proto_to_dict
from drdroid_sdk.core.sdk_base import BaseSDK

logger = logging.getLogger(__name__)


class TemplateSDK(BaseSDK):
    """
    Template SDK implementation
    Replace 'Template' with your source name (e.g., 'DatadogSDK', 'NewRelicSDK')
    """
    
    def _initialize_source_managers(self) -> Dict[str, Any]:
        """Initialize your source manager"""
        managers = {}
        
        if 'your_source_name' in self.credentials:  # Replace with actual source name
            # managers['your_source_name'] = YourSourceManager()
            pass
            
        return managers
    
    def _get_key_type_mapping(self) -> Dict[str, SourceKeyType]:
        """Get your source-specific key type mapping"""
        return {
            # Map your configuration keys to SourceKeyType
            # 'your_config_key': SourceKeyType.YOUR_SOURCE_KEY_TYPE,
        }
    
    def your_method_name(self,
                        param1: str,
                        param2: Optional[int] = None,
                        start_time: Optional[datetime] = None,
                        end_time: Optional[datetime] = None,
                        duration_minutes: Optional[int] = None) -> Dict[str, Any]:
        """
        Your method description
        
        Args:
            param1: Description of param1
            param2: Description of param2
            start_time: Start time for the query
            end_time: End time for the query
            duration_minutes: Duration in minutes
            
        Returns:
            Query results as dictionary
        """
        try:
            source_manager = self._get_source_manager('your_source_name')
            connector = self._get_connector('your_source_name')
            time_range = self._create_time_range(start_time, end_time, duration_minutes)
            
            # Create task proto
            # from ..protos.playbooks.source_task_definitions.your_source_task_pb2 import YourSource
            # from google.protobuf.wrappers_pb2 import StringValue, Int32Value
            
            # task = YourSource()
            # task.type = YourSource.TaskType.YOUR_TASK_TYPE
            
            # your_task = task.your_task_field
            # your_task.param1.CopyFrom(StringValue(value=param1))
            
            # if param2:
            #     your_task.param2.CopyFrom(Int32Value(value=param2))
            
            # Execute task
            # result = source_manager.execute_your_method(
            #     time_range, task, connector
            # )
            
            # return proto_to_dict(result)
            
            # Placeholder return
            return {"message": "Template method - implement your logic here"}
            
        except Exception as e:
            raise TaskExecutionError(f"Your source method failed: {e}")
    
    def another_method(self, param: str) -> List[Dict[str, Any]]:
        """
        Another example method
        
        Args:
            param: Description of parameter
            
        Returns:
            List of results as dictionaries
        """
        try:
            source_manager = self._get_source_manager('your_source_name')
            connector = self._get_connector('your_source_name')
            time_range = self._create_time_range()
            
            # Implement your logic here
            # ...
            
            # Placeholder return
            return [{"message": "Template method - implement your logic here"}]
            
        except Exception as e:
            raise TaskExecutionError(f"Your source method failed: {e}")


# Steps to create a new source SDK:
# 1. Copy this template file and rename it to your_source_sdk.py
# 2. Replace 'TemplateSDK' with your source name (e.g., 'DatadogSDK')
# 3. Import your source manager in _initialize_source_managers
# 4. Define your key type mapping in _get_key_type_mapping
# 5. Implement your source-specific methods
# 6. Add your SDK to the SDKFactory registry in sdk_factory.py
# 7. Add your SDK to the sources/__init__.py file
# 8. Add wrapper methods to the main DroidSDK class in sdk_v2.py 