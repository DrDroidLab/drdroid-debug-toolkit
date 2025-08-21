"""
DroidSpace SDK v2 - Improved modular implementation
"""

import logging
from typing import Dict, Any, List, Union
from google.protobuf.struct_pb2 import Struct

from .exceptions import ConfigurationError, ConnectionError, ValidationError, TaskExecutionError
from .core.sdk_factory import SDKFactory
from .core.sources.grafana_sdk import GrafanaSDK
from .core.sources.signoz_sdk import SignozSDK
from .core.sources.newrelic_sdk import NewRelicSDK
from .core.sources.postgres_sdk import PostgresSDK
from .core.sources.posthog_sdk import PostHogSDK
from .core.sources.sql_database_connection_sdk import SqlDatabaseConnectionSDK
from .core.sources.clickhouse_sdk import ClickHouseSDK
from .core.protos.literal_pb2 import LiteralType
from .core.protos.playbooks.playbook_pb2 import PlaybookTask
from .core.utils.proto_utils import dict_to_proto, proto_to_dict
from .core.integrations.utils.executor_utils import check_multiple_task_results

logger = logging.getLogger(__name__)


class DroidSDK:
    """
    Main SDK class for DroidSpace integrations
    Uses factory pattern to manage multiple source-specific SDKs
    Provides dynamic access to source-specific SDKs
    """
    
    def __init__(self, credentials_file_path: str):
        """
        Initialize the SDK with credentials file path
        
        Args:
            credentials_file_path: Path to the YAML credentials file
        """
        self.factory = SDKFactory(credentials_file_path)
    
    # ============================================================================
    # Connection Management
    # ============================================================================
    
    def test_connection(self, source_name: str) -> bool:
        """
        Test connection to the specified source
        
        Args:
            source_name: Name of the source (e.g., 'grafana', 'signoz')
            
        Returns:
            True if connection successful
            
        Raises:
            ConnectionError: If connection fails
        """
        try:
            sdk = self.factory.get_sdk(source_name)
            return sdk.test_connection(source_name)
        except Exception as e:
            raise ConnectionError(f"Connection test failed for {source_name}: {e}")
    
    def test_all_connections(self) -> Dict[str, bool]:
        """
        Test connections for all configured sources
        
        Returns:
            Dictionary mapping source names to connection status
        """
        return self.factory.test_all_connections()
    
    # ============================================================================
    # Source Information
    # ============================================================================
    
    def get_supported_sources(self) -> List[str]:
        """Get list of supported sources"""
        return self.factory.get_available_sources()
    
    def get_configured_sources(self) -> List[str]:
        """Get list of configured sources"""
        # Get from any SDK instance since they all have the same credentials
        if self.factory._sdk_instances:
            first_sdk = next(iter(self.factory._sdk_instances.values()))
            return first_sdk.get_configured_sources()
        return []
    
    def get_sdk_info(self) -> Dict[str, Dict[str, Any]]:
        """
        Get information about all available SDKs
        
        Returns:
            Dictionary with SDK information
        """
        return self.factory.get_sdk_info()
    
    # ============================================================================
    # Dynamic SDK Access
    # ============================================================================
    
    def get_sdk(self, source_name: str) -> Union[GrafanaSDK, SignozSDK, NewRelicSDK, PostgresSDK, PostHogSDK, SqlDatabaseConnectionSDK, ClickHouseSDK]:
        """
        Get source-specific SDK by name
        
        Args:
            source_name: Name of the source (e.g., 'grafana', 'signoz')
            
        Returns:
            Source-specific SDK instance
            
        Raises:
            ValidationError: If source is not supported or not configured
        """
        return self.factory.get_sdk(source_name)
    
    def __getattr__(self, name: str):
        """
        Dynamic attribute access for source-specific SDKs
        Allows accessing SDKs as attributes (e.g., sdk.grafana, sdk.signoz)
        
        Args:
            name: Attribute name (should be a source name)
            
        Returns:
            Source-specific SDK instance
            
        Raises:
            AttributeError: If source is not supported or not configured
        """
        try:
            return self.get_sdk(name)
        except ValidationError as e:
            raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}': {e}")
    
    # ============================================================================
    # Convenience Methods (Optional - for backward compatibility)
    # ============================================================================
    
    def get_grafana_sdk(self) -> GrafanaSDK:
        """Get Grafana SDK instance (convenience method)"""
        return self.get_sdk('grafana')
    
    def get_signoz_sdk(self) -> SignozSDK:
        """Get Signoz SDK instance (convenience method)"""
        return self.get_sdk('signoz')

    def get_newrelic_sdk(self) -> NewRelicSDK:
        """Get NewRelic SDK instance (convenience method)"""
        return self.get_sdk('newrelic')
    
    def get_postgres_sdk(self) -> PostgresSDK:
        """Get Postgres SDK instance (convenience method)"""
        return self.get_sdk('postgres')
    
    def get_posthog_sdk(self) -> PostHogSDK:
        """Get PostHog SDK instance (convenience method)"""
        return self.get_sdk('posthog')
    
    def get_sql_database_connection_sdk(self) -> SqlDatabaseConnectionSDK:
        """Get SQL Database Connection SDK instance (convenience method)"""
        return self.get_sdk('sql_database_connection')
    
    def get_clickhouse_sdk(self) -> ClickHouseSDK:
        """Get ClickHouse SDK instance (convenience method)"""
        return self.get_sdk('clickhouse')
    
    # ============================================================================
    # Generic Task Execution (Advanced Usage)
    # ============================================================================
    
    def execute_task(self, source_name: str, task_name: str, **kwargs) -> Any:
        """
        Execute a task on a specific source dynamically
        
        Args:
            source_name: Name of the source (e.g., 'grafana', 'signoz')
            task_name: Name of the task method to execute
            **kwargs: Arguments to pass to the task method
            
        Returns:
            Task execution result
            
        Raises:
            ValidationError: If source or task is not found
            TaskExecutionError: If task execution fails
        """
        try:
            sdk = self.get_sdk(source_name)
            
            if not hasattr(sdk, task_name):
                available_methods = [method for method in dir(sdk) 
                                   if not method.startswith('_') and callable(getattr(sdk, method))]
                raise ValidationError(
                    f"Task '{task_name}' not found in {source_name} SDK. "
                    f"Available methods: {available_methods}"
                )
            
            method = getattr(sdk, task_name)
            return method(**kwargs)
            
        except ValidationError:
            raise
        except Exception as e:
            raise TaskExecutionError(f"Task execution failed: {e}")
    
    def list_available_tasks(self, source_name: str) -> List[str]:
        """
        List available tasks for a specific source
        
        Args:
            source_name: Name of the source
            
        Returns:
            List of available task method names
        """
        try:
            sdk = self.get_sdk(source_name)
            return [method for method in dir(sdk) 
                   if not method.startswith('_') and callable(getattr(sdk, method))]
        except Exception as e:
            logger.error(f"Failed to list tasks for {source_name}: {e}")
            return [] 

    # ============================================================================
    # LLM Tooling
    # ============================================================================

    def _convert_literal_type_to_json_type(self, literal_type: Any) -> str:
        """Convert protobuf LiteralType to JSON Schema type string.

        Fallbacks to "string" for unknown/unsupported types.
        """
        try:
            if literal_type == LiteralType.STRING:
                return "string"
            if literal_type == LiteralType.LONG:
                return "integer"
            if literal_type == LiteralType.DOUBLE:
                return "number"
            if literal_type == LiteralType.BOOLEAN:
                return "boolean"
            if literal_type == LiteralType.STRING_ARRAY:
                return "array"
            return "string"
        except Exception:
            return "string"

    def _convert_form_field_to_json_schema(self, field: Any) -> Dict[str, Any]:
        """Convert a FormField proto to JSON Schema property definition."""
        try:
            description = ""
            try:
                if getattr(field, "description", None) and getattr(field.description, "value", None):
                    description = field.description.value
                elif getattr(field, "display_name", None) and getattr(field.display_name, "value", None):
                    description = field.display_name.value
            except Exception:
                description = ""

            field_schema: Dict[str, Any] = {
                "type": self._convert_literal_type_to_json_type(getattr(field, "data_type", None)),
                "description": description,
            }

            # Handle array item typing for STRING_ARRAY
            if getattr(field, "data_type", None) == LiteralType.STRING_ARRAY:
                field_schema["items"] = {"type": "string"}

            # Default value (Literal)
            try:
                if hasattr(field, "HasField") and field.HasField("default_value"):
                    default_val = field.default_value
                    if default_val.type == LiteralType.STRING and getattr(default_val, "string", None):
                        field_schema["default"] = default_val.string.value
                    elif default_val.type == LiteralType.LONG and getattr(default_val, "long", None):
                        field_schema["default"] = default_val.long.value
                    elif default_val.type == LiteralType.BOOLEAN and getattr(default_val, "boolean", None):
                        field_schema["default"] = default_val.boolean.value
                    elif default_val.type == LiteralType.DOUBLE and getattr(default_val, "double", None):
                        field_schema["default"] = default_val.double.value
            except Exception:
                # best-effort; ignore default extraction failures
                pass

            return field_schema
        except Exception as e:
            logger.error(f"Error converting form field: {e}")
            return {"type": "string", "description": ""}

    def get_llm_tools(self, source_name: str) -> List[Dict[str, Any]]:
        """
        Generate LLM-friendly tool definitions for a given source.

        The returned format is suitable for passing directly to an LLM that
        supports tool/function calling. Each tool corresponds to a task defined
        by the underlying SourceManager for the given source.

        Args:
            source_name: Name of the source (e.g., "grafana")

        Returns:
            A list of dicts with keys: name, description, parameters
        """
        try:
            sdk = self.get_sdk(source_name)
            # Prefer using the BaseSDK helper to retrieve the proper manager instance
            source_manager = sdk._get_source_manager(source_name)

            # Build a clean source prefix for tool names
            source_prefix = source_name.lower().replace(" ", "_").replace("-", "_")

            tools: List[Dict[str, Any]] = []

            task_type_callable_map = getattr(source_manager, "task_type_callable_map", {})
            task_proto = getattr(source_manager, "task_proto", None)

            for task_type, task_info in task_type_callable_map.items():
                try:
                    # Derive a human-readable task type name
                    task_type_name = str(task_type).lower()
                    if task_proto is not None and hasattr(task_proto, "TaskType") and hasattr(task_proto.TaskType, "Name"):
                        try:
                            task_type_name = task_proto.TaskType.Name(task_type).lower()
                            if task_type_name.startswith("task_type_"):
                                task_type_name = task_type_name[10:]
                            elif task_type_name.startswith("task_"):
                                task_type_name = task_type_name[5:]
                        except Exception:
                            pass

                    tool_name = f"{source_prefix}_{task_type_name}"

                    # Safety truncate
                    if len(tool_name) > 60:
                        tool_name = tool_name[:60]

                    # Convert form fields to JSON Schema
                    properties: Dict[str, Any] = {}
                    required: List[str] = []

                    for field in task_info.get("form_fields", []):
                        try:
                            field_key = (
                                field.key_name.value
                                if getattr(field, "key_name", None) and getattr(field.key_name, "value", None)
                                else f"field_{len(properties)}"
                            )
                            field_schema = self._convert_form_field_to_json_schema(field)
                            properties[field_key] = field_schema

                            # Required if not is_optional (default to required if flag missing/False)
                            is_optional = bool(getattr(field, "is_optional", False))
                            if not is_optional:
                                required.append(field_key)
                        except Exception as field_err:
                            logger.error(f"Error processing form field for tool '{tool_name}': {field_err}")
                            continue

                    tool_def = {
                        "name": tool_name,
                        "description": task_info.get("display_name", f"{source_name.title()} task"),
                        "parameters": {
                            "type": "object",
                            "properties": properties,
                            "required": required,
                        },
                    }

                    tools.append(tool_def)
                except Exception as task_err:
                    logger.error(f"Error creating LLM tool for task_type {task_type}: {task_err}")
                    continue

            return tools
        except Exception as e:
            logger.error(f"Failed to generate LLM tools for {source_name}: {e}")
            return []

    def get_openai_functions(self, source_name: str) -> List[Dict[str, Any]]:
        """
        Generate OpenAI function format tools for a given source.
        
        Returns tools in the OpenAI function calling format, eliminating the need
        for manual conversion loops.
        
        Args:
            source_name: Name of the source (e.g., "grafana")
            
        Returns:
            A list of OpenAI function format dicts with structure:
            {
                "type": "function",
                "function": {
                    "name": "tool_name",
                    "description": "tool_description", 
                    "parameters": {...}
                }
            }
        """
        tools = self.get_llm_tools(source_name)
        return [
            {
                "type": "function",
                "function": {
                    "name": tool["name"],
                    "description": tool["description"],
                    "parameters": tool["parameters"]
                }
            }
            for tool in tools
        ]

    def execute_llm_tool(self, source_name: str, tool_name: str, **kwargs) -> Dict[str, Any]:
        """
        Execute an LLM tool by name with the provided arguments.
        
        This method allows an LLM to execute any tool returned by get_llm_tools().
        The tool_name should match the "name" field from the tool definition.
        
        Args:
            source_name: Name of the source (e.g., "grafana")
            tool_name: Name of the tool to execute (e.g., "grafana_datasource_query_execution")
            **kwargs: Arguments to pass to the tool
            
        Returns:
            Dictionary containing the execution result
            
        Raises:
            ValidationError: If source or tool is not found
            TaskExecutionError: If tool execution fails
        """
        try:
            # Get the SDK for the source
            sdk = self.get_sdk(source_name)
            source_manager = sdk._get_source_manager(source_name)
            
            # Extract task type from tool name
            # Tool name format: {source}_{task_type}
            task_type_name = tool_name.replace(f"{source_name.lower()}_", "", 1)
            
            # Find the matching task type in the source manager
            task_type_callable_map = getattr(source_manager, "task_type_callable_map", {})
            task_proto = getattr(source_manager, "task_proto", None)
            
            matching_task_type = None
            for task_type, task_info in task_type_callable_map.items():
                # Try to match by task type name
                try:
                    if task_proto and hasattr(task_proto, "TaskType") and hasattr(task_proto.TaskType, "Name"):
                        current_task_name = task_proto.TaskType.Name(task_type).lower()
                        if current_task_name.startswith("task_type_"):
                            current_task_name = current_task_name[10:]
                        elif current_task_name.startswith("task_"):
                            current_task_name = current_task_name[5:]
                        
                        if current_task_name == task_type_name:
                            matching_task_type = task_type
                            break
                except Exception:
                    pass
                
                # Fallback: try string comparison
                if str(task_type).lower() == task_type_name:
                    matching_task_type = task_type
                    break
            
            if matching_task_type is None:
                available_tools = [f"{source_name}_{str(t).lower()}" for t in task_type_callable_map.keys()]
                raise ValidationError(
                    f"Tool '{tool_name}' not found for source '{source_name}'. "
                    f"Available tools: {available_tools}"
                )
            
            # Build a PlaybookTask for execution through the SourceManager
            # Create minimal task dict structure expected by SourceManager.get_resolved_task
            # 1) Determine source enum value
            from .core.protos.base_pb2 import Source as SourceEnum, TimeRange
            try:
                source_enum_value = getattr(SourceEnum, source_name.upper())
            except AttributeError:
                raise ValidationError(f"Unsupported source '{source_name}'")

            # 2) Prepare task dictionary
            # Map kwargs directly as the task-type specific message payload
            source_key = source_name.lower()
            task_dict = {
                "source": source_enum_value,
                source_key: {
                    "type": matching_task_type,
                    task_type_name: kwargs or {}
                }
            }

            # 3) Convert to PlaybookTask proto
            task_proto: PlaybookTask = dict_to_proto(task_dict, PlaybookTask)

            # 4) Create a default time range (last 1 hour)
            from datetime import datetime, timedelta
            end_time = int(datetime.now().timestamp())
            start_time = int((datetime.now() - timedelta(hours=1)).timestamp())
            tr = TimeRange(time_geq=start_time, time_lt=end_time)

            # 5) Resolve task and execute via SourceManager
            resolved_task, resolved_source_task, task_local_variable_map = source_manager.get_resolved_task(
                Struct(), task_proto
            )

            playbook_task_result = source_manager.task_type_callable_map[matching_task_type]['executor'](
                tr, resolved_source_task, sdk._get_connector(source_name)
            )

            # 6) Post-process result and return as dict
            if check_multiple_task_results(playbook_task_result):
                task_results = []
                for result in playbook_task_result:
                    processed_result = source_manager.postprocess_task_result(
                        result, resolved_task, task_local_variable_map
                    )
                    task_results.append(processed_result)
                return {"results": [proto_to_dict(r) for r in task_results]}
            else:
                processed_result = source_manager.postprocess_task_result(
                    playbook_task_result, resolved_task, task_local_variable_map
                )
                return proto_to_dict(processed_result)
            
        except ValidationError:
            raise
        except Exception as e:
            raise TaskExecutionError(f"LLM tool execution failed: {e}")