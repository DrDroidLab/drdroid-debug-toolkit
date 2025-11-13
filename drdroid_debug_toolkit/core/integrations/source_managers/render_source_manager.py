import logging
from google.protobuf.struct_pb2 import Struct
from google.protobuf.wrappers_pb2 import StringValue, BoolValue, UInt64Value

from drdroid_debug_toolkit.core.integrations.source_api_processors.render_api_processor import RenderAPIProcessor
from drdroid_debug_toolkit.core.integrations.source_manager import SourceManager
from drdroid_debug_toolkit.core.protos.base_pb2 import TimeRange
from drdroid_debug_toolkit.core.protos.connectors.connector_pb2 import Connector as ConnectorProto
from drdroid_debug_toolkit.core.protos.base_pb2 import SourceKeyType, Source
from drdroid_debug_toolkit.core.protos.literal_pb2 import LiteralType, Literal
from drdroid_debug_toolkit.core.protos.playbooks.playbook_commons_pb2 import PlaybookTaskResult, PlaybookTaskResultType, ApiResponseResult
from drdroid_debug_toolkit.core.protos.playbooks.playbook_pb2 import PlaybookTask
from drdroid_debug_toolkit.core.protos.playbooks.source_task_definitions.render_task_pb2 import Render
from drdroid_debug_toolkit.core.protos.ui_definition_pb2 import FormField, FormFieldType
from utils.proto_utils import proto_to_dict, dict_to_proto
from core.utils.credentilal_utils import DISPLAY_NAME, CATEGORY, CLOUD_MANAGED_SERVICES, get_connector_key_type_string, generate_credentials_dict

logger = logging.getLogger(__name__)


class RenderSourceManager(SourceManager):
    def __init__(self):
        self.source = Source.RENDER
        self.task_proto = Render

        
        self.task_type_callable_map = {
            Render.TaskType.LIST_DEPLOYS: {
                'executor': self.list_deploys,
                'model_types': [],
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'List Deployments',
                'category': 'Deployments',
                'form_fields': [
                    FormField(key_name=StringValue(value="service_id"),
                              display_name=StringValue(value="Service ID"),
                              description=StringValue(value='Enter the Render service ID'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                ]
            },
            Render.TaskType.GET_DEPLOY: {
                'executor': self.get_deploy,
                'model_types': [],
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Get Deployment Details',
                'category': 'Deployments',
                'form_fields': [
                    FormField(key_name=StringValue(value="service_id"),
                              display_name=StringValue(value="Service ID"),
                              description=StringValue(value='Enter the Render service ID'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="deploy_id"),
                              display_name=StringValue(value="Deployment ID"),
                              description=StringValue(value='Enter the deployment ID'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                ]
            },
            Render.TaskType.LIST_SERVICES: {
                'executor': self.list_services,
                'model_types': [],
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'List Services',
                'category': 'Services',
                'form_fields': [
                    FormField(key_name=StringValue(value="include_previews"),
                              display_name=StringValue(value="Include Preview Services"),
                              description=StringValue(value='Whether to include preview services'),
                              data_type=LiteralType.BOOLEAN,
                              default_value=Literal(type=LiteralType.BOOLEAN, boolean=BoolValue(value=False)),
                              form_field_type=FormFieldType.CHECKBOX_FT,
                              is_optional=True),
                ]
            },
            Render.TaskType.GET_SERVICE: {
                'executor': self.get_service,
                'model_types': [],
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Get Service Details',
                'category': 'Services',
                'form_fields': [
                    FormField(key_name=StringValue(value="service_id"),
                              display_name=StringValue(value="Service ID"),
                              description=StringValue(value='Enter the Render service ID'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                ]
            },
            Render.TaskType.FETCH_LOGS: {
                'executor': self.fetch_logs,
                'model_types': [],
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Fetch Service Logs',
                'category': 'Logs',
                'form_fields': [
                    FormField(key_name=StringValue(value="service_id"),
                              display_name=StringValue(value="Service ID"),
                              description=StringValue(value='Enter the Render service ID'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="start_time"),
                              display_name=StringValue(value="Start Time"),
                              description=StringValue(value='Start time for logs in ISO format (optional)'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT,
                              is_optional=True),
                    FormField(key_name=StringValue(value="end_time"),
                              display_name=StringValue(value="End Time"),
                              description=StringValue(value='End time for logs in ISO format (optional)'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT,
                              is_optional=True),
                    FormField(key_name=StringValue(value="limit"),
                              display_name=StringValue(value="Limit"),
                              description=StringValue(value='Maximum number of log entries to fetch (1-100, optional, defaults to 20)'),
                              data_type=LiteralType.LONG,
                              form_field_type=FormFieldType.TEXT_FT,
                              is_optional=True),
                    FormField(key_name=StringValue(value="instance"),
                              display_name=StringValue(value="Instance"),
                              description=StringValue(value='Filter logs by instance ID(s) - array of strings'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.STRING_ARRAY_FT,
                              is_optional=True),
                    FormField(key_name=StringValue(value="host"),
                              display_name=StringValue(value="Host"),
                              description=StringValue(value='Filter request logs by host - supports wildcards and regex'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.STRING_ARRAY_FT,
                              is_optional=True),
                    FormField(key_name=StringValue(value="status_code"),
                              display_name=StringValue(value="Status Code"),
                              description=StringValue(value='Filter request logs by status code - supports wildcards and regex'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.STRING_ARRAY_FT,
                              is_optional=True),
                    FormField(key_name=StringValue(value="method"),
                              display_name=StringValue(value="Method"),
                              description=StringValue(value='Filter request logs by HTTP method (GET, POST, etc.)'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.STRING_ARRAY_FT,
                              is_optional=True),
                    FormField(key_name=StringValue(value="task"),
                              display_name=StringValue(value="Task"),
                              description=StringValue(value='Filter logs by task(s)'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.STRING_ARRAY_FT,
                              is_optional=True),
                    FormField(key_name=StringValue(value="task_run"),
                              display_name=StringValue(value="Task Run"),
                              description=StringValue(value='Filter logs by task run ID(s)'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.STRING_ARRAY_FT,
                              is_optional=True),
                    FormField(key_name=StringValue(value="level"),
                              display_name=StringValue(value="Level"),
                              description=StringValue(value='Filter logs by severity level - supports wildcards and regex'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.STRING_ARRAY_FT,
                              is_optional=True),
                    FormField(key_name=StringValue(value="type"),
                              display_name=StringValue(value="Type"),
                              description=StringValue(value='Filter logs by type (app, request, build, etc.)'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.STRING_ARRAY_FT,
                              is_optional=True),
                    FormField(key_name=StringValue(value="text"),
                              display_name=StringValue(value="Text"),
                              description=StringValue(value='Filter by log text content - supports wildcards and regex'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.STRING_ARRAY_FT,
                              is_optional=True),
                    FormField(key_name=StringValue(value="path"),
                              display_name=StringValue(value="Path"),
                              description=StringValue(value='Filter request logs by path - supports wildcards and regex'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.STRING_ARRAY_FT,
                              is_optional=True),
                ]
            },
        }
        self.connector_form_configs = [
            {
                "name": StringValue(value="Render API Key Connection"),
                "description": StringValue(value="Connect to Render using your API Key."),
                "form_fields": {
                    SourceKeyType.RENDER_API_KEY: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.RENDER_API_KEY)),
                        display_name=StringValue(value="API Key"),
                        description=StringValue(value="Enter your Render API Key"),
                        helper_text=StringValue(value="Get your API key from Render Dashboard > Account Settings > API Keys"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=False,
                        is_sensitive=True
                    )
                }
            }
        ]
        self.connector_type_details = {
            DISPLAY_NAME: "Render",
            CATEGORY: CLOUD_MANAGED_SERVICES,
        }
        


    def _get_api_processor(self, connector_proto: ConnectorProto) -> RenderAPIProcessor:
        """Get the Render API processor with credentials from the connector."""
        if not connector_proto.keys:
            raise Exception("Render connector keys are required")
        
        # Find the RENDER_API_KEY in the connector keys
        api_key = None
        for key in connector_proto.keys:
            if key.key_type == SourceKeyType.RENDER_API_KEY and key.key.value:
                api_key = key.key.value
                break
        
        if not api_key:
            raise Exception("Render API key is required in connector keys")
        
        return RenderAPIProcessor(api_key)

    def get_connector_processor(self, render_connector, **kwargs):
        """Get the Render API processor with credentials from the connector."""
        generated_credentials = generate_credentials_dict(render_connector.type, render_connector.keys)
        if 'api_key' not in generated_credentials:
            raise Exception("Render API key is required in connector keys")
        return RenderAPIProcessor(generated_credentials['api_key'])

    def get_resolved_task(self, global_variable_set: Struct, input_task: PlaybookTask):
        """Resolve the task by extracting the render-specific task data."""
        source = input_task.source
        if not source or source == Source.UNKNOWN or source != self.source:
            raise Exception("RenderSourceManager.get_resolved_task:: Applicable Source not found for task")
        
        source_str = Source.Name(source).lower()
        
        task_dict = proto_to_dict(input_task)
        
        source_task_dict = task_dict.get(source_str, {})
        
        if not source_task_dict:
            raise Exception(f"RenderSourceManager.get_resolved_task:: No task definition found for: {source_str}")
        
        source_task_proto = dict_to_proto(source_task_dict, self.task_proto)
        
        task_type = source_task_proto.type
        
        if task_type not in self.task_type_callable_map:
            raise Exception(f"RenderSourceManager.get_resolved_task:: Task type {task_type} not supported for source: {source_str}")
        
        task_type_name = self.task_proto.TaskType.Name(task_type).lower()
        
        source_task_type_dict = source_task_dict.get(task_type_name, {})
        
        if 'form_fields' not in self.task_type_callable_map[task_type]:
            raise Exception(f"RenderSourceManager.get_resolved_task:: Form fields not found for task type: {task_type_name}")
        
        # Check if form fields are required but not provided
        form_fields = self.task_type_callable_map[task_type]['form_fields']
        required_fields = [field for field in form_fields if not field.is_optional]
        
        if not source_task_type_dict and required_fields:
            raise Exception(f"RenderSourceManager.get_resolved_task:: Required fields missing for task type: {task_type_name}")
        
        # For now, return the task as-is since we don't have complex global variable resolution
        return input_task, source_task_proto, {}

    def list_deploys(self, time_range: TimeRange, render_task: Render, connector_proto: ConnectorProto):
        """Execute list deploys task."""
        try:
            task_data = render_task.list_deploys
            service_id = task_data.service_id.value
            
            if not service_id:
                raise Exception("Service ID is required")
            
            api_processor = self._get_api_processor(connector_proto)
            result = api_processor.list_deploys(service_id)
            
            # Convert result to protobuf struct
            result_struct = Struct()
            # Convert list result to dictionary format if needed
            if isinstance(result, list):
                result_dict = {"deploys": result, "count": len(result)}
            else:
                result_dict = result
            result_struct.update(result_dict)
            
            api_response = ApiResponseResult(
                request_method=StringValue(value="GET"),
                request_url=StringValue(value=f"/services/{service_id}/deploys"),
                response_status=UInt64Value(value=200),
                response_body=result_struct
            )
            
            return PlaybookTaskResult(
                source=self.source,
                type=PlaybookTaskResultType.API_RESPONSE,
                api_response=api_response,
            )
        except Exception as e:
            raise Exception(f"Error executing list deploys task: {e}")

    def get_deploy(self, time_range: TimeRange, render_task: Render, connector_proto: ConnectorProto):
        """Execute get deploy task."""
        try:
            task_data = render_task.get_deploy
            service_id = task_data.service_id.value
            deploy_id = task_data.deploy_id.value
            
            if not service_id:
                raise Exception("Service ID is required")
            if not deploy_id:
                raise Exception("Deployment ID is required")
            
            api_processor = self._get_api_processor(connector_proto)
            result = api_processor.get_deploy(service_id, deploy_id)
            
            # Convert result to protobuf struct
            result_struct = Struct()
            # Convert list result to dictionary format if needed
            if isinstance(result, list):
                result_dict = {"deploy": result, "count": len(result)}
            else:
                result_dict = result
            result_struct.update(result_dict)
            
            api_response = ApiResponseResult(
                request_method=StringValue(value="GET"),
                request_url=StringValue(value=f"/services/{service_id}/deploys/{deploy_id}"),
                response_status=UInt64Value(value=200),
                response_body=result_struct
            )
            
            return PlaybookTaskResult(
                source=self.source,
                type=PlaybookTaskResultType.API_RESPONSE,
                api_response=api_response,
            )
        except Exception as e:
            raise Exception(f"Error executing get deploy task: {e}")

    def list_services(self, time_range: TimeRange, render_task: Render, connector_proto: ConnectorProto):
        """Execute list services task."""
        try:
            task_data = render_task.list_services
            include_previews = task_data.include_previews.value if task_data.include_previews else False
            
            api_processor = self._get_api_processor(connector_proto)
            result = api_processor.list_services(include_previews)
            
            # Convert result to protobuf struct
            result_struct = Struct()
            # Convert list result to dictionary format
            if isinstance(result, list):
                result_dict = {"services": result, "count": len(result)}
            else:
                result_dict = result
            result_struct.update(result_dict)
            
            api_response = ApiResponseResult(
                request_method=StringValue(value="GET"),
                request_url=StringValue(value="/services"),
                response_status=UInt64Value(value=200),
                response_body=result_struct
            )
            
            return PlaybookTaskResult(
                source=self.source,
                type=PlaybookTaskResultType.API_RESPONSE,
                api_response=api_response,
            )
            
        except Exception as e:
            raise Exception(f"Error executing list services task: {e}")

    def get_service(self, time_range: TimeRange, render_task: Render, connector_proto: ConnectorProto):
        """Execute get service task."""
        try:
            task_data = render_task.get_service
            service_id = task_data.service_id.value
            
            if not service_id:
                raise Exception("Service ID is required")
            
            api_processor = self._get_api_processor(connector_proto)
            result = api_processor.get_service(service_id)
            
            # Convert result to protobuf struct
            result_struct = Struct()
            # Convert list result to dictionary format if needed
            if isinstance(result, list):
                result_dict = {"service": result, "count": len(result)}
            else:
                result_dict = result
            result_struct.update(result_dict)
            
            api_response = ApiResponseResult(
                request_method=StringValue(value="GET"),
                request_url=StringValue(value=f"/services/{service_id}"),
                response_status=UInt64Value(value=200),
                response_body=result_struct
            )
            
            return PlaybookTaskResult(
                source=self.source,
                type=PlaybookTaskResultType.API_RESPONSE,
                api_response=api_response,
            )
        except Exception as e:
            raise Exception(f"Error executing get service task: {e}")

    def fetch_logs(self, time_range: TimeRange, render_task: Render, connector_proto: ConnectorProto):
        """Execute fetch logs task."""
        try:
            task_data = render_task.fetch_logs
            service_id = task_data.service_id.value
            
            if not service_id:
                raise Exception("Service ID is required")
            
            start_time = task_data.start_time.value if task_data.start_time else None
            end_time = task_data.end_time.value if task_data.end_time else None
            limit = task_data.limit.value if task_data.limit else None
            
            # Extract filter parameters (repeated string fields)
            instance = list(task_data.instance) if task_data.instance else None
            host = list(task_data.host) if task_data.host else None
            status_code = list(task_data.status_code) if task_data.status_code else None
            method = list(task_data.method) if task_data.method else None
            task = list(task_data.task) if task_data.task else None
            task_run = list(task_data.task_run) if task_data.task_run else None
            level = list(task_data.level) if task_data.level else None
            type = list(task_data.type) if task_data.type else None
            text = list(task_data.text) if task_data.text else None
            path = list(task_data.path) if task_data.path else None
            
            api_processor = self._get_api_processor(connector_proto)
            result = api_processor.fetch_logs(
                service_id, start_time, end_time, limit,
                instance=instance, host=host, status_code=status_code, method=method,
                task=task, task_run=task_run, level=level, type=type, text=text, path=path
            )
            
            # Convert result to protobuf struct
            result_struct = Struct()
            # Convert list result to dictionary format if needed
            if isinstance(result, list):
                result_dict = {"logs": result, "count": len(result)}
            else:
                result_dict = result
            result_struct.update(result_dict)
            
            api_response = ApiResponseResult(
                request_method=StringValue(value="GET"),
                request_url=StringValue(value=f"/services/{service_id}/logs"),
                response_status=UInt64Value(value=200),
                response_body=result_struct
            )
            
            return PlaybookTaskResult(
                source=self.source,
                type=PlaybookTaskResultType.API_RESPONSE,
                api_response=api_response,
            )
        except Exception as e:
            raise Exception(f"Error executing fetch logs task: {e}")
