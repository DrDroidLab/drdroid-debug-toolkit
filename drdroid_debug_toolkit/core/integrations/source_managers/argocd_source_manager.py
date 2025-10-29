import logging
from datetime import datetime

from google.protobuf.struct_pb2 import Struct
from google.protobuf.wrappers_pb2 import BoolValue, Int64Value, StringValue, UInt64Value

from core.integrations.source_api_processors.argocd_api_processor import ArgoCDAPIProcessor
from core.integrations.source_manager import SourceManager
from core.protos.base_pb2 import Source, SourceModelType, TimeRange, SourceKeyType
from core.protos.connectors.connector_pb2 import Connector as ConnectorProto
from core.protos.literal_pb2 import Literal, LiteralType
from core.protos.playbooks.playbook_commons_pb2 import ApiResponseResult, PlaybookTaskResult, PlaybookTaskResultType, TableResult
from core.protos.playbooks.source_task_definitions.argocd_task_pb2 import ArgoCD
from core.protos.ui_definition_pb2 import FormField, FormFieldType
from core.utils.credentilal_utils import generate_credentials_dict, get_connector_key_type_string, DISPLAY_NAME, CATEGORY, CI_CD
from core.utils.proto_utils import dict_to_proto

logger = logging.getLogger(__name__)


class TimeoutException(Exception):
    pass


class ArgoCDSourceManager(SourceManager):
    def __init__(self):
        self.source = Source.ARGOCD
        self.task_proto = ArgoCD
        self.task_type_callable_map = {
            ArgoCD.TaskType.FETCH_DEPLOYMENT_INFO: {
                'executor': self.fetch_deployment_info,
                'model_types': [SourceModelType.ARGOCD_APPS],
                'result_type': PlaybookTaskResultType.TABLE,
                'display_name': 'Fetch Latest Deployment Info',
                'category': 'CI/CD',
                'form_fields': [
                    FormField(key_name=StringValue(value="count"),
                              display_name=StringValue(value="Enter Count"),
                              data_type=LiteralType.LONG,
                              default_value=Literal(type=LiteralType.LONG, long=Int64Value(value=10)),
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="app_name"),
                              display_name=StringValue(value="App Name"),
                              description=StringValue(value='Select App Name'),
                              data_type=LiteralType.STRING,
                              default_value=Literal(type=LiteralType.STRING, string=StringValue(value="")),
                              form_field_type=FormFieldType.TYPING_DROPDOWN_FT,
                              is_optional=True),
                ]
            },
            ArgoCD.TaskType.ROLLBACK_APPLICATION: {
                'executor': self.rollback_application,
                'model_types': [],
                'task_descriptor': self.rollback_task_descriptor,
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Rollback Application',
                'category': 'CI/CD',
                'form_fields': [
                    FormField(key_name=StringValue(value="app_name"),
                              display_name=StringValue(value="App Name"),
                              description=StringValue(value='Select App Name'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TYPING_DROPDOWN_FT),
                    FormField(key_name=StringValue(value="revision"),
                              display_name=StringValue(value="Revision"),
                              description=StringValue(value='Enter Revision to rollback to'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="deployment_id"),
                              display_name=StringValue(value="Deployment ID"),
                              description=StringValue(value='Enter Deployment ID of the revision'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                ]
            },
            ArgoCD.TaskType.GET_APPLICATION_HEALTH: {
                'executor': self.get_application_health,
                'model_types': [SourceModelType.ARGOCD_APPS],
                'result_type': PlaybookTaskResultType.TABLE,
                'display_name': 'Get Application Health',
                'category': 'CI/CD',
                'form_fields': [
                    FormField(key_name=StringValue(value="app_name"),
                              display_name=StringValue(value="App Name"),
                              description=StringValue(value='Select App Name'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TYPING_DROPDOWN_FT)
                ]
            },
            ArgoCD.TaskType.FETCH_APPS: {
                'executor': self.fetch_apps,
                'model_types': [],
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Fetch Applications',
                'category': 'CI/CD',
                'form_fields': [
                    FormField(key_name=StringValue(value="count"),
                              display_name=StringValue(value="Count"),
                              description=StringValue(value='Maximum number of applications to return (optional)'),
                              data_type=LiteralType.LONG,
                              default_value=Literal(type=LiteralType.LONG, long=Int64Value(value=50)),
                              form_field_type=FormFieldType.TEXT_FT,
                              is_optional=True)
                ]
            },
            ArgoCD.TaskType.FETCH_REVISION_HISTORY: {
                'executor': self.fetch_revision_history,
                'model_types': [SourceModelType.ARGOCD_APPS],
                'result_type': PlaybookTaskResultType.TABLE,
                'display_name': 'Fetch Revision History',
                'category': 'CI/CD',
                'form_fields': [
                    FormField(key_name=StringValue(value="app_name"),
                              display_name=StringValue(value="App Name"),
                              description=StringValue(value='Select App Name'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TYPING_DROPDOWN_FT),
                    FormField(key_name=StringValue(value="count"),
                              display_name=StringValue(value="Count"),
                              description=StringValue(value='Maximum number of revisions to return (optional)'),
                              data_type=LiteralType.LONG,
                              default_value=Literal(type=LiteralType.LONG, long=Int64Value(value=10)),
                              form_field_type=FormFieldType.TEXT_FT,
                              is_optional=True)
                ]
            }
        }
        self.connector_form_configs = [
            {
                "name": StringValue(value="ArgoCD Server and Token Authentication"),
                "description": StringValue(value="Connect to ArgoCD using Server URL and Authentication Token."),
                "form_fields": {
                    SourceKeyType.ARGOCD_SERVER: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.ARGOCD_SERVER)),
                        display_name=StringValue(value="ArgoCD Server URL"),
                        description=StringValue(value='e.g. "https://argocd.company.com", "https://argo.dev-cluster.com:8080"'),
                        helper_text=StringValue(value="Enter the URL of your ArgoCD server"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=False
                    ),
                    SourceKeyType.ARGOCD_TOKEN: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.ARGOCD_TOKEN)),
                        display_name=StringValue(value="Authentication Token"),
                        description=StringValue(value='e.g. "eyJhbGciOiJIUzI1NiIs..."'),
                        helper_text=StringValue(value="Enter your ArgoCD authentication token"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=False,
                        is_sensitive=True
                    )
                }
            }
        ]
        self.connector_type_details = {
            DISPLAY_NAME: "ARGOCD",
            CATEGORY: CI_CD,
        }

    def _extract_api_url_from_connector(self, argocd_connector: ConnectorProto) -> str:
        """Extract the ArgoCD server URL from the connector."""
        if not argocd_connector or not argocd_connector.keys:
            return ""
        
        for key in argocd_connector.keys:
            if key.key_type == SourceKeyType.ARGOCD_SERVER and key.key.value:
                return key.key.value
        
        return ""

    def _build_argocd_url(self, server_url: str, task_type: str, params: dict = None) -> str:
        """
        Build ArgoCD URLs for different task types.
        
        Args:
            server_url: Base ArgoCD server URL from connector
            task_type: Type of task ("fetch_deployment_info", "get_application_health", "rollback_application", "fetch_apps")
            params: Dictionary containing task-specific parameters
        
        Returns:
            Complete ArgoCD URL for the specific task type
        """
        if not server_url:
            return ""
        
        # Remove trailing slash from server URL if present
        base_url = server_url.rstrip('/')
        
        # Convert API URL to frontend URL (remove /api/v1 if present)
        if '/api/v1' in base_url:
            frontend_url = base_url.replace('/api/v1', '')
        else:
            frontend_url = base_url
        
        if params is None:
            params = {}
        
        if task_type == "fetch_deployment_info":
            if 'app_name' in params and params['app_name']:
                # For specific app deployment info - use frontend URL format
                # Pattern: /applications/{project}/{app_name}
                # For now, we'll use a default project or extract from app_name if it contains '/'
                app_name = params['app_name']
                if '/' in app_name:
                    # If app_name contains '/', treat it as project/app_name
                    return f"{frontend_url}/applications/{app_name}"
                else:
                    # Default to 'argocd' project if not specified
                    return f"{frontend_url}/applications/argocd/{app_name}"
            else:
                # For all apps deployment info
                return f"{frontend_url}/applications"
        
        elif task_type == "get_application_health":
            if 'app_name' in params and params['app_name']:
                app_name = params['app_name']
                if '/' in app_name:
                    return f"{frontend_url}/applications/{app_name}"
                else:
                    return f"{frontend_url}/applications/argocd/{app_name}"
            else:
                return f"{frontend_url}/applications"
        
        elif task_type == "rollback_application":
            if 'app_name' in params and params['app_name']:
                app_name = params['app_name']
                if '/' in app_name:
                    return f"{frontend_url}/applications/{app_name}"
                else:
                    return f"{frontend_url}/applications/argocd/{app_name}"
            else:
                return f"{frontend_url}/applications"
        
        elif task_type == "fetch_apps":
            return f"{frontend_url}/applications"
        
        elif task_type == "fetch_revision_history":
            if 'app_name' in params and params['app_name']:
                app_name = params['app_name']
                if '/' in app_name:
                    return f"{frontend_url}/applications/{app_name}?view=tree&resource=&rollback=0"
                else:
                    return f"{frontend_url}/applications/argocd/{app_name}?view=tree&resource=&rollback=0"
            else:
                return f"{frontend_url}/applications"
        
        else:
            # Default fallback
            return f"{frontend_url}/applications"

    def _create_metadata_with_argocd_url(self, server_url: str, task_type: str, params: dict = None) -> Struct:
        """Create metadata struct with ArgoCD URL."""
        argocd_url = self._build_argocd_url(server_url, task_type, params)
        metadata_dict = {
            "link": argocd_url
        }
        return dict_to_proto(metadata_dict, Struct)

    def get_connector_processor(self, argocd_connector, **kwargs):
        generated_credentials = generate_credentials_dict(argocd_connector.type, argocd_connector.keys)
        return ArgoCDAPIProcessor(**generated_credentials)

    @staticmethod
    def rollback_task_descriptor(argocd_task: ArgoCD):
        try:
            app_name = argocd_task.rollback_application.app_name
            revision = argocd_task.rollback_application.revision
            return f"""Rollback for application {app_name.value} to version {revision.value}"""
        except Exception as e:
            raise Exception(f"Error while generating descriptor for ArgoCD task: {e}") from e

    def fetch_deployment_info(self, time_range: TimeRange, argocd_task: ArgoCD,
                              argocd_connector: ConnectorProto):
        # Loop through the commits and get the diff for each one
        try:
            deployment_info = self.get_connector_processor(argocd_connector).get_deployment_info()
            deployment_count = argocd_task.fetch_deployment_info.count
            app_name = argocd_task.fetch_deployment_info.app_name
            
            argocd_app_path = None
            
            # Use time_range parameter for start and end times directly
            start_time = time_range.time_geq
            end_time = time_range.time_lt

            rows = []
            for item in deployment_info.get('items', []):
                for hist in item.get('status', {}).get('history', []):
                    timestamp_str = hist['deployedAt']
                    dt = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%SZ")
                    epoch_seconds = int(dt.timestamp())

                    include = True
                    
                    if app_name and app_name.value: # Explicit filter if app name filter is present
                        if argocd_app_path and argocd_app_path != "":
                            if argocd_app_path != hist['source'].get('path'): # If path exists but doesn't match
                                include = False
                        else: # If path doesn't exist
                            include = False

                    if end_time < epoch_seconds or start_time > epoch_seconds:
                        include = False

                    if include:
                        name_column = TableResult.TableColumn(name=StringValue(value='app_name'),
                                                              value=StringValue(value=hist['source']['path']))
                        time_column = TableResult.TableColumn(name=StringValue(value='deployment_time'),
                                                              value=StringValue(value=hist['deployedAt']))
                        revision_column = TableResult.TableColumn(name=StringValue(value='Revision'),
                                                                  value=StringValue(value=hist['revision']))
                        deployment_id_column = TableResult.TableColumn(name=StringValue(value='Deployment ID'),
                                                                       value=StringValue(value=str(hist['id'])))
                        row = TableResult.TableRow(
                            columns=[name_column, time_column, revision_column, deployment_id_column])
                        rows.append(row)

            # Extract ArgoCD server URL and create metadata
            server_url = self._extract_api_url_from_connector(argocd_connector)
            task_params = {
                'app_name': app_name.value if app_name and app_name.value else None
            }
            metadata = self._create_metadata_with_argocd_url(server_url, "fetch_deployment_info", task_params)

            if rows:
                rows = sorted(rows, key=lambda x: x.columns[1].value.value, reverse=True)

                if deployment_count and deployment_count.value and deployment_count.value < len(rows):
                    rows = rows[:deployment_count.value]

                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TABLE,
                    source=self.source,
                    table=TableResult(raw_query=StringValue(value=f"{len(rows)} Deployments found"),
                                      total_count=UInt64Value(value=len(rows)), rows=rows,
                                      searchable=BoolValue(value=True)),
                    metadata=metadata
                )
            else:
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TABLE,
                    source=self.source,
                    table=TableResult(raw_query=StringValue(value="No Deployments found"),
                                      total_count=UInt64Value(value=len(rows)), rows=rows),
                    metadata=metadata
                )
        except Exception as e:
            raise Exception(f"Error while executing ArgoCD fetch_deployment_info task: {e}")

    def rollback_application(self, time_range: TimeRange, argocd_task: ArgoCD,
                             argocd_connector: ConnectorProto):
        try:
            app_name = argocd_task.rollback_application.app_name
            revision = argocd_task.rollback_application.revision
            deployment_id = argocd_task.rollback_application.deployment_id

            argocd_api_processor = self.get_connector_processor(argocd_connector)

            app_details = argocd_api_processor.get_application_details(app_name.value)
            if not app_details:
                raise Exception(f"Application {app_name.value} not found in ArgoCD")

            sync_policy = app_details.get("spec", {}).get("syncPolicy", {})
            if "automated" in sync_policy:
                argocd_api_processor.disable_auto_sync(app_name.value)

            argocd_api_processor.update_application_revision(app_name.value, revision.value, int(deployment_id.value))

            # Extract ArgoCD server URL and create metadata
            server_url = self._extract_api_url_from_connector(argocd_connector)
            task_params = {
                'app_name': app_name.value if app_name and app_name.value else None
            }
            metadata = self._create_metadata_with_argocd_url(server_url, "rollback_application", task_params)

            response_obj = {
                "response": f"Successfully rolled back application {app_name.value} to version {revision.value}"}
            response_body = Struct()
            response_body.update(response_obj)

            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                source=self.source,
                api_response=ApiResponseResult(
                    response_body=response_body
                ),
                metadata=metadata
            )
        except Exception as e:
            raise Exception(f"Error while executing ArgoCD rollback_application task: {e}")

    @staticmethod
    def validate_command(argocd_task: ArgoCD):
        return "REQUIRES_APPROVAL"

    def get_application_health(self, time_range: TimeRange, argocd_task: ArgoCD,
                              argocd_connector: ConnectorProto):
        try:
            app_name = argocd_task.get_application_health.app_name

            argocd_api_processor = self.get_connector_processor(argocd_connector)
            health_info = argocd_api_processor.get_application_health(app_name.value)

            # Extract ArgoCD server URL and create metadata
            server_url = self._extract_api_url_from_connector(argocd_connector)
            task_params = {
                'app_name': app_name.value if app_name and app_name.value else None
            }
            metadata = self._create_metadata_with_argocd_url(server_url, "get_application_health", task_params)

            if not health_info:
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TABLE,
                    source=self.source,
                    table=TableResult(
                        raw_query=StringValue(value=f"No health information found for application {app_name.value}"),
                        total_count=UInt64Value(value=0),
                        rows=[]
                    ),
                    metadata=metadata
                )

            # Create table columns for the health information
            status_column = TableResult.TableColumn(
                name=StringValue(value='Status'),
                value=StringValue(value=health_info['status'])
            )
            sync_status_column = TableResult.TableColumn(
                name=StringValue(value='Sync Status'),
                value=StringValue(value=health_info['sync_status'])
            )

            # Create a single row with all the health information
            row = TableResult.TableRow(columns=[
                status_column,
                sync_status_column
            ])

            return PlaybookTaskResult(
                type=PlaybookTaskResultType.TABLE,
                source=self.source,
                table=TableResult(
                    raw_query=StringValue(value=f"Health status for application {app_name.value}"),
                    total_count=UInt64Value(value=1),
                    rows=[row]
                ),
                metadata=metadata
            )

        except Exception as e:
            raise Exception(f"Error while executing ArgoCD get_application_health task: {e}")

    def fetch_apps(self, time_range: TimeRange, argocd_task: ArgoCD,
                   argocd_connector: ConnectorProto):
        try:
            count = argocd_task.fetch_apps.count
            count_value = count.value if count and count.value else None

            argocd_api_processor = self.get_connector_processor(argocd_connector)
            apps_data = argocd_api_processor.fetch_apps(count_value)

            # Extract ArgoCD server URL and create metadata
            server_url = self._extract_api_url_from_connector(argocd_connector)
            metadata = self._create_metadata_with_argocd_url(server_url, "fetch_apps")

            if not apps_data or not apps_data.get('items'):
                response_obj = {
                    "message": "No applications found",
                    "items": [],
                    "total_count": 0
                }
            else:
                applications = apps_data.get('items', [])
                response_obj = {
                    "message": f"Successfully fetched {len(applications)} applications",
                    "items": applications,
                    "total_count": len(applications),
                    "metadata": apps_data.get('metadata', {})
                }

            response_body = Struct()
            response_body.update(response_obj)

            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                source=self.source,
                api_response=ApiResponseResult(
                    response_body=response_body
                ),
                metadata=metadata
            )

        except Exception as e:
            raise Exception(f"Error while executing ArgoCD fetch_apps task: {e}")

    def fetch_revision_history(self, time_range: TimeRange, argocd_task: ArgoCD,
                              argocd_connector: ConnectorProto):
        """
        Fetch the revision history of a specific ArgoCD application.
        
        Args:
            time_range: Time range for the query (not used for revision history)
            argocd_task: ArgoCD task containing app_name and count parameters
            argocd_connector: ArgoCD connector configuration
            
        Returns:
            PlaybookTaskResult: Table result containing revision history
        """
        try:
            app_name = argocd_task.fetch_revision_history.app_name
            count = argocd_task.fetch_revision_history.count
            count_value = count.value if count and count.value else 10

            if not app_name or not app_name.value:
                raise Exception("App name is required for fetching revision history")

            argocd_api_processor = self.get_connector_processor(argocd_connector)
            app_data = argocd_api_processor.get_revision_history(app_name.value, count_value)

            # Extract ArgoCD server URL and create metadata
            server_url = self._extract_api_url_from_connector(argocd_connector)
            task_params = {
                'app_name': app_name.value
            }
            metadata = self._create_metadata_with_argocd_url(server_url, "fetch_revision_history", task_params)

            if not app_data:
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TABLE,
                    source=self.source,
                    table=TableResult(
                        raw_query=StringValue(value=f"No revision history found for application {app_name.value}"),
                        total_count=UInt64Value(value=0),
                        rows=[]
                    ),
                    metadata=metadata
                )

            # Extract revision history from the application data
            history = app_data.get('status', {}).get('history', [])
            
            if not history:
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TABLE,
                    source=self.source,
                    table=TableResult(
                        raw_query=StringValue(value=f"No revision history available for application {app_name.value}"),
                        total_count=UInt64Value(value=0),
                        rows=[]
                    ),
                    metadata=metadata
                )

            # Create table rows for each revision
            rows = []
            for i, revision in enumerate(history):
                # Extract revision information
                revision_id = revision.get('id', f'revision-{i}')
                deployed_at = revision.get('deployedAt', '')
                revision_hash = revision.get('revision', '')
                source = revision.get('source', {})
                source_type = source.get('type', '')
                source_repo = source.get('repoURL', '')
                source_path = source.get('path', '')
                source_target_revision = source.get('targetRevision', '')
                
                # Create table columns for this revision
                columns = [
                    TableResult.TableColumn(
                        name=StringValue(value='Revision ID'),
                        value=StringValue(value=str(revision_id))
                    ),
                    TableResult.TableColumn(
                        name=StringValue(value='Deployed At'),
                        value=StringValue(value=deployed_at)
                    ),
                    TableResult.TableColumn(
                        name=StringValue(value='Revision Hash'),
                        value=StringValue(value=revision_hash)
                    ),
                    TableResult.TableColumn(
                        name=StringValue(value='Source Type'),
                        value=StringValue(value=source_type)
                    ),
                    TableResult.TableColumn(
                        name=StringValue(value='Source Repo'),
                        value=StringValue(value=source_repo)
                    ),
                    TableResult.TableColumn(
                        name=StringValue(value='Source Path'),
                        value=StringValue(value=source_path)
                    ),
                    TableResult.TableColumn(
                        name=StringValue(value='Target Revision'),
                        value=StringValue(value=source_target_revision)
                    )
                ]
                
                # Create the table row
                row = TableResult.TableRow(columns=columns)
                rows.append(row)

            return PlaybookTaskResult(
                type=PlaybookTaskResultType.TABLE,
                source=self.source,
                table=TableResult(
                    raw_query=StringValue(value=f"Revision history for application {app_name.value}"),
                    total_count=UInt64Value(value=len(rows)),
                    rows=rows,
                    searchable=BoolValue(value=True)
                ),
                metadata=metadata
            )

        except Exception as e:
            raise Exception(f"Error while executing ArgoCD fetch_revision_history task: {e}")
