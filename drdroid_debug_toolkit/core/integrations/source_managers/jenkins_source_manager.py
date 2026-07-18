import json
import logging
from google.protobuf.wrappers_pb2 import StringValue, UInt64Value, BoolValue

from core.integrations.source_api_processors.jenkins_api_processor import JenkinsAPIProcessor
from core.integrations.source_manager import SourceManager
from core.protos.base_pb2 import Source, SourceModelType, TimeRange, SourceKeyType
from core.protos.connectors.connector_pb2 import Connector
from core.protos.literal_pb2 import LiteralType, Literal
from core.protos.playbooks.playbook_commons_pb2 import PlaybookTaskResultType, PlaybookTaskResult, TableResult
from core.protos.playbooks.source_task_definitions.jenkins_task_pb2 import Jenkins
from core.protos.ui_definition_pb2 import FormField, FormFieldType
from core.utils.credentilal_utils import generate_credentials_dict, get_connector_key_type_string, DISPLAY_NAME, CATEGORY, CI_CD
from core.utils.proto_utils import proto_to_dict

logger = logging.getLogger(__name__)


class JenkinsSourceManager(SourceManager):

    def __init__(self):
        self.source = Source.JENKINS
        self.task_proto = Jenkins
        self.task_type_callable_map = {
            Jenkins.TaskType.FETCH_LAST_BUILD_DETAILS: {
                'executor': self.fetch_last_build_details,
                'model_types': [SourceModelType.JENKINS_JOBS],
                'result_type': PlaybookTaskResultType.TABLE,
                'display_name': 'Fetch last build details',
                'category': 'CI/CD',
                'form_fields': [
                    FormField(key_name=StringValue(value="job_name"),
                              display_name=StringValue(value="Job Path"),
                              description=StringValue(value='Enter job path (e.g., folder/job or nested/folder/job)'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT)
                ]
            },
            Jenkins.TaskType.RUN_JOB: {
                'executor': self.run_job,
                'model_types': [SourceModelType.JENKINS_JOBS],
                'result_type': PlaybookTaskResultType.TABLE,
                'display_name': 'Run Jenkins Job',
                'category': 'CI/CD',
                'form_fields': [
                    FormField(key_name=StringValue(value="job_name"),
                              display_name=StringValue(value="Job Path"),
                              description=StringValue(value='Enter job path (e.g., folder/job or nested/folder/job)'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="parameters"),
                              display_name=StringValue(value="Parameters"),
                              description=StringValue(value='Enter parameters as JSON if required (e.g., {"USER_INPUT": "abcxyz"}), or leave empty for jobs without parameters'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.MULTILINE_FT)
                ]
            },
            # Task type 3 (GET_ALL_BRANCHES) existed in the proto + api processor but was never
            # wired into this dispatch map, so `jenkins_get_all_branches` failed with "Task type 3
            # not supported for source: jenkins". That dead-ended the multibranch flow:
            # fetch_last_build_details on a multibranch job tells the agent to "Use the Get All
            # Branches task", which then errored — an infinite loop for the agent.
            Jenkins.TaskType.GET_ALL_BRANCHES: {
                'executor': self.get_all_branches,
                'model_types': [SourceModelType.JENKINS_JOBS],
                'result_type': PlaybookTaskResultType.TABLE,
                'display_name': 'Get All Branches',
                'category': 'CI/CD',
                'form_fields': [
                    FormField(key_name=StringValue(value="job_name"),
                              display_name=StringValue(value="Job Path"),
                              description=StringValue(value='Enter multibranch job path (e.g., folder/job or nested/folder/job)'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT)
                ]
            },
            # Discovery task. Every other Jenkins task needs an exact job path, but nothing
            # told the agent what paths exist — so it guessed from service names
            # (`wi-candidate-profile-ms`, then `gh-wi-candidate-profile-ms-wi-candidate-profile-ms`)
            # and Jenkins answered 404. This lists the real paths so the agent can look one up.
            Jenkins.TaskType.LIST_JOBS: {
                'executor': self.list_jobs,
                'model_types': [SourceModelType.JENKINS_JOBS],
                'result_type': PlaybookTaskResultType.TABLE,
                'display_name': 'List Jobs',
                'category': 'CI/CD',
                'form_fields': [
                    FormField(key_name=StringValue(value="folder_path"),
                              display_name=StringValue(value="Folder Path"),
                              description=StringValue(value='Optional: restrict listing to a folder (e.g. team-a or team-a/subfolder). Leave empty to list all jobs.'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT,
                              is_optional=True)
                ]
            },
        }

        self.connector_form_configs = [
            {
                "name": StringValue(value="Jenkins Authentication with Crumb"),
                "description": StringValue(value="Connect to Jenkins using URL, Username, API Token, and enable Crumb if CSRF protection is enabled."),
                "form_fields": {
                    SourceKeyType.JENKINS_URL: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.JENKINS_URL)),
                        display_name=StringValue(value="Jenkins URL"),
                        description=StringValue(value="Enter the base URL of your Jenkins instance (e.g., http://jenkins.example.com)."),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=False
                    ),
                    SourceKeyType.JENKINS_USERNAME: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.JENKINS_USERNAME)),
                        display_name=StringValue(value="Jenkins Username"),
                        description=StringValue(value="Enter your Jenkins username."),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=False
                    ),
                    SourceKeyType.JENKINS_API_TOKEN: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.JENKINS_API_TOKEN)),
                        display_name=StringValue(value="Jenkins API Token"),
                        description=StringValue(value="Enter your Jenkins API token."),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=False,
                        is_sensitive=True
                    ),
                    SourceKeyType.JENKINS_CRUMB: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.JENKINS_CRUMB)),
                        display_name=StringValue(value="Enable Crumb (CSRF Protection)"),
                        description=StringValue(value="Check if your Jenkins instance has CSRF protection enabled and requires a crumb."),
                        data_type=LiteralType.BOOLEAN,
                        form_field_type=FormFieldType.CHECKBOX_FT,
                        is_optional=False,
                        default_value=Literal(type=LiteralType.BOOLEAN, boolean=BoolValue(value=False))
                    )
                }
            },
            {
                "name": StringValue(value="Jenkins Authentication without Crumb"),
                "description": StringValue(value="Connect to Jenkins using URL, Username, and API Token. Use this if CSRF protection (crumb) is disabled."),
                "form_fields": {
                    SourceKeyType.JENKINS_URL: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.JENKINS_URL)),
                        display_name=StringValue(value="Jenkins URL"),
                        description=StringValue(value='e.g. "http://jenkins.example.com"'),
                        helper_text=StringValue(value="Enter the base URL of your Jenkins instance (e.g., http://jenkins.example.com)."),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=False
                    ),
                    SourceKeyType.JENKINS_USERNAME: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.JENKINS_USERNAME)),
                        display_name=StringValue(value="Jenkins Username"),
                        description=StringValue(value='e.g. "admin"'),
                        helper_text=StringValue(value="Enter your Jenkins username."),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=False
                    ),
                    SourceKeyType.JENKINS_API_TOKEN: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.JENKINS_API_TOKEN)),
                        display_name=StringValue(value="Jenkins API Token"),
                        description=StringValue(value='e.g. "1234567890abcdefghijklmnopqrstuvwxyz"'),
                        helper_text=StringValue(value="Enter your Jenkins API token."),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=False,
                        is_sensitive=True
                    )
                }
            }
        ]
        self.connector_type_details = {
            DISPLAY_NAME: "JENKINS",
            CATEGORY: CI_CD,
        }

    def get_connector_processor(self, jenkins_connector, **kwargs):
        generated_credentials = generate_credentials_dict(jenkins_connector.type, jenkins_connector.keys)
        return JenkinsAPIProcessor(**generated_credentials)

    def run_job(self, time_range: TimeRange, jenkins_task: Jenkins, jenkins_connector: Connector):
        try:
            if not jenkins_connector:
                raise Exception("Task execution Failed:: No Jenkins source found")

            run_job = jenkins_task.run_job
            if not run_job.job_name.value:
                raise Exception("Task execution Failed:: Job Name is required")

            # Get the job name, which might be a simple name or a full path (folder/job)
            job_name = run_job.job_name.value
            job_parameters_str = run_job.parameters.value if run_job.HasField('parameters') else None
            try:
                job_parameters = json.loads(job_parameters_str) if job_parameters_str else {}
            except json.JSONDecodeError as e:
                logger.error(f"Received Invalid JSON in parameters: {job_parameters_str}, error: {e}")
                job_parameters = {}
            jenkins_processor = self.get_connector_processor(jenkins_connector)
            print("Playbook Task Downstream Request: Type -> {}, Account -> {}".format("Jenkins",
                                                                                       jenkins_connector.account_id.value),
                  job_name, flush=True)

            jenkins_processor.run_job(job_name, parameters=job_parameters)
            result = [{'Status': 'Job triggered successfully', 'Job Name': job_name,
                       'Parameters': json.dumps(job_parameters, indent=2) if job_parameters else 'No parameters',
                       'Next Steps': 'Use FETCH_LAST_BUILD_DETAILS task to get latest build information'}]
            table_rows: [TableResult.TableRow] = []
            for r in list(result):
                table_columns = []
                for key, value in r.items():
                    table_columns.append(TableResult.TableColumn(name=StringValue(value=str(key)),
                                                                 value=StringValue(value=str(value))))
                table_row = TableResult.TableRow(columns=table_columns)
                table_rows.append(table_row)
            table = TableResult(raw_query=StringValue(value=f"Job trigger result for ```{job_name}```"),
                                total_count=UInt64Value(value=len(list(result))),
                                rows=table_rows)
            return PlaybookTaskResult(type=PlaybookTaskResultType.TABLE, table=table, source=self.source)
        except Exception as e:
            raise Exception(f"Error while executing Jenkins task: {e}")

    def fetch_last_build_details(self, time_range: TimeRange, jenkins_task: Jenkins,
                                 jenkins_connector: Connector):
        try:
            if not jenkins_connector:
                raise Exception("Task execution Failed:: No Jenkins source found")

            fetch_last_build_details = jenkins_task.fetch_last_build_details
            if not fetch_last_build_details.job_name.value:
                raise Exception("Task execution Failed:: Job Name is required")

            # Get the job name, which might be a simple name or a full path (folder/job)
            job_name = fetch_last_build_details.job_name.value
            jenkins_processor = self.get_connector_processor(jenkins_connector)
            print("Playbook Task Downstream Request: Type -> {}, Account -> {}".format("Jenkins",
                                                                                       jenkins_connector.account_id.value),
                  job_name, flush=True)
            result = jenkins_processor.get_last_build(job_name)
            table_rows: [TableResult.TableRow] = []
            for r in list(result):
                table_columns = []
                for key, value in r.items():
                    table_columns.append(TableResult.TableColumn(name=StringValue(value=str(key)),
                                                                 value=StringValue(value=str(value))))
                table_row = TableResult.TableRow(columns=table_columns)
                table_rows.append(table_row)
            table = TableResult(raw_query=StringValue(value=f"Last Build details for ```{job_name}```"),
                                total_count=UInt64Value(value=len(list(result))),
                                rows=table_rows)
            return PlaybookTaskResult(type=PlaybookTaskResultType.TABLE, table=table, source=self.source)
        except Exception as e:
            raise Exception(f"Error while executing Jenkins task: {e}")

    def get_all_branches(self, time_range: TimeRange, jenkins_task: Jenkins,
                         jenkins_connector: Connector):
        # Executor for GET_ALL_BRANCHES (task type 3). The api processor already implements the
        # multibranch branch listing; this just adapts it into a TABLE PlaybookTaskResult, mirroring
        # fetch_last_build_details. Needed so the agent can actually follow the "Use Get All Branches"
        # guidance that fetch_last_build_details emits for multibranch pipeline jobs.
        try:
            if not jenkins_connector:
                raise Exception("Task execution Failed:: No Jenkins source found")

            get_all_branches = jenkins_task.get_all_branches
            if not get_all_branches.job_name.value:
                raise Exception("Task execution Failed:: Job Name is required")

            job_name = get_all_branches.job_name.value
            jenkins_processor = self.get_connector_processor(jenkins_connector)
            print("Playbook Task Downstream Request: Type -> {}, Account -> {}".format(
                "Jenkins", jenkins_connector.account_id.value), job_name, flush=True)
            result = jenkins_processor.get_all_branches(job_name)
            table_rows: [TableResult.TableRow] = []
            for r in list(result):
                table_columns = []
                for key, value in r.items():
                    table_columns.append(TableResult.TableColumn(name=StringValue(value=str(key)),
                                                                 value=StringValue(value=str(value))))
                table_rows.append(TableResult.TableRow(columns=table_columns))
            table = TableResult(raw_query=StringValue(value=f"All branches for ```{job_name}```"),
                                total_count=UInt64Value(value=len(table_rows)),
                                rows=table_rows)
            return PlaybookTaskResult(type=PlaybookTaskResultType.TABLE, table=table, source=self.source)
        except Exception as e:
            raise Exception(f"Error while executing Jenkins task: {e}")

    def list_jobs(self, time_range: TimeRange, jenkins_task: Jenkins,
                  jenkins_connector: Connector):
        # Executor for LIST_JOBS (task type 4). Returns the exact job paths that the other
        # Jenkins tasks require, so the agent can look a path up instead of inventing one.
        #
        # Two deliberate shaping decisions:
        #  - Folder entries are dropped. get_all_jobs_recursive returns folders alongside jobs,
        #    but a folder is not runnable and their children are already included as separate
        #    rows, so listing them is pure noise for the agent.
        #  - `is_multibranch` is surfaced. A multibranch job rejects fetch_last_build_details
        #    without a branch and tells the caller to use Get All Branches; flagging it here
        #    lets the agent take that path directly rather than discovering it via an error.
        try:
            if not jenkins_connector:
                raise Exception("Task execution Failed:: No Jenkins source found")

            list_jobs_task = jenkins_task.list_jobs
            folder_path = list_jobs_task.folder_path.value or ""

            jenkins_processor = self.get_connector_processor(jenkins_connector)
            print("Playbook Task Downstream Request: Type -> {}, Account -> {}".format(
                "Jenkins", jenkins_connector.account_id.value), folder_path or "<root>", flush=True)
            all_entries = jenkins_processor.get_all_jobs_recursive(folder_path)

            table_rows: [TableResult.TableRow] = []
            for entry in list(all_entries):
                job_class = str(entry.get("class", ""))
                if "folder" in job_class.lower() or "directory" in job_class.lower():
                    continue
                row = {
                    "name": entry.get("name", ""),
                    "full_path": entry.get("full_path", ""),
                    "class": job_class,
                    "is_multibranch": "multibranch" in job_class.lower(),
                }
                table_columns = []
                for key, value in row.items():
                    table_columns.append(TableResult.TableColumn(name=StringValue(value=str(key)),
                                                                 value=StringValue(value=str(value))))
                table_rows.append(TableResult.TableRow(columns=table_columns))

            scope = f" under ```{folder_path}```" if folder_path else ""
            table = TableResult(raw_query=StringValue(value=f"All Jenkins jobs{scope}"),
                                total_count=UInt64Value(value=len(table_rows)),
                                rows=table_rows)
            return PlaybookTaskResult(type=PlaybookTaskResultType.TABLE, table=table, source=self.source)
        except Exception as e:
            raise Exception(f"Error while executing Jenkins task: {e}")
