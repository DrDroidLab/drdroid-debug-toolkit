import json
import logging
from google.protobuf.wrappers_pb2 import StringValue, UInt64Value

from core.integrations.source_api_processors.jenkins_api_processor import JenkinsAPIProcessor
from core.integrations.source_manager import SourceManager
from core.protos.base_pb2 import Source, SourceModelType, TimeRange
from core.protos.connectors.connector_pb2 import Connector
from core.protos.literal_pb2 import LiteralType
from core.protos.playbooks.playbook_commons_pb2 import PlaybookTaskResultType, PlaybookTaskResult, TableResult
from core.protos.playbooks.source_task_definitions.jenkins_task_pb2 import Jenkins
from core.protos.ui_definition_pb2 import FormField, FormFieldType
from core.utils.credentilal_utils import generate_credentials_dict
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
                              display_name=StringValue(value="Job Name"),
                              description=StringValue(value='Enter Job Name'),
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
                              display_name=StringValue(value="Job Name"),
                              description=StringValue(value='Enter Job Name'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT),
                    FormField(key_name=StringValue(value="parameters"),
                              display_name=StringValue(value="Parameters"),
                              description=StringValue(
                                  value='Enter parameters as JSON if required (e.g., {"USER_INPUT": "Make America Great Again"}), or leave empty for jobs without parameters'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.MULTILINE_FT)
                ]
            },
            Jenkins.TaskType.GET_ALL_BRANCHES: {
                'executor': self.get_all_branches,
                'model_types': [SourceModelType.JENKINS_JOBS],
                'result_type': PlaybookTaskResultType.TABLE,
                'display_name': 'Get All Branches',
                'category': 'CI/CD',
                'form_fields': [
                    FormField(key_name=StringValue(value="job_name"),
                              display_name=StringValue(value="Job Name"),
                              description=StringValue(value='Enter Multibranch Job Name'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT)
                ]
            },
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
            job_parameter_json = run_job.parameter_json
            job_parameter_dict = proto_to_dict(job_parameter_json) if job_parameter_json else {}
            job_parameters.update(job_parameter_dict)
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

            # Get the job name and optional branch name
            job_name = fetch_last_build_details.job_name.value
            branch_name = fetch_last_build_details.branch_name.value if fetch_last_build_details.HasField('branch_name') else None
            
            jenkins_processor = self.get_connector_processor(jenkins_connector)
            print("Playbook Task Downstream Request: Type -> {}, Account -> {}".format("Jenkins",
                                                                                       jenkins_connector.account_id.value),
                  job_name, branch_name or "No branch specified", flush=True)
            
            result = jenkins_processor.get_last_build(job_name, branch_name)
            table_rows: [TableResult.TableRow] = []
            for r in list(result):
                table_columns = []
                for key, value in r.items():
                    table_columns.append(TableResult.TableColumn(name=StringValue(value=str(key)),
                                                                 value=StringValue(value=str(value))))
                table_row = TableResult.TableRow(columns=table_columns)
                table_rows.append(table_row)
            
            query_description = f"Last Build details for ```{job_name}```"
            if branch_name:
                query_description += f" (branch: ```{branch_name}```)"
            
            table = TableResult(raw_query=StringValue(value=query_description),
                                total_count=UInt64Value(value=len(list(result))),
                                rows=table_rows)
            return PlaybookTaskResult(type=PlaybookTaskResultType.TABLE, table=table, source=self.source)
        except Exception as e:
            raise Exception(f"Error while executing Jenkins task: {e}")

    def get_all_branches(self, time_range: TimeRange, jenkins_task: Jenkins,
                        jenkins_connector: Connector):
        try:
            if not jenkins_connector:
                raise Exception("Task execution Failed:: No Jenkins source found")

            get_all_branches = jenkins_task.get_all_branches
            if not get_all_branches.job_name.value:
                raise Exception("Task execution Failed:: Job Name is required")

            # Get the multibranch job name
            job_name = get_all_branches.job_name.value
            jenkins_processor = self.get_connector_processor(jenkins_connector)
            print("Playbook Task Downstream Request: Type -> {}, Account -> {}".format("Jenkins",
                                                                                       jenkins_connector.account_id.value),
                  job_name, flush=True)
            
            result = jenkins_processor.get_all_branches(job_name)
            table_rows: [TableResult.TableRow] = []
            for r in list(result):
                table_columns = []
                for key, value in r.items():
                    table_columns.append(TableResult.TableColumn(name=StringValue(value=str(key)),
                                                                 value=StringValue(value=str(value))))

                table_row = TableResult.TableRow(columns=table_columns)
                table_rows.append(table_row)
            
            table = TableResult(raw_query=StringValue(value=f"All branches for multibranch job ```{job_name}```"),
                                total_count=UInt64Value(value=len(list(result))),
                                rows=table_rows)
            return PlaybookTaskResult(type=PlaybookTaskResultType.TABLE, table=table, source=self.source)
        except Exception as e:
            raise Exception(f"Error while executing Jenkins task: {e}")
