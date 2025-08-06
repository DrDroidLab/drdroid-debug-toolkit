import logging

from google.protobuf.struct_pb2 import Struct

from core.integrations.source_api_processors.jira_api_processor import JiraApiProcessor
from core.integrations.source_manager import SourceManager
from core.protos.literal_pb2 import LiteralType, Literal
from core.protos.playbooks.source_task_definitions.jira_task_pb2 import Jira

from google.protobuf.wrappers_pb2 import StringValue
from core.protos.base_pb2 import Source, SourceModelType

from core.utils.credentilal_utils import generate_credentials_dict
from core.protos.playbooks.playbook_commons_pb2 import PlaybookTaskResult, PlaybookTaskResultType, ApiResponseResult, \
    TextResult
from core.protos.base_pb2 import TimeRange
from core.protos.ui_definition_pb2 import FormField, FormFieldType
from core.protos.connectors.connector_pb2 import Connector as ConnectorProto
from core.utils.proto_utils import dict_to_proto

logger = logging.getLogger(__name__)


class JiraSourceManager(SourceManager):
    def __init__(self):
        self.source = Source.JIRA_CLOUD
        self.task_proto = Jira
        self.task_type_callable_map = {
            Jira.TaskType.CREATE_TICKET: {
                'executor': self.create_ticket,
                'model_types': [SourceModelType.JIRA_PROJECT],
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Create JIRA Ticket',
                'category': 'Task',
                'form_fields': [
                    FormField(
                        key_name=StringValue(value="project_key"),
                        display_name=StringValue(value="Project Key"),
                        description=StringValue(value='Enter JIRA Project Key (e.g., PROJ)'),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.DROPDOWN_FT
                    ),
                    FormField(
                        key_name=StringValue(value="summary"),
                        display_name=StringValue(value="Summary"),
                        description=StringValue(value='Enter ticket summary/title'),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT
                    ),
                    FormField(
                        key_name=StringValue(value="description"),
                        display_name=StringValue(value="Description"),
                        description=StringValue(value='Enter ticket description'),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.MULTILINE_FT
                    ),
                    FormField(
                        key_name=StringValue(value="issue_type"),
                        display_name=StringValue(value="Issue Type"),
                        description=StringValue(value='Select issue type'),
                        data_type=LiteralType.STRING,
                        valid_values=[
                            Literal(type=LiteralType.STRING, string=StringValue(value="Task"))
                        ],
                        form_field_type=FormFieldType.DROPDOWN_FT
                    ),
                    FormField(
                        key_name=StringValue(value="priority"),
                        display_name=StringValue(value="Priority"),
                        description=StringValue(value='Select priority'),
                        data_type=LiteralType.STRING,
                        valid_values=[
                            Literal(type=LiteralType.STRING, string=StringValue(value="Lowest")),
                            Literal(type=LiteralType.STRING, string=StringValue(value="Low")),
                            Literal(type=LiteralType.STRING, string=StringValue(value="Medium")),
                            Literal(type=LiteralType.STRING, string=StringValue(value="High")),
                            Literal(type=LiteralType.STRING, string=StringValue(value="Highest"))
                        ],
                        form_field_type=FormFieldType.DROPDOWN_FT,
                        is_optional=True,
                    ),
                    FormField(
                        key_name=StringValue(value="labels"),
                        display_name=StringValue(value="Labels"),
                        description=StringValue(value='Enter comma-separated labels (no spaces)'),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                    )
                ]
            },
            Jira.TaskType.ASSIGN_TICKET: {
                'executor': self.assign_ticket,
                'model_types': [SourceModelType.JIRA_USER],
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Assign JIRA Ticket',
                'category': 'Task',
                'form_fields': [
                    FormField(
                        key_name=StringValue(value="ticket_key"),
                        display_name=StringValue(value="Ticket Key"),
                        description=StringValue(value='Enter JIRA ticket key (e.g., PROJ-123)'),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT
                    ),
                    FormField(
                        key_name=StringValue(value="assignee"),
                        display_name=StringValue(value="Assignee"),
                        description=StringValue(value='Enter assignee username'),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.DROPDOWN_FT
                    )
                ]
            },
            Jira.TaskType.GET_USERS: {
                'executor': self.get_users,
                'model_types': [],
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Get JIRA Users',
                'category': 'Task',
                'form_fields': [
                    FormField(
                        key_name=StringValue(value="query"),
                        display_name=StringValue(value="Query"),
                        description=StringValue(value='Enter query to find users (e.g., John)'),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TYPING_DROPDOWN_FT
                    ),
                ]
            },
            Jira.TaskType.GET_TICKET: {
                'executor': self.get_ticket,
                'model_types': [],
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Get JIRA Ticket',
                'category': 'Task',
                'form_fields': [
                    FormField(
                        key_name=StringValue(value="ticket_key"),
                        display_name=StringValue(value="Ticket Key"),
                        description=StringValue(value='Enter JIRA ticket key (e.g., PROJ-123)'),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT
                    )
                ]
            },
            Jira.TaskType.SEARCH_TICKETS: {
                'executor': self.search_tickets,
                'model_types': [SourceModelType.JIRA_PROJECT],
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Search JIRA Tickets',
                'category': 'Task',
                'form_fields': [
                    FormField(
                        key_name=StringValue(value="query"),
                        display_name=StringValue(value="Query (jql)"),
                        description=StringValue(value='Enter query (jql) to search tickets'),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT
                    )
                ]
            },
            Jira.TaskType.ADD_COMMENT: {
                'executor': self.add_comment,
                'model_types': [],
                'result_type': PlaybookTaskResultType.API_RESPONSE,
                'display_name': 'Add Comment to JIRA Ticket',
                'category': 'Task',
                'form_fields': [
                    FormField(
                        key_name=StringValue(value="ticket_key"),
                        display_name=StringValue(value="Ticket Key"),
                        description=StringValue(value='Enter JIRA ticket key (e.g., PROJ-123)'),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT
                    ),
                    FormField(
                        key_name=StringValue(value="comment_text"),
                        display_name=StringValue(value="Comment"),
                        description=StringValue(value='Enter comment text'),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.MULTILINE_FT
                    ),
                    FormField(
                        key_name=StringValue(value="image_urls"),
                        display_name=StringValue(value="Image URLs"),
                        description=StringValue(value='Enter URLs for images to include in the comment'),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.STRING_ARRAY_FT,
                        is_optional=True,
                    )
                ]
            }
        }

    def get_connector_processor(self, jira_connector, **kwargs):

        try:
            if not jira_connector or not jira_connector.keys:
                raise Exception("Invalid JIRA connector: Missing required credentials")
            credentials = generate_credentials_dict(jira_connector.type, jira_connector.keys)
            required_keys = ['jira_cloud_api_key', 'jira_domain', 'jira_email']
            missing_keys = [key for key in required_keys if key not in credentials]
            if missing_keys:
                raise Exception(f"Missing required JIRA credentials: {', '.join(missing_keys)}")
            return JiraApiProcessor(**credentials)
        except Exception as e:
            logger.error(f"Error creating JIRA processor: {str(e)}")
            raise

    def get_users(self, time_range: TimeRange, jira_task: Jira,
                  jira_connector: ConnectorProto):
        try:
            if not jira_connector:
                logger.error("Task execution Failed: No JIRA source found")
                raise ValueError("No JIRA source found")

            task = jira_task.get_users
            query = task.query.value

            # Get JIRA processor
            jira_processor = self.get_connector_processor(jira_connector)

            # Log attempt to get users
            logger.info(f"Attempting to get JIRA users")

            # Get the users
            result = jira_processor.get_users(query)

            if not result:
                error_msg = "JIRA API returned no result when fetching users"
                logger.error(error_msg)
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value=error_msg)),
                    source=self.source
                )

            # Convert result to Struct for API response
            response_dict = {
                'users': result
            }

            logger.info(f"Successfully fetched JIRA users")

            response_struct = dict_to_proto(response_dict, Struct)
            jira_output = ApiResponseResult(response_body=response_struct)

            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                source=self.source,
                api_response=jira_output
            )
        except Exception as e:
            error_msg = f"Exception occurred while fetching JIRA users: {str(e)}"
            logger.error(error_msg, exc_info=True)

            return PlaybookTaskResult(
                type=PlaybookTaskResultType.TEXT,
                text=TextResult(output=StringValue(value=error_msg)),
                source=self.source
            )

    def assign_ticket(self, time_range: TimeRange, jira_task: Jira,
                      jira_connector: ConnectorProto):
        try:
            if not jira_connector:
                logger.error("Task execution Failed: No JIRA source found")
                raise ValueError("No JIRA source found")

            task = jira_task.assign_ticket

            # Extract and validate fields
            ticket_key = task.ticket_key.value
            assignee = task.assignee.value

            # Validate required fields
            if not ticket_key or not assignee:
                missing_fields = []
                if not ticket_key: missing_fields.append("ticket_key")
                if not assignee: missing_fields.append("assignee")
                error_msg = f"Missing required fields: {', '.join(missing_fields)}"
                logger.error(error_msg)
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value=error_msg)),
                    source=self.source
                )

            # Get JIRA processor
            jira_processor = self.get_connector_processor(jira_connector)

            # Log attempt to assign ticket
            logger.info(f"Attempting to assign JIRA ticket {ticket_key} to {assignee}")

            # Find the assignee from list of users
            users = jira_processor.get_users(query=assignee)
            if not users:
                error_msg = f"Could not find user {assignee}"
                logger.error(error_msg)
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value=error_msg)),
                    source=self.source
                )

            assignee_id = users[0]['accountId']

            # Assign the ticket
            result = jira_processor.assign_ticket(ticket_key=ticket_key, assignee=assignee_id)

            if not result:
                error_msg = "JIRA API returned no result when assigning ticket"
                logger.error(error_msg)
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value=error_msg)),
                    source=self.source
                )

            # Convert result to Struct for API response
            response_dict = {
                'ticket_key': ticket_key,
                'assignee': assignee,
                'status': 'Assigned'
            }

            logger.info(f"Successfully assigned JIRA ticket {ticket_key} to {assignee}")

            response_struct = dict_to_proto(response_dict, Struct)
            jira_output = ApiResponseResult(response_body=response_struct)

            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                source=self.source,
                api_response=jira_output
            )
        except Exception as e:
            error_msg = f"Exception occurred while assigning JIRA ticket: {str(e)}"
            logger.error(error_msg, exc_info=True)

            return PlaybookTaskResult(
                type=PlaybookTaskResultType.TEXT,
                text=TextResult(output=StringValue(value=error_msg)),
                source=self.source
            )

    def create_ticket(self, time_range: TimeRange, jira_task: Jira,
                      jira_connector: ConnectorProto):
        try:
            if not jira_connector:
                logger.error("Task execution Failed: No JIRA source found")
                raise ValueError("No JIRA source found")

            task = jira_task.create_ticket

            # Extract and validate fields
            project_key = task.project_key.value
            summary = task.summary.value
            description = task.description.value
            issue_type = task.issue_type.value
            priority = task.priority.value
            labels = task.labels.value.split(',') if task.HasField('labels') else None

            # Validate required fields
            if not project_key or not summary or not issue_type:
                missing_fields = []
                if not project_key: missing_fields.append("project_key")
                if not summary: missing_fields.append("summary")
                if not issue_type: missing_fields.append("issue_type")
                error_msg = f"Missing required fields: {', '.join(missing_fields)}"
                logger.error(error_msg)
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value=error_msg)),
                    source=self.source
                )

            # Get JIRA processor
            jira_processor = self.get_connector_processor(jira_connector)

            # Log attempt to create ticket
            logger.info(f"Attempting to create JIRA ticket in project {project_key}")

            # Create the ticket
            result = jira_processor.create_ticket(
                project_key=project_key,
                summary=summary,
                description=description,
                issue_type=issue_type,
                priority=priority,
                labels=labels
            )

            if not result:
                error_msg = "JIRA API returned no result when creating ticket"
                logger.error(error_msg)
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value=error_msg)),
                    source=self.source
                )

            # Convert result to Struct for API response
            ticket_url = f"https://{jira_processor.domain}.atlassian.net/browse/{result['key']}"
            response_dict = {
                'ticket_key': result['key'],
                'ticket_id': result['id'],
                'ticket_url': ticket_url,
                'status': 'Created'
            }

            logger.info(f"Successfully created JIRA ticket {result['key']}")

            response_struct = dict_to_proto(response_dict, Struct)
            jira_output = ApiResponseResult(response_body=response_struct)

            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                source=self.source,
                api_response=jira_output
            )

        except Exception as e:
            error_msg = f"Exception occurred while creating JIRA ticket: {str(e)}"
            logger.error(error_msg, exc_info=True)  # Include full stack trace
            return PlaybookTaskResult(
                type=PlaybookTaskResultType.TEXT,
                text=TextResult(output=StringValue(value=error_msg)),
                source=self.source
            )

    def get_ticket(self, time_range: TimeRange, jira_task: Jira,
                   jira_connector: ConnectorProto):
        try:
            if not jira_connector:
                logger.error("Task execution Failed: No JIRA source found")
                raise ValueError("No JIRA source found")
            task = jira_task.get_ticket
            # Extract and validate fields
            ticket_key = task.ticket_key.value
            # Validate required fields
            if not ticket_key:
                missing_fields = []
                if not ticket_key: missing_fields.append("ticket_key")
                error_msg = f"Missing required fields: {', '.join(missing_fields)}"
                logger.error(error_msg)
                return PlaybookTaskResult(type=PlaybookTaskResultType.TEXT,
                                          text=TextResult(output=StringValue(value=error_msg)), source=self.source)
            # Get JIRA processor
            jira_processor = self.get_connector_processor(jira_connector)
            # Log attempt to get ticket
            logger.info(f"Attempting to get JIRA ticket {ticket_key}")
            # Get the ticket
            result = jira_processor.get_ticket(ticket_key)
            if not result:
                error_msg = "JIRA API returned no result when fetching ticket"
                logger.error(error_msg)
                return PlaybookTaskResult(type=PlaybookTaskResultType.TEXT,
                                          text=TextResult(output=StringValue(value=error_msg)), source=self.source)
            # Convert result to Struct for API response
            response_dict = {'ticket': result}
            logger.info(f"Successfully fetched JIRA ticket {ticket_key}")
            response_struct = dict_to_proto(response_dict, Struct)
            jira_output = ApiResponseResult(response_body=response_struct)
            return PlaybookTaskResult(type=PlaybookTaskResultType.API_RESPONSE, source=self.source,
                                      api_response=jira_output)
        except Exception as e:
            error_msg = f"Exception occurred while fetching JIRA ticket: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return PlaybookTaskResult(type=PlaybookTaskResultType.TEXT,
                                      text=TextResult(output=StringValue(value=error_msg)), source=self.source)

    def search_tickets(self, time_range: TimeRange, jira_task: Jira,
                       jira_connector: ConnectorProto):
        try:
            if not jira_connector:
                logger.error("Task execution Failed: No JIRA source found")
                raise ValueError("No JIRA source found")
            task = jira_task.search_tickets
            # Extract and validate fields
            query = task.query.value
            # Validate required fields
            if not query:
                missing_fields = []
                if not query: missing_fields.append("query")
                error_msg = f"Missing required fields: {', '.join(missing_fields)}"
                logger.error(error_msg)
                return PlaybookTaskResult(type=PlaybookTaskResultType.TEXT,
                                          text=TextResult(output=StringValue(value=error_msg)), source=self.source)
            # Get JIRA processor
            jira_processor = self.get_connector_processor(jira_connector)
            # Log attempt to search tickets
            logger.info(f"Attempting to search JIRA tickets with query: {query}")
            # Search for tickets
            result = jira_processor.search_tickets(query)
            if not result:
                error_msg = "JIRA API returned no result when searching tickets"
                logger.error(error_msg)
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value=error_msg)),
                    source=self.source
                )
            response_dict = {'tickets': result}
            response_struct = dict_to_proto(response_dict, Struct)
            jira_output = ApiResponseResult(response_body=response_struct)
            return PlaybookTaskResult(type=PlaybookTaskResultType.API_RESPONSE, source=self.source,
                                      api_response=jira_output)
        except Exception as e:
            error_msg = f"Exception occurred while searching JIRA tickets: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return PlaybookTaskResult(type=PlaybookTaskResultType.TEXT,
                                      text=TextResult(output=StringValue(value=error_msg)), source=self.source)

    def add_comment(self, time_range: TimeRange, jira_task: Jira,
                    jira_connector: ConnectorProto):
        try:
            if not jira_connector:
                logger.error("Task execution Failed: No JIRA source found")
                raise ValueError("No JIRA source found")

            task = jira_task.add_comment

            # Extract and validate fields
            ticket_key = task.ticket_key.value
            comment_text = task.comment_text.value
            image_urls = [url.value for url in task.image_urls] if hasattr(task, 'image_urls') else []

            # Validate required fields
            if not ticket_key or not comment_text:
                missing_fields = []
                if not ticket_key: missing_fields.append("ticket_key")
                if not comment_text: missing_fields.append("comment_text")
                error_msg = f"Missing required fields: {', '.join(missing_fields)}"
                logger.error(error_msg)
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value=error_msg)),
                    source=self.source
                )

            # Get JIRA processor
            jira_processor = self.get_connector_processor(jira_connector)

            # Log attempt to add comment
            logger.info(f"Attempting to add comment to JIRA ticket {ticket_key}")

            # Add the comment
            result = jira_processor.add_comment(
                ticket_key=ticket_key,
                comment_text=comment_text,
                image_urls=image_urls
            )

            if not result:
                error_msg = "JIRA API returned no result when adding comment"
                logger.error(error_msg)
                return PlaybookTaskResult(
                    type=PlaybookTaskResultType.TEXT,
                    text=TextResult(output=StringValue(value=error_msg)),
                    source=self.source
                )

            # Extract comment information from the result
            comment_id = result.get('id', 'Unknown')
            author = result.get('author', {}).get('displayName', 'Unknown')

            # Prepare the response
            response_dict = {
                'ticket_key': ticket_key,
                'comment_id': comment_id,
                'author': author,
                'status': 'Comment Added'
            }

            logger.info(f"Successfully added comment to JIRA ticket {ticket_key}")

            response_struct = dict_to_proto(response_dict, Struct)
            jira_output = ApiResponseResult(response_body=response_struct)

            return PlaybookTaskResult(
                type=PlaybookTaskResultType.API_RESPONSE,
                source=self.source,
                api_response=jira_output
            )

        except Exception as e:
            error_msg = f"Exception occurred while adding comment to JIRA ticket: {str(e)}"
            logger.error(error_msg, exc_info=True)

            return PlaybookTaskResult(
                type=PlaybookTaskResultType.TEXT,
                text=TextResult(output=StringValue(value=error_msg)),
                source=self.source
            )
