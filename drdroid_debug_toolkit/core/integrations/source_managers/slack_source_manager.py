import logging
import os

from google.protobuf.wrappers_pb2 import StringValue

from core.integrations.source_api_processors.slack_api_processor import SlackApiProcessor
from core.integrations.source_manager import SourceManager
from core.protos.base_pb2 import Source, TimeRange, SourceModelType, SourceKeyType
from core.protos.connectors.connector_pb2 import Connector as ConnectorProto
from core.protos.literal_pb2 import LiteralType
from core.protos.playbooks.playbook_commons_pb2 import PlaybookTaskResult, PlaybookTaskResultType
from core.protos.playbooks.source_task_definitions.slack_task_pb2 import Slack
from core.protos.ui_definition_pb2 import FormField, FormFieldType
from core.utils.credentilal_utils import generate_credentials_dict, get_connector_key_type_string, DISPLAY_NAME, CATEGORY, ALERTING
from core.utils.proto_utils import proto_to_dict

logger = logging.getLogger(__name__)


class SlackSourceManager(SourceManager):

    def __init__(self):
        self.source = Source.SLACK
        self.task_proto = Slack
        self.task_type_callable_map = {
            Slack.TaskType.SEND_MESSAGE: {
                'executor': self.execute_send_message,
                'model_types': [SourceModelType.SLACK_CHANNEL],
                'result_type': PlaybookTaskResultType.TEXT,
                'display_name': 'Send a message to slack channel',
                'category': 'Actions',
                'form_fields': [
                    FormField(key_name=StringValue(value="channel"),
                              display_name=StringValue(value="Channel"),
                              description=StringValue(value="e.g. #general, #alerts, #team-dev"),
                              helper_text=StringValue(value="Select Slack Channel"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TYPING_DROPDOWN_FT),
                    FormField(key_name=StringValue(value="text"),
                              display_name=StringValue(value="Message"),
                              description=StringValue(value='e.g. "Alert: High CPU usage detected", "Deployment completed successfully"'),
                              helper_text=StringValue(value="Enter Message"),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.MULTILINE_FT),
                ]
            },
            # SlackTaskProto.TaskType.SEND_THREAD_REPLY: {
            #     'executor': self.execute_send_thread_reply,
            #     'model_types': [SourceModelType.SLACK_CHANNEL],
            #     'result_type': PlaybookTaskResultType.TEXT,
            #     'display_name': 'Send a reply to a thread in slack channel',
            #     'category': 'Actions'
            # },
        }
        self.connector_form_configs = [
            {
                "name": StringValue(value="Slack Bot Token Connection"),
                "description": StringValue(value="Connect to Slack using a Bot User OAuth Token."),
                "form_fields": {
                    SourceKeyType.SLACK_BOT_AUTH_TOKEN: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.SLACK_BOT_AUTH_TOKEN)),
                        display_name=StringValue(value="Bot Auth Token"),
                        description=StringValue(value="Your bot auth token that starts with xoxb-"),
                        helper_text=StringValue(value="Enter your Slack Bot User OAuth Token (starts with xoxb-)."),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=False
                    )
                }
            },
            {
                "name": StringValue(value="Slack App Credentials Connection"),
                "description": StringValue(value="Connect to Slack using App credentials (App ID, Client ID, Client Secret, Signing Secret, Bot Auth Token)."),
                "form_fields": {
                    SourceKeyType.SLACK_APP_ID: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.SLACK_APP_ID)),
                        display_name=StringValue(value="App ID"),
                        description=StringValue(value="e.g. A1234567890"),
                        helper_text=StringValue(value="Enter your Slack App ID."),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=False
                    ),
                    SourceKeyType.SLACK_APP_CLIENT_ID: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.SLACK_APP_CLIENT_ID)),
                        display_name=StringValue(value="Client ID"),
                        description=StringValue(value="e.g. 1234567890.1234567890"),
                        helper_text=StringValue(value="Enter your Slack App Client ID."),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=False
                    ),
                    SourceKeyType.SLACK_APP_CLIENT_SECRET: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.SLACK_APP_CLIENT_SECRET)),
                        display_name=StringValue(value="Client Secret"),
                        description=StringValue(value="e.g. 1234567890abcdef1234567890abcdef"),
                        helper_text=StringValue(value="Enter your Slack App Client Secret."),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=False,
                        is_sensitive=True
                    ),
                    SourceKeyType.SLACK_APP_SIGNING_SECRET: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.SLACK_APP_SIGNING_SECRET)),
                        display_name=StringValue(value="Signing Secret"),
                        description=StringValue(value="e.g. 1234567890abcdef1234567890abcdef"),
                        helper_text=StringValue(value="Enter your Slack App Signing Secret."),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=False,
                        is_sensitive=True
                    ),
                    SourceKeyType.SLACK_BOT_AUTH_TOKEN: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.SLACK_BOT_AUTH_TOKEN)),
                        display_name=StringValue(value="Bot Auth Token"),
                        description=StringValue(value="Your bot auth token that starts with xoxb-"),
                        helper_text=StringValue(value="Enter your Slack Bot User OAuth Token (starts with xoxb-)."),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=False,
                        is_sensitive=True
                    )
                }
            }
        ]
        self.connector_type_details = {
            DISPLAY_NAME: "SLACK",
            CATEGORY: ALERTING,
        }

    @staticmethod
    def validate_connector(connector: ConnectorProto) -> bool:
        from core.integrations.source_facade import source_facade as playbook_source_facade
        if connector.is_proxy_enabled.value:
            return True
        keys = connector.keys
        all_ck_types = [ck.key_type for ck in keys if ck.key.value]
        all_ck_types = list(set(all_ck_types))
        required_key_types = playbook_source_facade.get_connector_required_keys(connector.type)
        all_keys_found = False
        for rkt in required_key_types:
            if set(rkt) <= set(all_ck_types):
                all_keys_found = True
                break
        return all_keys_found

    def get_connector_processor(self, grafana_connector, **kwargs):
        generated_credentials = generate_credentials_dict(grafana_connector.type, grafana_connector.keys)
        return SlackApiProcessor(**generated_credentials)

    def execute_send_message(self, time_range: TimeRange, slack_task: Slack,
                             slack_connector: ConnectorProto):
        try:
            if not slack_connector:
                raise Exception("Task execution Failed:: No Postgres source found")

            send_message_task: Slack.SendMessage = slack_task.send_message
            channel = send_message_task.channel.value
            text = send_message_task.text.value
            blocks = proto_to_dict(send_message_task.blocks) if send_message_task.blocks else None
            file_uploads = proto_to_dict(send_message_task.file_uploads) if send_message_task.file_uploads else []
            if not channel:
                raise Exception("Task execution Failed:: No Slack channel found")

            slack_api_processor = self.get_connector_processor(slack_connector)
            print("Playbook Task Downstream Request: Type -> {}, Account -> {}, Channel -> {}".format("Slack",
                                                                                                      slack_connector.account_id.value,
                                                                                                      channel),
                  flush=True)

            if blocks:
                message_params = {'blocks': blocks, 'channel_id': channel}
            elif text:
                message_params = {'text_message': text, 'channel_id': channel}
            else:
                raise ValueError("No message content found")

            slack_api_processor.send_bot_message(**message_params)
            for file_upload in file_uploads:
                try:
                    file_upload['channel_id'] = channel
                    slack_api_processor.files_upload(**file_upload)
                    os.remove(file_upload['file_path'])
                except Exception as e:
                    logger.error(f"Error uploading file to slack: {e}")
                    continue
            return PlaybookTaskResult(source=self.source, type=PlaybookTaskResultType.TEXT)
        except Exception as e:
            raise Exception(f"Error while executing Slack send message task: {e}")

    def execute_send_thread_reply(self, time_range: TimeRange, slack_task: Slack,
                                  slack_connector: ConnectorProto):
        try:
            if not slack_connector:
                raise Exception("Task execution Failed:: No Postgres source found")

            send_message_task: Slack.SendThreadReply = slack_task.send_message
            channel = send_message_task.channel.value
            thread_ts = send_message_task.thread_ts.value
            text = send_message_task.text.value
            blocks = proto_to_dict(send_message_task.blocks)
            file_uploads = proto_to_dict(send_message_task.file_uploads)
            if not channel or not thread_ts:
                raise Exception("Task execution Failed:: No Slack channel or thread_ts found")

            slack_api_processor = self.get_connector_processor(slack_connector)
            print("Playbook Task Downstream Request: Type -> {}, Account -> {}, Channel -> {}".format("Slack",
                                                                                                      slack_connector.account_id.value,
                                                                                                      channel),
                  flush=True)

            if blocks:
                message_params = {'blocks': blocks, 'channel_id': channel, 'reply_to': thread_ts}
            elif text:
                message_params = {'text_message': text, 'channel_id': channel}
            else:
                raise ValueError("No message content found")

            slack_api_processor.send_bot_message(**message_params)
            for file_upload in file_uploads:
                try:
                    file_upload['channel_id'] = channel
                    file_upload['thread_ts'] = thread_ts
                    slack_api_processor.files_upload(**file_upload)
                    os.remove(file_upload['file_path'])
                except Exception as e:
                    logger.error(f"Error uploading file to slack: {e}")
                    continue
            return PlaybookTaskResult(source=self.source, type=PlaybookTaskResultType.TEXT)
        except Exception as e:
            raise Exception(f"Error while executing Slack send thread reply task: {e}")
