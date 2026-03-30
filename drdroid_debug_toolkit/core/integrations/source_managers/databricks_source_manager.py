import logging

from google.protobuf.wrappers_pb2 import StringValue

from core.integrations.source_api_processors.databricks_api_processor import DatabricksApiProcessor
from core.integrations.source_manager import SourceManager
from core.protos.base_pb2 import Source, SourceKeyType
from core.protos.ui_definition_pb2 import FormField, FormFieldType
from core.protos.literal_pb2 import LiteralType
from core.utils.credentilal_utils import generate_credentials_dict, get_connector_key_type_string, DISPLAY_NAME, CATEGORY, CLOUD_MANAGED_SERVICES

logger = logging.getLogger(__name__)


class DatabricksSourceManager(SourceManager):

    def __init__(self):
        self.source = Source.DATABRICKS
        self.task_proto = None
        self.task_type_callable_map = {}
        self.connector_form_configs = [
            {
                "name": StringValue(value="Databricks Personal Access Token Authentication"),
                "description": StringValue(value="Connect to Databricks using your workspace URL and a personal access token."),
                "form_fields": {
                    SourceKeyType.DATABRICKS_HOST: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.DATABRICKS_HOST)),
                        display_name=StringValue(value="Workspace URL"),
                        description=StringValue(value='e.g. "https://adb-1234567890123456.7.azuredatabricks.net"'),
                        helper_text=StringValue(value="Enter your Databricks workspace URL"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=False
                    ),
                    SourceKeyType.DATABRICKS_TOKEN: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.DATABRICKS_TOKEN)),
                        display_name=StringValue(value="Personal Access Token"),
                        description=StringValue(value='e.g. "dapi1234567890abcdef"'),
                        helper_text=StringValue(value="Enter your Databricks personal access token"),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=False,
                        is_sensitive=True
                    ),
                }
            }
        ]
        self.connector_type_details = {
            DISPLAY_NAME: "Databricks",
            CATEGORY: CLOUD_MANAGED_SERVICES,
        }

    def get_connector_processor(self, databricks_connector, **kwargs):
        generated_credentials = generate_credentials_dict(databricks_connector.type, databricks_connector.keys)
        return DatabricksApiProcessor(**generated_credentials)

    def test_connector_processor(self, connector, **kwargs):
        try:
            processor = self.get_connector_processor(connector, **kwargs)
            processor.test_connection()
            return True, "Databricks connection successful."
        except Exception as e:
            return False, f"Databricks connection failed: {str(e)}"
