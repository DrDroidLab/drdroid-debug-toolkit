import logging

from google.protobuf.wrappers_pb2 import StringValue

from core.integrations.source_api_processors.metabase_api_processor import MetabaseApiProcessor
from core.integrations.source_manager import SourceManager
from core.protos.base_pb2 import Source, SourceKeyType
from core.protos.literal_pb2 import LiteralType
from core.protos.ui_definition_pb2 import FormField, FormFieldType
from core.utils.credentilal_utils import generate_credentials_dict, get_connector_key_type_string, DISPLAY_NAME, CATEGORY, ANALYTICS

logger = logging.getLogger(__name__)


class MetabaseSourceManager(SourceManager):

    def __init__(self):
        self.source = Source.METABASE
        self.task_proto = None
        self.task_type_callable_map = {}

        self.connector_form_configs = [
            {
                "name": StringValue(value="Metabase Connection"),
                "description": StringValue(value="Connect to Metabase using your instance URL and API Key."),
                "form_fields": {
                    SourceKeyType.METABASE_URL: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.METABASE_URL)),
                        display_name=StringValue(value="Metabase URL"),
                        helper_text=StringValue(value="Enter your Metabase instance URL"),
                        description=StringValue(value='e.g. "https://your-metabase-instance.com"'),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=False
                    ),
                    SourceKeyType.METABASE_API_KEY: FormField(
                        key_name=StringValue(value=get_connector_key_type_string(SourceKeyType.METABASE_API_KEY)),
                        display_name=StringValue(value="API Key"),
                        helper_text=StringValue(value="Enter your Metabase API Key"),
                        description=StringValue(value='Generate an API key from Admin Settings > Authentication > API Keys'),
                        data_type=LiteralType.STRING,
                        form_field_type=FormFieldType.TEXT_FT,
                        is_optional=False,
                        is_sensitive=True
                    ),
                }
            }
        ]
        self.connector_type_details = {
            DISPLAY_NAME: "METABASE",
            CATEGORY: ANALYTICS,
        }

    def get_connector_processor(self, metabase_connector, **kwargs):
        generated_credentials = generate_credentials_dict(metabase_connector.type, metabase_connector.keys)
        return MetabaseApiProcessor(**generated_credentials)

    def test_connector_processor(self, connector, **kwargs):
        """
        Test the Metabase connector by calling the test_connection method.

        Returns:
            Tuple[bool, str]: (success_status, message)
        """
        try:
            processor = self.get_connector_processor(connector, **kwargs)
            if processor.test_connection():
                return True, "Metabase connection successful."
            return False, "Metabase connection test failed."
        except Exception as e:
            logger.error(f"Error testing Metabase connection: {e}")
            return False, str(e)
