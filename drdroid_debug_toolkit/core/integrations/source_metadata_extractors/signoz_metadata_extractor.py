import logging

from core.integrations.source_api_processors.signoz_api_processor import SignozApiProcessor
from core.integrations.source_metadata_extractor import SourceMetadataExtractor
from core.protos.base_pb2 import Source, SourceModelType
from core.utils.logging_utils import log_function_call

logger = logging.getLogger(__name__)


class SignozSourceMetadataExtractor(SourceMetadataExtractor):

    def __init__(self, request_id: str, connector_name: str, signoz_api_url, signoz_api_token=None):
        self.__signoz_api_processor = SignozApiProcessor(signoz_api_url, signoz_api_token)
        super().__init__(request_id, connector_name, Source.SIGNOZ)

    @log_function_call
    def extract_dashboards(self):
        model_type = SourceModelType.SIGNOZ_DASHBOARD
        model_data = {}
        try:
            response = self.__signoz_api_processor.fetch_dashboards()
            if not response:
                return model_data
            dashboards = response.get('data', [])
            dashboard_ids = [dashboard.get('uuid', dashboard['id']) for dashboard in dashboards]
            
            for dashboard_id in dashboard_ids:
                try:
                    dashboard = self.__signoz_api_processor.fetch_dashboard_details(dashboard_id)
                except Exception as e:
                    logger.error(f'Error fetching signoz dashboard details for dashboard_id: {dashboard_id} - {e}')
                    continue
                if not dashboard:
                    continue
                dashboard_title = dashboard.get('data', {}).get('title')
                dashboard_id = str(dashboard['id'])
                model_data[dashboard_id] = dashboard
                
            if len(model_data) > 0:
                self.create_or_update_model_metadata(model_type, model_data)
        except Exception as e:
            logger.error(f'Error extracting signoz dashboards: {e}')
            
        return model_data

    @log_function_call
    def extract_services(self):
        model_type = SourceModelType.SIGNOZ_SERVICE
        model_data = {}
        try:
            response = self.__signoz_api_processor.fetch_services()
            if not response:
                return model_data

            # Check if response has error status (when it's a dict with status field)
            if isinstance(response, dict) and response.get('status') == 'error':
                logger.error(f'Error response when fetching signoz services: {response.get("message")}')
                return model_data
                
            # Handle different response formats
            services = []
            if isinstance(response, dict):
                services = response.get('data', [])
            elif isinstance(response, list):
                # Response is directly a list of services
                services = response
            
            for service in services:
                try:
                    service_name = service.get('serviceName')
                    if not service_name:
                        continue
                        
                    # Use service name as model_uid as requested
                    model_data[service_name] = service
                except Exception as e:
                    logger.error(f'Error processing signoz service: {service} - {e}')
                    continue
                    
            if len(model_data) > 0:
                self.create_or_update_model_metadata(model_type, model_data)
        except Exception as e:
            logger.error(f'Error extracting signoz services: {e}')
            
        return model_data

    @log_function_call
    def extract_log_attributes(self):
        """
        Extract log attributes and their types from SigNoz logs.
        Uses ClickHouse query to get all unique attribute keys and their types.
        """
        model_type = SourceModelType.SIGNOZ_LOG_ATTRIBUTES
        model_data = {}
        
        try:
            # ClickHouse query to get all log attributes and their types
            query = """
            SELECT attr_key, attr_type, source
            FROM
            (
              SELECT arrayJoin(mapKeys(attributes_string)) AS attr_key, 'string' AS attr_type, 'attribute' AS source FROM signoz_logs.distributed_logs_v2
              UNION ALL
              SELECT arrayJoin(mapKeys(attributes_number)) AS attr_key, 'number' AS attr_type, 'attribute' AS source FROM signoz_logs.distributed_logs_v2
              UNION ALL
              SELECT arrayJoin(mapKeys(attributes_bool)) AS attr_key, 'bool' AS attr_type, 'attribute' AS source FROM signoz_logs.distributed_logs_v2
              UNION ALL
              SELECT arrayJoin(mapKeys(resources_string)) AS attr_key, 'string' AS attr_type, 'resource' AS source FROM signoz_logs.distributed_logs_v2
            )
            GROUP BY attr_key, attr_type, source
            ORDER BY source, attr_key
            """
            
            # Execute the query using the API processor
            from datetime import datetime, timezone, timedelta
            
            # Use a 1-month time range for the query
            end_time = datetime.now(timezone.utc)
            start_time = end_time - timedelta(days=30)
            
            result = self.__signoz_api_processor.signoz_query_clickhouse(
                query=query,
                time_geq=int(start_time.timestamp()),
                time_lt=int(end_time.timestamp())
            )
            logger.debug(f"SigNoz log attributes response: {result}")
            if not result or "error" in result:
                logger.error(f"Failed to execute log attributes query: {result}")
                return model_data
            
            # Parse the results and store all attributes in a single row
            all_attributes = []
            # Handle new response format: data.data.results[].rows[]
            results_data = result.get("data", {}).get("data", {}).get("results", [])
            if results_data:
                for query_result in results_data:
                    rows = query_result.get("rows", [])
                    for row in rows:
                        row_data = row.get("data", {})
                        if isinstance(row_data, dict):
                            attr_key = row_data.get("attr_key")
                            attr_type = row_data.get("attr_type")
                            source = row_data.get("source")

                            if attr_key and attr_type and source:
                                attribute_info = {
                                    "attribute_key": attr_key,
                                    "attribute_type": attr_type,
                                    "source": source,
                                    "description": f"Log {source} attribute of type {attr_type}"
                                }
                                all_attributes.append(attribute_info)
            # Fallback to old format: data.result[].table.rows[]
            elif result.get("data", {}).get("result"):
                for item in result["data"]["result"]:
                    table = item.get("table", {})
                    rows = table.get("rows", [])
                    for row in rows:
                        row_data = row.get("data", {})
                        if isinstance(row_data, dict):
                            attr_key = row_data.get("attr_key")
                            attr_type = row_data.get("attr_type")
                            source = row_data.get("source")

                            if attr_key and attr_type and source:
                                attribute_info = {
                                    "attribute_key": attr_key,
                                    "attribute_type": attr_type,
                                    "source": source,
                                    "description": f"Log {source} attribute of type {attr_type}"
                                }
                                all_attributes.append(attribute_info)
            
            # Store all attributes in a single row
            if all_attributes:
                single_row_data = {
                    "attributes": all_attributes,
                    "total_count": len(all_attributes),
                    "extraction_timestamp": int(end_time.timestamp() * 1000),
                    "time_range_days": 30,
                    "description": f"All log attributes discovered from SigNoz logs over the past 30 days"
                }
                
                # Use a single unique key for all log attributes
                unique_key = "signoz_log_attributes_all"
                model_data[unique_key] = single_row_data
            if len(model_data) > 0:
                self.create_or_update_model_metadata(model_type, model_data)
            logger.info(f"Extracted {len(all_attributes)} log attributes from SigNoz")
            
        except Exception as e:
            logger.error(f'Error extracting signoz log attributes: {e}')
            
        return model_data