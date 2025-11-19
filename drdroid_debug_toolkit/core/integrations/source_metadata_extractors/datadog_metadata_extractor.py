import logging

from core.integrations.source_metadata_extractor import SourceMetadataExtractor
from core.integrations.source_api_processors.datadog_api_processor import DatadogApiProcessor, extract_services_and_downstream
from core.protos.base_pb2 import Source, SourceModelType
from core.utils.logging_utils import log_function_call

logger = logging.getLogger(__name__)


class DatadogSourceMetadataExtractor(SourceMetadataExtractor):

    def __init__(self, request_id: str, connector_name: str, dd_app_key, dd_api_key, dd_api_domain='datadoghq.com'):
        self.__dd_api_processor = DatadogApiProcessor(dd_app_key, dd_api_key, dd_api_domain)
        super().__init__(request_id, connector_name, Source.DATADOG)

    @log_function_call
    def extract_services(self):
        model_type = SourceModelType.DATADOG_SERVICE
        model_data = {}
        prod_env_tags = ['prod', 'production', 'prd', 'prod_env', 'production_env', 'production_environment',
                         'prod_environment']
        for tag in prod_env_tags:
            try:
                services = self.__dd_api_processor.fetch_service_map(tag)
            except Exception as e:
                logger.error(f'Error fetching datadog services for env: {tag} - {e}')
                continue
            if not services:
                continue
            for service, metadata in services.items():
                service_metadata = model_data.get(service, {})
                service_metadata[tag] = metadata
                model_data[service] = service_metadata
        try:
            all_metrics = self.__dd_api_processor.fetch_metrics().get('data', [])
        except Exception as e:
            logger.error(f'Error fetching datadog metrics: {e}')
            all_metrics = []
        if not all_metrics:
            logger.info(f'Extracted {len(model_data)} datadog services. Starting processing service relationships...')
            extract_services_and_downstream(self.account_id, model_data)
            if len(model_data) > 0:
                self.create_or_update_model_metadata(model_type, model_data)
            return model_data
        logger.info(f'Extracted {len(model_data)} datadog services. Starting processing metrics...')
        service_metric_map = {}
        for mt in all_metrics:
            try:
                tags = self.__dd_api_processor.fetch_metric_tags(mt['id']).get('data', {}).get('attributes', {}).get(
                    'tags', [])
            except Exception as e:
                logger.error(f'Error fetching datadog metric tags for metric: {mt["id"]} - {e}')
                tags = []
            family = mt['id'].split('.')[0]
            for tag in tags:
                if tag.startswith('service:'):
                    service = tag.split(':')[1]
                    metrics = service_metric_map.get(service, [])
                    essential_tags = [tag for tag in tags if tag.startswith('env:') or tag.startswith('service:')]
                    metrics.append({'id': mt['id'], 'type': mt['type'], 'family': family, 'tags': essential_tags})
                    service_metric_map[service] = metrics
        for service, metrics in service_metric_map.items():
            service_model_data = model_data.get(service, {})
            service_model_data['metrics'] = metrics
            model_data[service] = service_model_data
        logger.info(f'Extracted {len(model_data)} datadog services. Starting processing service relationships...')
        extract_services_and_downstream(self.account_id, model_data)
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_monitor(self):
        model_type = SourceModelType.DATADOG_MONITOR
        model_data = {}
        try:
            monitors = self.__dd_api_processor.fetch_monitors()
            if not monitors or len(monitors) == 0:
                return model_data
            for monitor in monitors:
                monitor_dict = monitor.to_dict()
                monitor_id = str(monitor_dict['id'])
                model_data[monitor_id] = monitor_dict
        except Exception as e:
            logger.error(f'Error extracting datadog monitors: {e}')
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_dashboard(self):
        model_type = SourceModelType.DATADOG_DASHBOARD
        model_data = {}
        try:
            response = self.__dd_api_processor.fetch_dashboards()
            if not response or 'dashboards' not in response:
                return model_data
            dashboards = response['dashboards']
            dashboard_ids = [dashboard['id'] for dashboard in dashboards]
            for dashboard_id in dashboard_ids:
                try:
                    dashboard = self.__dd_api_processor.fetch_dashboard_details(dashboard_id)
                except Exception as e:
                    logger.error(f'Error fetching datadog dashboard details for dashboard_id: {dashboard_id} - {e}')
                    continue
                if not dashboard:
                    continue
                dashboard_id = str(dashboard['id'])
                model_data[dashboard_id] = dashboard
        except Exception as e:
            logger.error(f'Error extracting datadog dashboards: {e}')
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_active_aws_integrations(self):
        model_type = SourceModelType.DATADOG_LIVE_INTEGRATION_AWS
        model_data = {}
        try:
            response = self.__dd_api_processor.fetch_aws_integrations()
            if not response or 'accounts' not in response:
                return model_data
            aws_accounts = response['accounts']
            for account in aws_accounts:
                aws_account_id = str(account['account_id'])
                enabled_account_specific_namespace_rules = {}
                for service, enabled in account['account_specific_namespace_rules'].items():
                    if enabled:
                        enabled_account_specific_namespace_rules[service] = enabled
                account['account_specific_namespace_rules'] = enabled_account_specific_namespace_rules
                model_data[aws_account_id] = account
        except Exception as e:
            logger.error(f'Error extracting datadog active aws integrations: {e}')
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_active_aws_log_integrations(self):
        model_type = SourceModelType.DATADOG_LIVE_INTEGRATION_AWS_LOG
        model_data = {}
        try:
            response = self.__dd_api_processor.fetch_aws_log_integrations()
            if not response or len(response) == 0:
                return model_data
            for account in response:
                account_dict = account.to_dict()
                aws_account_id = str(account_dict['account_id'])
                model_data[aws_account_id] = account_dict
        except Exception as e:
            logger.error(f'Error extracting datadog active aws log integrations: {e}')
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_active_azure_integrations(self):
        model_type = SourceModelType.DATADOG_LIVE_INTEGRATION_AZURE
        model_data = {}
        try:
            response = self.__dd_api_processor.fetch_azure_integrations()
            if not response or response.value is None:
                return model_data
            for azure_account in response.value:
                client_id = str(azure_account.get('client_id', None))
                if client_id:
                    model_data[client_id] = azure_account
        except Exception as e:
            logger.error(f'Error extracting datadog active azure integrations: {e}')
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_active_cloudflare_integrations(self):
        model_type = SourceModelType.DATADOG_LIVE_INTEGRATION_CLOUDFLARE
        model_data = {}
        try:
            response = self.__dd_api_processor.fetch_cloudflare_integrations()
            if not response or 'data' not in response:
                return model_data
            data = response['data']
            for ca in data:
                c_id = str(ca['id'])
                model_data[c_id] = ca
        except Exception as e:
            logger.error(f'Error extracting datadog active cloudflare integrations: {e}')
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_active_confluent_integrations(self):
        model_type = SourceModelType.DATADOG_LIVE_INTEGRATION_CONFLUENT
        model_data = {}
        try:
            response = self.__dd_api_processor.fetch_confluent_integrations()
            if not response or 'data' not in response:
                return model_data
            data = response['data']
            for ca in data:
                c_id = str(ca['id'])
                model_data[c_id] = ca
        except Exception as e:
            logger.error(f'Error extracting datadog active confluent integrations: {e}')
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_active_fastly_integrations(self):
        model_type = SourceModelType.DATADOG_LIVE_INTEGRATION_FASTLY
        model_data = {}
        try:
            response = self.__dd_api_processor.fetch_fastly_integrations()
            if not response or 'data' not in response:
                return model_data
            data = response['data']
            for fa in data:
                f_id = str(fa['id'])
                model_data[f_id] = fa
        except Exception as e:
            logger.error(f'Error extracting datadog active fastly integrations: {e}')
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_active_gcp_integrations(self):
        model_type = SourceModelType.DATADOG_LIVE_INTEGRATION_GCP
        model_data = {}
        try:
            response = self.__dd_api_processor.fetch_gcp_integrations()
            if not response or 'data' not in response:
                return model_data
            data = response['data']
            for gcpa in data:
                gcp_id = str(gcpa['id'])
                model_data[gcp_id] = gcpa
        except Exception as e:
            logger.error(f'Error extracting datadog active gcp integrations: {e}')
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_metrics(self):
        model_type = SourceModelType.DATADOG_METRIC
        model_data = {}

        try:
            all_metrics = self.__dd_api_processor.fetch_metrics().get('data', [])
        except Exception as e:
            logger.error(f'Error fetching datadog metrics: {e}')
            all_metrics = []
        if not all_metrics:
            return model_data
        for mt in all_metrics:
            try:
                tags = self.__dd_api_processor.fetch_metric_tags(mt['id']).get('data', {}).get('attributes', {}).get(
                    'tags', [])
            except Exception as e:
                logger.error(f'Error fetching datadog metric tags for metric: {mt["id"]} - {e}')
                tags = []
            family = mt['id'].split('.')[0]
            model_data[mt['id']] = {**mt, 'tags': tags, 'family': family}
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_fields(self, log_limit=5000):
        """
        Extract unique fields, tags, and attributes from recent Datadog logs.
        
        Args:
            log_limit (int): Maximum number of logs to analyze (default: 10000)
            
        Returns:
            dict: Dictionary containing extracted field information
        """
        model_type = SourceModelType.DATADOG_FIELDS
        model_data = {}
        
        try:
            # Import required modules
            from datetime import datetime, timezone, timedelta
            from collections import defaultdict, Counter
            import json
            
            # Calculate time range for last 24 hours
            end_time = datetime.now(timezone.utc)
            start_time = end_time - timedelta(hours=24)
            
            # Convert to Unix timestamps
            start_timestamp = int(start_time.timestamp())
            end_timestamp = int(end_time.timestamp())
            
            # Fetch recent logs
            logger.info(f"Requesting {log_limit} logs for field extraction from {start_timestamp} to {end_timestamp}")
            logs = self.__dd_api_processor.fetch_logs_for_field_extraction(
                start_time=start_timestamp,
                end_time=end_timestamp,
                limit=log_limit
            )
            logger.info(f"Successfully fetched {len(logs) if logs else 0} logs for field extraction")
            
            if not logs:
                logger.warning("No logs found for field extraction")
                return model_data
            
            # Initialize data structures for collecting field information
            field_info = defaultdict(lambda: {'type': 'string', 'values': set(), 'count': 0})
            tag_info = defaultdict(lambda: {'values': set(), 'count': 0})
            attribute_info = defaultdict(lambda: {'type': 'string', 'values': set(), 'count': 0})
            
            total_logs_analyzed = 0
            
            # Process each log entry
            for log_entry in logs:
                total_logs_analyzed += 1
                
                # Extract basic fields from log structure
                if 'id' in log_entry:
                    field_info['id']['values'].add(str(log_entry['id']))
                    field_info['id']['count'] += 1
                
                if 'type' in log_entry:
                    field_info['type']['values'].add(str(log_entry['type']))
                    field_info['type']['count'] += 1
                
                # Extract attributes
                if 'attributes' in log_entry:
                    attributes = log_entry['attributes']
                    
                    for attr_name, attr_value in attributes.items():
                        # Determine attribute type
                        if isinstance(attr_value, bool):
                            attr_type = 'boolean'
                        elif isinstance(attr_value, (int, float)):
                            attr_type = 'number'
                        elif isinstance(attr_value, list):
                            attr_type = 'array'
                        elif isinstance(attr_value, dict):
                            attr_type = 'object'
                        else:
                            attr_type = 'string'
                        
                        # Store attribute information
                        attribute_info[attr_name]['type'] = attr_type
                        attribute_info[attr_name]['values'].add(str(attr_value))
                        attribute_info[attr_name]['count'] += 1
                        
                        # Also store as field for backward compatibility
                        field_info[attr_name]['type'] = attr_type
                        field_info[attr_name]['values'].add(str(attr_value))
                        field_info[attr_name]['count'] += 1
                        
                        # Extract tags from attributes
                        if attr_name == 'tags' and isinstance(attr_value, list):
                            for tag in attr_value:
                                if isinstance(tag, str) and ':' in tag:
                                    tag_name = tag.split(':', 1)[0]
                                    tag_value = tag.split(':', 1)[1]
                                    tag_info[tag_name]['values'].add(tag_value)
                                    tag_info[tag_name]['count'] += 1
            
            # Convert to the required format
            fields_data = {
                'source': 'logs',
                'fields': [],
                'tags': [],
                'attributes': [],
                'total_logs_analyzed': total_logs_analyzed,
                'extraction_timestamp': int(datetime.now(timezone.utc).timestamp())
            }
            
            # Process field information
            for field_name, info in field_info.items():
                if info['count'] > 0:  # Only include fields that actually appeared
                    fields_data['fields'].append({
                        'field_name': field_name,
                        'field_type': info['type'],
                    })
            
            # Process tag information
            for tag_name, info in tag_info.items():
                if info['count'] > 0:  # Only include tags that actually appeared
                    fields_data['tags'].append({
                        'tag_name': tag_name,
                    })
            
            # Process attribute information
            for attr_name, info in attribute_info.items():
                if info['count'] > 0:  # Only include attributes that actually appeared
                    fields_data['attributes'].append({
                        'attribute_name': attr_name,
                        'attribute_type': info['type'],
                    })
            
            # Store in model_data
            model_data['log_fields'] = fields_data
            
            # Save to database if requested
            if len(model_data) > 0:
                self.create_or_update_model_metadata(model_type, model_data)
            
            logger.info(f"Extracted {len(fields_data['fields'])} fields, {len(fields_data['tags'])} tags, "
                       f"and {len(fields_data['attributes'])} attributes from {total_logs_analyzed} logs")
            
        except Exception as e:
            logger.error(f'Error extracting datadog fields: {e}')
            
        return model_data
