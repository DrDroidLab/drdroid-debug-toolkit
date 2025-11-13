import json
import logging

from core.integrations.source_metadata_extractor import SourceMetadataExtractor
from core.integrations.source_api_processors.datadog_api_processor import DatadogApiProcessor
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
        model_data = self._collect_services_data()
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    def _collect_services_data(self):
        """Collect services data - used by both extract_services and get_services_data."""
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
            return model_data
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
        return model_data

    def get_services_data(self):
        """Get services data directly without storing to database."""
        return self._collect_services_data()



    @log_function_call
    def extract_monitor(self):
        model_type = SourceModelType.DATADOG_MONITOR
        model_data = self._collect_monitors_data()
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    def _collect_monitors_data(self):
        """Collect monitors data - used by both extract_monitor and get_monitors_data."""
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
            logger.error(f'Error extracting monitors: {e}')
        return model_data

    def get_monitors_data(self):
        """Get monitors data directly without storing to database."""
        return self._collect_monitors_data()

    @log_function_call
    def extract_dashboard(self):
        model_type = SourceModelType.DATADOG_DASHBOARD
        model_data = self._collect_dashboards_data()
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    def _collect_dashboards_data(self):
        """Collect dashboards data - used by both extract_dashboard and get_dashboards_data."""
        model_data = {}
        try:
            response = self.__dd_api_processor.fetch_dashboards()
            if isinstance(response, str):
                try:
                    response = json.loads(response)
                except json.JSONDecodeError as decode_error:
                    logger.error(f'Error parsing datadog dashboards response: {decode_error}')
                    return model_data
            if not response or 'dashboards' not in response:
                return model_data
            dashboards = response['dashboards']
            dashboard_ids = [dashboard['id'] for dashboard in dashboards]
            for dashboard_id in dashboard_ids:
                try:
                    dashboard = self.__dd_api_processor.fetch_dashboard_details(dashboard_id)
                except Exception as e:
                    logger.error(f'Error fetching dashboard details for dashboard_id: {dashboard_id} - {e}')
                    continue
                if not dashboard:
                    continue
                dashboard_id = str(dashboard['id'])
                model_data[dashboard_id] = dashboard
        except Exception as e:
            logger.error(f'Error extracting dashboards: {e}')
        return model_data

    def get_dashboards_data(self):
        """Get dashboards data directly without storing to database."""
        return self._collect_dashboards_data()

    @log_function_call
    def extract_active_aws_integrations(self):
        model_type = SourceModelType.DATADOG_LIVE_INTEGRATION_AWS
        model_data = {}
        try:
            response = self.__dd_api_processor.fetch_aws_integrations()
            if not response or 'accounts' not in response:
                return
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
            logger.error(f'Error extracting active aws integrations: {e}')
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)

    @log_function_call
    def extract_active_aws_log_integrations(self):
        model_type = SourceModelType.DATADOG_LIVE_INTEGRATION_AWS_LOG
        model_data = {}
        try:
            response = self.__dd_api_processor.fetch_aws_log_integrations()
            if not response or len(response) == 0:
                return
            for account in response:
                account_dict = account.to_dict()
                aws_account_id = str(account_dict['account_id'])
                model_data[aws_account_id] = account_dict
        except Exception as e:
            logger.error(f'Error extracting active aws log integrations: {e}')
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)

    @log_function_call
    def extract_active_azure_integrations(self):
        model_type = SourceModelType.DATADOG_LIVE_INTEGRATION_AZURE
        model_data = {}
        try:
            response = self.__dd_api_processor.fetch_azure_integrations()
            if not response or response.value is None:
                return
            for azure_account in response.value:
                client_id = str(azure_account.get('client_id', None))
                if client_id:
                    model_data[client_id] = azure_account
        except Exception as e:
            logger.error(f'Error extracting active azure integrations: {e}')
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)

    @log_function_call
    def extract_active_cloudflare_integrations(self):
        model_type = SourceModelType.DATADOG_LIVE_INTEGRATION_CLOUDFLARE
        model_data = {}
        try:
            response = self.__dd_api_processor.fetch_cloudflare_integrations()
            if not response or 'data' not in response:
                return
            data = response['data']
            for ca in data:
                c_id = str(ca['id'])
                model_data[c_id] = ca
        except Exception as e:
            logger.error(f'Error extracting active cloudflare integrations: {e}')
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)

    @log_function_call
    def extract_active_confluent_integrations(self):
        model_type = SourceModelType.DATADOG_LIVE_INTEGRATION_CONFLUENT
        model_data = {}
        try:
            response = self.__dd_api_processor.fetch_confluent_integrations()
            if not response or 'data' not in response:
                return
            data = response['data']
            for ca in data:
                c_id = str(ca['id'])
                model_data[c_id] = ca
        except Exception as e:
            logger.error(f'Error extracting active confluent integrations: {e}')
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)

    @log_function_call
    def extract_active_fastly_integrations(self):
        model_type = SourceModelType.DATADOG_LIVE_INTEGRATION_FASTLY
        model_data = {}
        try:
            response = self.__dd_api_processor.fetch_fastly_integrations()
            if not response or 'data' not in response:
                return
            data = response['data']
            for fa in data:
                f_id = str(fa['id'])
                model_data[f_id] = fa
        except Exception as e:
            logger.error(f'Error extracting active fastly integrations: {e}')
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)

    @log_function_call
    def extract_active_gcp_integrations(self):
        model_type = SourceModelType.DATADOG_LIVE_INTEGRATION_GCP
        model_data = {}
        try:
            response = self.__dd_api_processor.fetch_gcp_integrations()
            if not response or 'data' not in response:
                return
            data = response['data']
            for gcpa in data:
                gcp_id = str(gcpa['id'])
                model_data[gcp_id] = gcpa
        except Exception as e:
            logger.error(f'Error extracting active gcp integrations: {e}')
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)

    @log_function_call
    def extract_metrics(self):
        model_type = SourceModelType.DATADOG_METRIC
        model_data = self._collect_metrics_data()
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    def _collect_metrics_data(self):
        """Collect metrics data - used by both extract_metrics and get_metrics_data."""
        model_data = {}
        try:
            all_metrics = self.__dd_api_processor.fetch_metrics().get('data', [])
        except Exception as e:
            logger.error(f'Error fetching metrics: {e}')
            all_metrics = []
        if not all_metrics:
            return model_data
        for mt in all_metrics:
            try:
                tags = self.__dd_api_processor.fetch_metric_tags(mt['id']).get('data', {}).get('attributes', {}).get(
                    'tags', [])
            except Exception as e:
                logger.error(f'Error fetching metric tags for metric: {mt["id"]} - {e}')
                tags = []
            family = mt['id'].split('.')[0]
            model_data[mt['id']] = {**mt, 'tags': tags, 'family': family}
        return model_data

    def get_metrics_data(self):
        """Get metrics data directly without storing to database."""
        return self._collect_metrics_data()
