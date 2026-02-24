import re
import logging

from core.integrations.source_metadata_extractor import SourceMetadataExtractor
from core.integrations.source_api_processors.grafana_api_processor import GrafanaApiProcessor
from core.protos.base_pb2 import Source, SourceModelType
from core.utils.logging_utils import log_function_call

logger = logging.getLogger(__name__)


def promql_get_metric_name(promql):
    metric_name_end = promql.index('{')
    i = metric_name_end - 1
    while i >= 0:
        if promql[i] == ' ' or promql[i] == '(' or i == 0:
            metric_name_start = i + 1
            if i == 0:
                metric_name_start = 0
            metric_name = promql[metric_name_start:metric_name_end]
            return metric_name
        i -= 1
    return None


def promql_get_metric_optional_label_variable_pairs(promql):
    expr_label_str = promql.split('{')[1].split('}')[0]
    expr_label_str = expr_label_str.replace(' ', '')
    pattern = r'(\w+)\s*([=~]+)\s*"?(\$[\w]+)"?'
    matches = re.findall(pattern, expr_label_str)
    label_value_pairs = {label: value for label, op, value in matches}
    return label_value_pairs


class GrafanaSourceMetadataExtractor(SourceMetadataExtractor):

    def __init__(self, request_id, connector_name, grafana_host, grafana_api_key, ssl_verify="true"):
        self.__grafana_api_processor = GrafanaApiProcessor(grafana_host, grafana_api_key, ssl_verify)
        super().__init__(request_id, connector_name, Source.GRAFANA)

    @log_function_call
    def extract_data_source(self):
        model_type = SourceModelType.GRAFANA_DATASOURCE
        try:
            datasources = self.__grafana_api_processor.fetch_data_sources()
        except Exception as e:
            logger.error(f'Error fetching grafana data sources: {e}')
            return
        if not datasources:
            return
        model_data = {}
        for ds in datasources:
            datasource_id = ds['uid']
            model_data[datasource_id] = ds

        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_prometheus_data_source(self):
        model_type = SourceModelType.GRAFANA_PROMETHEUS_DATASOURCE
        try:
            all_data_sources = self.__grafana_api_processor.fetch_data_sources()
            if not all_data_sources:
                return
            all_promql_data_sources = [ds for ds in all_data_sources if
                                       ds['type'] == 'prometheus' or ds['type'] == 'influxdb']
        except Exception as e:
            logger.error(f'Error fetching grafana prometheus data sources: {e}')
            return
        if not all_promql_data_sources:
            return
        model_data = {}
        for ds in all_promql_data_sources:
            datasource_id = ds['uid']
            model_data[datasource_id] = ds
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_loki_data_source(self):
        model_type = SourceModelType.GRAFANA_LOKI_DATASOURCE
        try:
            all_data_sources = self.__grafana_api_processor.fetch_data_sources()
            if not all_data_sources:
                return
            all_loki_data_sources = [ds for ds in all_data_sources if ds['type'] == 'loki']
        except Exception as e:
            logger.error(f'Error fetching grafana loki data sources: {e}')
            return
        if not all_loki_data_sources:
            return
        model_data = {}
        for ds in all_loki_data_sources:
            datasource_id = ds['uid']
            # Fetch available labels for this Loki datasource
            try:
                loki_labels = self.__grafana_api_processor.fetch_loki_labels(datasource_id)
                ds['available_labels'] = loki_labels
            except Exception as e:
                logger.error(f'Error fetching loki labels for datasource {datasource_id}: {e}')
                ds['available_labels'] = []
            
            model_data[datasource_id] = ds
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_dashboards(self, save_to_db=True):
        model_type = SourceModelType.GRAFANA_DASHBOARD
        try:
            all_dashboards = self.__grafana_api_processor.fetch_dashboards()
        except Exception as e:
            logger.error(f'Error fetching grafana dashboards: {e}')
            return
        if not all_dashboards:
            return
        all_db_dashboard_uids = []
        for db in all_dashboards:
            if db['type'] == 'dash-db':
                all_db_dashboard_uids.append(db['uid'])

        model_data = {}
        for uid in all_db_dashboard_uids:
            try:
                dashboard_details = self.__grafana_api_processor.fetch_dashboard_details(uid)
            except Exception as e:
                logger.error(f'Error fetching grafana dashboard details: {e}')
                continue
            if not dashboard_details:
                continue
            model_data[uid] = dashboard_details
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    # @log_function_call
    # def extract_dashboard_target_metric_promql(self, save_to_db=True):
    #     model_type = SourceModelType.GRAFANA_TARGET_METRIC_PROMQL
    #     try:
    #         all_data_sources = self.__grafana_api_processor.fetch_data_sources()
    #     except Exception as e:
    #         capture_exception(Exception(f'Error fetching grafana data sources for target metric promql: {e}'))
    #         return
    #     if not all_data_sources:
    #         return
    #     promql_datasources = [ds for ds in all_data_sources if ds['type'] == 'prometheus']
    #     try:
    #         all_dashboards = self.__grafana_api_processor.fetch_dashboards()
    #     except Exception as e:
    #         capture_exception(Exception(f'Error fetching grafana dashboard for target metric promql: {e}'))
    #         return
    #     if not all_dashboards:
    #         return
    #     all_db_dashboard_uids = []
    #     for db in all_dashboards:
    #         if db['type'] == 'dash-db':
    #             all_db_dashboard_uids.append(db['uid'])

    #     model_data = {}
    #     for uid in all_db_dashboard_uids:
    #         try:
    #             dashboard_details = self.__grafana_api_processor.fetch_dashboard_details(uid)
    #         except Exception as e:
    #             capture_exception(Exception(f'Error fetching grafana dashboard details for target metric promql: {e}'))
    #             continue
    #         if not dashboard_details:
    #             continue
    #         try:
    #             if 'dashboard' in dashboard_details and 'panels' in dashboard_details['dashboard']:
    #                 dashboard_title = ''
    #                 if 'title' in dashboard_details['dashboard']:
    #                     dashboard_title = dashboard_details['dashboard']['title']
    #                 panels = dashboard_details['dashboard']['panels']
    #                 for p in panels:
    #                     panel_title = ''
    #                     if 'title' in p:
    #                         panel_title = p['title']
    #                     if 'targets' in p:
    #                         targets = p['targets']
    #                         for t in targets:
    #                             if 'expr' in t:
    #                                 # datasource = p['datasource']
    #                                 # datasource_uid = None
    #                                 # if isinstance(datasource, dict):
    #                                 #     datasource_uid = datasource['uid']
    #                                 # else:
    #                                 #     for promql_datasource in promql_datasources:
    #                                 #         if promql_datasource['typeName'] == datasource:
    #                                 #             datasource_uid = promql_datasource['uid']
    #                                 #             break
    #                                 # if not datasource_uid:
    #                                 #     datasource_uid = promql_datasources[0]['uid']
    #                                 # TODO(MG): Check how to remove data source hard coding
    #                                 datasource_uid = promql_datasources[0]['uid']

    #                                 model_uid = f"{uid}#{p['id']}#{t.get('refId', 'A')}"
    #                                 expr = t['expr'].replace('$__rate_interval', '5m')
    #                                 expr = expr.replace('$__interval', '5m')
    #                                 model_data[model_uid] = {'expr': expr, 'dashboard_id': uid, 'panel_id': p['id'],
    #                                                          'panel_title': panel_title,
    #                                                          'dashboard_title': dashboard_title,
    #                                                          'target_metric_ref_id': t.get('refId', 'A'),
    #                                                          'datasource_uid': datasource_uid}
    #                                 if '$' in expr:
    #                                     metric_name = promql_get_metric_name(expr)
    #                                     if metric_name:
    #                                         model_data[model_uid]['metric_name'] = metric_name
    #                                         optional_label_variable_pairs = promql_get_metric_optional_label_variable_pairs(
    #                                             expr)
    #                                         if optional_label_variable_pairs:
    #                                             model_data[model_uid][
    #                                                 'optional_label_variable_pairs'] = optional_label_variable_pairs
    #                                             retry_attempts = 0
    #                                             try:
    #                                                 response = self.__grafana_api_processor.fetch_promql_metric_labels(
    #                                                     datasource_uid, metric_name)
    #                                             except Exception as e:
    #                                                 capture_exception(Exception(
    #                                                     f'Error fetching promql metric labels for target metric promql: {e}'))
    #                                                 response = None
    #                                             while not response and retry_attempts < 3:
    #                                                 try:
    #                                                     response = self.__grafana_api_processor.fetch_promql_metric_labels(
    #                                                         datasource_uid, metric_name)
    #                                                 except Exception as e:
    #                                                     capture_exception(Exception(
    #                                                         f'Error fetching promql metric labels for target metric promql: {e}'))
    #                                                     response = None
    #                                                 time.sleep(5)
    #                                                 retry_attempts += 1
    #                                             if response and 'data' in response:
    #                                                 promql_labels = response['data']
    #                                                 label_value_options = {}
    #                                                 for lb in promql_labels:
    #                                                     if lb in optional_label_variable_pairs:
    #                                                         optional_variable_name = optional_label_variable_pairs[lb]
    #                                                         retry_attempts = 0
    #                                                         try:
    #                                                             response = self.__grafana_api_processor.fetch_promql_metric_label_values(
    #                                                                 datasource_uid, metric_name, lb)
    #                                                         except Exception as e:
    #                                                             capture_exception(Exception(
    #                                                                 f'Error fetching promql metric label values for target metric promql: {e}'))
    #                                                             response = None
    #                                                         while not response and retry_attempts < 3:
    #                                                             try:
    #                                                                 response = self.__grafana_api_processor.fetch_promql_metric_label_values(
    #                                                                     datasource_uid, metric_name, lb)
    #                                                             except Exception as e:
    #                                                                 capture_exception(Exception(
    #                                                                     f'Error fetching promql metric label values for target metric promql: {e}'))
    #                                                                 response = None
    #                                                             time.sleep(5)
    #                                                             retry_attempts += 1
    #                                                         if response and 'data' in response:
    #                                                             label_values = response['data']
    #                                                             label_value_options[
    #                                                                 optional_variable_name] = label_values
    #                                                 if label_value_options:
    #                                                     model_data[model_uid][
    #                                                         'optional_label_options'] = label_value_options
    #                                 if save_to_db:
    #                                     self.create_or_update_model_metadata(model_type, model_uid,
    #                                                                          model_data[model_uid])
    #             if 'dashboard' in dashboard_details and 'rows' in dashboard_details['dashboard']:
    #                 rows = dashboard_details['dashboard']['rows']
    #                 for r in rows:
    #                     if 'panels' in r:
    #                         panels = r['panels']
    #                         dashboard_title = ''
    #                         if 'title' in dashboard_details['dashboard']:
    #                             dashboard_title = dashboard_details['dashboard']['title']
    #                         for p in panels:
    #                             panel_title = ''
    #                             if 'title' in p:
    #                                 panel_title = p['title']
    #                             if 'targets' in p:
    #                                 targets = p['targets']
    #                                 for t in targets:
    #                                     if 'expr' in t:
    #                                         # datasource = p['datasource']
    #                                         # datasource_uid = None
    #                                         # if isinstance(datasource, dict):
    #                                         #     datasource_uid = datasource['uid']
    #                                         # else:
    #                                         #     for promql_datasource in promql_datasources:
    #                                         #         if promql_datasource['typeName'] == datasource:
    #                                         #             datasource_uid = promql_datasource['uid']
    #                                         #             break
    #                                         # if not datasource_uid:
    #                                         #     datasource_uid = promql_datasources[0]['uid']
    #                                         # TODO(MG): Check how to remove data source hard coding
    #                                         datasource_uid = promql_datasources[0]['uid']

    #                                         model_uid = f"{uid}#{p['id']}#{t.get('refId', 'A')}"
    #                                         expr = t['expr'].replace('$__rate_interval', '5m')
    #                                         expr = expr.replace('$__interval', '5m')
    #                                         model_data[model_uid] = {'expr': expr, 'dashboard_id': uid,
    #                                                                  'panel_id': p['id'],
    #                                                                  'panel_title': panel_title,
    #                                                                  'dashboard_title': dashboard_title,
    #                                                                  'target_metric_ref_id': t.get('refId', 'A'),
    #                                                                  'datasource_uid': datasource_uid}
    #                                         if '$' in expr:
    #                                             metric_name = promql_get_metric_name(expr)
    #                                             if metric_name:
    #                                                 model_data[model_uid]['metric_name'] = metric_name
    #                                                 optional_label_variable_pairs = promql_get_metric_optional_label_variable_pairs(
    #                                                     expr)
    #                                                 if optional_label_variable_pairs:
    #                                                     model_data[model_uid][
    #                                                         'optional_label_variable_pairs'] = optional_label_variable_pairs
    #                                                     retry_attempts = 0
    #                                                     try:
    #                                                         response = self.__grafana_api_processor.fetch_promql_metric_labels(
    #                                                             datasource_uid, metric_name)
    #                                                     except Exception as e:
    #                                                         capture_exception(Exception(
    #                                                             f'Error fetching promql metric labels for target metric promql: {e}'))
    #                                                         response = None
    #                                                     while not response and retry_attempts < 3:
    #                                                         try:
    #                                                             response = self.__grafana_api_processor.fetch_promql_metric_labels(
    #                                                                 datasource_uid, metric_name)
    #                                                         except Exception as e:
    #                                                             capture_exception(Exception(
    #                                                                 f'Error fetching promql metric labels for target metric promql: {e}'))
    #                                                             response = None
    #                                                         time.sleep(5)
    #                                                         retry_attempts += 1
    #                                                     if response and 'data' in response:
    #                                                         promql_labels = response['data']
    #                                                         label_value_options = {}
    #                                                         for lb in promql_labels:
    #                                                             if lb in optional_label_variable_pairs:
    #                                                                 optional_variable_name = \
    #                                                                     optional_label_variable_pairs[lb]
    #                                                                 retry_attempts = 0
    #                                                                 try:
    #                                                                     response = self.__grafana_api_processor.fetch_promql_metric_label_values(
    #                                                                         datasource_uid, metric_name, lb)
    #                                                                 except Exception as e:
    #                                                                     capture_exception(Exception(
    #                                                                         f'Error fetching promql metric label values for target metric promql: {e}'))
    #                                                                     response = None
    #                                                                 while not response and retry_attempts < 3:
    #                                                                     try:
    #                                                                         response = self.__grafana_api_processor.fetch_promql_metric_label_values(
    #                                                                             datasource_uid, metric_name, lb)
    #                                                                     except Exception as e:
    #                                                                         capture_exception(Exception(
    #                                                                             f'Error fetching promql metric label values for target metric promql: {e}'))
    #                                                                         response = None
    #                                                                     time.sleep(5)
    #                                                                     retry_attempts += 1
    #                                                                 if response and 'data' in response:
    #                                                                     label_values = response['data']
    #                                                                     label_value_options[
    #                                                                         optional_variable_name] = label_values
    #                                                         if label_value_options:
    #                                                             model_data[model_uid][
    #                                                                 'optional_label_options'] = label_value_options
    #                                         if save_to_db:
    #                                             self.create_or_update_model_metadata(model_type, model_uid,
    #                                                                                  model_data[model_uid])
    #             if 'dashboard' in dashboard_details and 'panels' in dashboard_details['dashboard']:
    #                 panels = dashboard_details['dashboard']['panels']
    #                 for panel in panels:
    #                     if 'panels' in panel:
    #                         panels = panel['panels']
    #                         dashboard_title = ''
    #                         if 'title' in dashboard_details['dashboard']:
    #                             dashboard_title = dashboard_details['dashboard']['title']
    #                         for p in panels:
    #                             panel_title = ''
    #                             if 'title' in p:
    #                                 panel_title = p['title']
    #                             if 'targets' in p:
    #                                 targets = p['targets']
    #                                 for t in targets:
    #                                     if 'expr' in t:
    #                                         # datasource = p['datasource']
    #                                         # datasource_uid = None
    #                                         # if isinstance(datasource, dict):
    #                                         #     datasource_uid = datasource['uid']
    #                                         # else:
    #                                         #     for promql_datasource in promql_datasources:
    #                                         #         if promql_datasource['typeName'] == datasource:
    #                                         #             datasource_uid = promql_datasource['uid']
    #                                         #             break
    #                                         # if not datasource_uid:
    #                                         #     datasource_uid = promql_datasources[0]['uid']
    #                                         # TODO(MG): Check how to remove data source hard coding
    #                                         datasource_uid = promql_datasources[0]['uid']

    #                                         model_uid = f"{uid}#{p['id']}#{t.get('refId', 'A')}"
    #                                         expr = t['expr'].replace('$__rate_interval', '5m')
    #                                         expr = expr.replace('$__interval', '5m')
    #                                         model_data[model_uid] = {'expr': expr, 'dashboard_id': uid,
    #                                                                  'panel_id': p['id'],
    #                                                                  'panel_title': panel_title,
    #                                                                  'dashboard_title': dashboard_title,
    #                                                                  'target_metric_ref_id': t.get('refId', 'A'),
    #                                                                  'datasource_uid': datasource_uid}
    #                                         if '$' in expr:
    #                                             metric_name = promql_get_metric_name(expr)
    #                                             if metric_name:
    #                                                 model_data[model_uid]['metric_name'] = metric_name
    #                                                 optional_label_variable_pairs = promql_get_metric_optional_label_variable_pairs(
    #                                                     expr)
    #                                                 if optional_label_variable_pairs:
    #                                                     model_data[model_uid][
    #                                                         'optional_label_variable_pairs'] = optional_label_variable_pairs
    #                                                     retry_attempts = 0
    #                                                     try:
    #                                                         response = self.__grafana_api_processor.fetch_promql_metric_labels(
    #                                                             datasource_uid, metric_name)
    #                                                     except Exception as e:
    #                                                         capture_exception(Exception(
    #                                                             f'Error fetching promql metric labels for target metric promql: {e}'))
    #                                                         response = None
    #                                                     while not response and retry_attempts < 3:
    #                                                         try:
    #                                                             response = self.__grafana_api_processor.fetch_promql_metric_labels(
    #                                                                 datasource_uid, metric_name)
    #                                                         except Exception as e:
    #                                                             capture_exception(Exception(
    #                                                                 f'Error fetching promql metric labels for target metric promql: {e}'))
    #                                                             response = None
    #                                                         time.sleep(5)
    #                                                         retry_attempts += 1
    #                                                     if response and 'data' in response:
    #                                                         promql_labels = response['data']
    #                                                         label_value_options = {}
    #                                                         for lb in promql_labels:
    #                                                             if lb in optional_label_variable_pairs:
    #                                                                 optional_variable_name = \
    #                                                                     optional_label_variable_pairs[lb]
    #                                                                 retry_attempts = 0
    #                                                                 try:
    #                                                                     response = self.__grafana_api_processor.fetch_promql_metric_label_values(
    #                                                                         datasource_uid, metric_name, lb)
    #                                                                 except Exception as e:
    #                                                                     capture_exception(Exception(
    #                                                                         f"Error fetching promql metric label values for target metric promql: {e}"))
    #                                                                     response = None
    #                                                                 while not response and retry_attempts < 3:
    #                                                                     try:
    #                                                                         response = self.__grafana_api_processor.fetch_promql_metric_label_values(
    #                                                                             datasource_uid, metric_name, lb)
    #                                                                     except Exception as e:
    #                                                                         capture_exception(Exception(
    #                                                                             f"Error fetching promql metric label values for target metric promql: {e}"))
    #                                                                         response = None
    #                                                                     time.sleep(5)
    #                                                                     retry_attempts += 1
    #                                                                 if response and 'data' in response:
    #                                                                     label_values = response['data']
    #                                                                     label_value_options[
    #                                                                         optional_variable_name] = label_values
    #                                                         if label_value_options:
    #                                                             model_data[model_uid][
    #                                                                 'optional_label_options'] = label_value_options
    #                                         if save_to_db:
    #                                             self.create_or_update_model_metadata(model_type, model_uid,
    #                                                                                  model_data[model_uid])
    #         except Exception as e:
    #             capture_exception(Exception(f'Error extracting grafana target metric promql: {e}'))
    #     return model_data

    @log_function_call
    def extract_tempo_data_source(self):
        model_type = SourceModelType.GRAFANA_TEMPO_DATASOURCE
        try:
            all_data_sources = self.__grafana_api_processor.fetch_data_sources()
            if not all_data_sources:
                return
            all_tempo_data_sources = [ds for ds in all_data_sources if ds['type'] == 'tempo']
        except Exception as e:
            logger.error(f'Error fetching grafana tempo data sources: {e}')
            return
        if not all_tempo_data_sources:
            return
        model_data = {}
        for ds in all_tempo_data_sources:
            datasource_id = ds['uid']
            model_data[datasource_id] = ds
        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_tempo_services(self):
        """Discover services from all Tempo datasources."""
        model_type = SourceModelType.GRAFANA_TEMPO_SERVICE
        tempo_datasources = self.extract_tempo_data_source()
        if not tempo_datasources:
            return
        model_data = {}
        for ds_uid, ds in tempo_datasources.items():
            try:
                services_response = self.__grafana_api_processor.tempo_get_services(ds_uid)
                service_list = services_response.get('tagValues', services_response) if isinstance(services_response, dict) else services_response
                if not isinstance(service_list, list):
                    service_list = []
                for svc in service_list:
                    svc_name = svc.get('value', svc) if isinstance(svc, dict) else svc
                    model_uid = f"{ds_uid}::{svc_name}"
                    model_data[model_uid] = {
                        'datasource_uid': ds_uid,
                        'datasource_name': ds.get('name', ''),
                        'service_name': svc_name,
                        'type': svc.get('type', 'string') if isinstance(svc, dict) else 'string'
                    }
            except Exception as e:
                logger.error(f'Error fetching tempo services for {ds_uid}: {e}')
        if model_data:
            self.create_or_update_model_metadata(model_type, model_data)
        return model_data

    @log_function_call
    def extract_alert_rules(self):
        model_type = SourceModelType.GRAFANA_ALERT_RULE
        try:
            alert_rules = self.__grafana_api_processor.fetch_alert_rules()
        except Exception as e:
            logger.error(f"Error fetching Grafana alert rules: {e}")
            return

        if not alert_rules:
            return

        model_data = {}
        for alert_rule in alert_rules:
            alert_uid = alert_rule['uid']

            model_data[alert_uid] = {
                'alert_rule_id': alert_uid,
                'alert_rule_json': alert_rule
            }

        if len(model_data) > 0:
            self.create_or_update_model_metadata(model_type, model_data)

        return model_data
