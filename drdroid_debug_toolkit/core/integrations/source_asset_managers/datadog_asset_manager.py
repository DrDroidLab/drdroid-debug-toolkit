# -*- coding: utf-8 -*-
"""Datadog Asset Manager for processing raw metadata extractor data."""

from datetime import timezone
from typing import Dict, Any, List

from google.protobuf.struct_pb2 import Struct
from google.protobuf.wrappers_pb2 import UInt64Value, StringValue

from core.protos.base_pb2 import Source, SourceModelType
from core.protos.assets.asset_pb2 import AccountConnectorAssets, AccountConnectorAssetsModelFilters, ConnectorModelTypeOptions
from core.protos.assets.datadog_asset_pb2 import (
    DatadogServiceAssetOptions, DatadogServiceAssetModel, DatadogAssetModel, 
    DatadogAssets, DatadogDashboardModel, DatadogDashboardAssetOptions
)
from core.protos.connectors.connector_pb2 import Connector as ConnectorProto
from core.utils.proto_utils import dict_to_proto
from .asset_manager import SourceAssetManager

import logging

logger = logging.getLogger(__name__)


class DatadogAssetManager(SourceAssetManager):
    """Asset manager for Datadog that processes raw metadata extractor data."""

    def __init__(self):
        self.source = Source.DATADOG
        self.asset_type_callable_map = {
            SourceModelType.DATADOG_SERVICE: {
                'vector_encoding_string': self.get_dd_service_vector_encoding_string,
                'options': self.get_dd_service_options,
                'values': self.get_dd_service_values,
            },
            SourceModelType.DATADOG_DASHBOARD: {
                'values': self.get_dd_dashboard_values,
            }
        }

    @staticmethod
    def get_dd_service_vector_encoding_string(service, environment):
        return f'{service} in {environment}'

    def get_dd_service_options(self, raw_data: Dict[str, Any]):
        """Get Datadog service options from raw metadata extractor data."""
        all_service_metric_family_map = {}
        asset_model_options = []
        
        for service_name, service_data in raw_data.items():
            metrics = service_data.get('metrics', [])
            families = []
            for metric in metrics:
                family = metric.get('family')
                if family:
                    families.append(family)
            
            if families:
                all_service_metric_family_map[service_name] = families
                asset_model_options.append(
                    DatadogServiceAssetOptions.DatadogServiceAssetOption(
                        name=StringValue(value=service_name),
                        metric_families=list(set(families))
                    )
                )
        
        options = DatadogServiceAssetOptions(services=asset_model_options)
        return ConnectorModelTypeOptions(
            model_type=SourceModelType.DATADOG_SERVICE,
            datadog_service_model_options=options
        )

    def get_dd_service_values(self, connector: ConnectorProto, 
                             filters: AccountConnectorAssetsModelFilters,
                             model_type: SourceModelType,
                             raw_data: Dict[str, Any]) -> AccountConnectorAssets:
        """Get Datadog service values from raw metadata extractor data."""
        which_one_of = filters.WhichOneOf('filters')
        if which_one_of and which_one_of != 'datadog_service_model_filters':
            raise ValueError(f"Invalid filter: {which_one_of}")

        options: DatadogServiceAssetOptions = filters.datadog_service_model_filters
        filter_service_options = options.services
        filter_services = []
        filter_service_metric_family_map = {}
        
        for option in filter_service_options:
            filter_services.append(option.name.value)
            filter_service_metric_family_map[option.name.value] = option.metric_families
        
        # Filter raw data based on requested services
        filtered_data = {}
        if filter_services:
            for service_name in filter_services:
                if service_name in raw_data:
                    filtered_data[service_name] = raw_data[service_name]
        else:
            filtered_data = raw_data

        dd_asset_protos = []
        for service_name, service_data in filtered_data.items():
            metrics = service_data.get('metrics', [])
            if not metrics:
                continue
                
            metric_protos: List[DatadogServiceAssetModel.Metric] = []
            metric_env_list = []
            filter_metric_families = filter_service_metric_family_map.get(service_name, [])
            
            for metric in metrics:
                family = metric.get('family')
                if filter_metric_families and family not in filter_metric_families:
                    continue
                    
                tags = metric.get('tags', [])
                for tag in tags:
                    if tag.startswith('env:'):
                        env = tag.split(':')[1]
                        if env not in metric_env_list:
                            metric_env_list.append(env)
                            
                query = metric.get('id')
                tags = metric.get('tags', [])
                metric_protos.append(
                    DatadogServiceAssetModel.Metric(
                        metric=StringValue(value=query),
                        metric_family=StringValue(value=family),
                        tags=[StringValue(value=tag) for tag in tags]
                    )
                )
            
            # Get environments from service data
            env_list = list(service_data.keys())
            if 'metrics' in env_list:
                env_list.remove('metrics')
            for env in metric_env_list:
                if env not in env_list:
                    env_list.append(env)
                    
            dd_asset_protos.append(DatadogAssetModel(
                id=UInt64Value(value=0),  # No database ID in real-time mode
                connector_type=connector.type,
                type=model_type,
                last_updated=int(timezone.utc.timestamp()),
                datadog_service=DatadogServiceAssetModel(
                    service_name=StringValue(value=service_name),
                    environments=env_list,
                    metrics=metric_protos
                )
            ))
            
        return AccountConnectorAssets(datadog=DatadogAssets(assets=dd_asset_protos))

    def get_dd_dashboard_values(self, connector: ConnectorProto, 
                               filters: AccountConnectorAssetsModelFilters,
                               model_type: SourceModelType,
                               raw_data: Dict[str, Any]) -> AccountConnectorAssets:
        """Get Datadog dashboard values from raw metadata extractor data."""
        which_one_of = filters.WhichOneOf('filters')
        if which_one_of and which_one_of != 'datadog_dashboard_model_filters':
            raise ValueError(f"Invalid filter: {which_one_of}")

        options: DatadogDashboardAssetOptions = filters.datadog_dashboard_model_filters
        filter_dashboard_options = options.dashboards
        filter_dashboards = []
        
        for option in filter_dashboard_options:
            filter_dashboards.append(option.name.value)
            
        # Filter raw data based on requested dashboards
        filtered_data = {}
        if filter_dashboards:
            for dashboard_id, dashboard_data in raw_data.items():
                dashboard_title = dashboard_data.get('title', '')
                if dashboard_title in filter_dashboards:
                    filtered_data[dashboard_id] = dashboard_data
        else:
            filtered_data = raw_data

        dd_asset_protos = []
        for dashboard_id, dashboard_data in filtered_data.items():
            dashboard_url = dashboard_data.get('url', "")
            dashboard_title = dashboard_data.get('title', "")
            dashboard_description = dashboard_data.get('description', "")
            template_variables = dashboard_data.get("template_variables", [])
            widgets = dashboard_data.get('widgets', [])
            
            panels: List[DatadogDashboardModel.Panel] = []
            for w in widgets:
                panel_id = w.get('id', "")
                definition = w.get('definition', {})
                if not definition:
                    continue
                    
                widget_type = definition.get('type', "")
                if not widget_type:
                    continue

                inner_widgets = [w]
                widget_protos: List[DatadogDashboardModel.Panel.Widget] = []
                
                if widget_type == 'group':
                    inner_widgets = definition.get('widgets', [])
                    
                for inner_w in inner_widgets:
                    inner_definition = inner_w.get('definition', {})
                    widget_id = inner_w.get('id', panel_id)
                    if not inner_definition:
                        continue
                        
                    inner_widget_type = inner_definition.get('type', "")
                    if not inner_widget_type or inner_widget_type == 'group':
                        continue
                        
                    title = inner_definition.get('title', "")
                    request = inner_definition.get('requests', [{}])
                    
                    if isinstance(request, dict):
                        if 'fill' in request:
                            query = {
                                'data_source': request.get('data_source', "metrics"), 
                                'name': "query1",
                                'query': request.get('fill', {}).get('q', '')
                            }
                            formula = {'formula': 'query1'}
                            response_type = request.get('response_format', "timeseries")
                            query_protos = [dict_to_proto(query, Struct)]
                            formula_protos = [dict_to_proto(formula, Struct)]
                        elif 'table' in request:
                            table_request = request.get("table", {})
                            queries = table_request.get("queries", [])
                            formulas = table_request.get("formulas", [])
                            if 'response_format' in table_request:
                                response_type = table_request.get('response_format', "scalar")
                            else:
                                response_type = request.get('response_format', "timeseries")
                            query_protos = [dict_to_proto(q, Struct) for q in queries]
                            formula_protos = [dict_to_proto(f, Struct) for f in formulas]
                        else:
                            queries = request.get('queries', [])
                            formulas = request.get('formulas', [])
                            response_type = request.get('response_format', "timeseries")
                            query_protos = [dict_to_proto(q, Struct) for q in queries]
                            formula_protos = [dict_to_proto(f, Struct) for f in formulas]
                            
                        widget_protos.append(DatadogDashboardModel.Panel.Widget(
                            title=StringValue(value=title),
                            response_type=StringValue(value=response_type),
                            queries=query_protos,
                            formulas=formula_protos,
                            id=StringValue(value=str(widget_id)),
                            widget_type=StringValue(value=inner_widget_type)
                        ))
                    elif isinstance(request, list):
                        for r in request:
                            if 'query' in r:
                                queries = [{
                                    'data_source': r.get('query', {}).get('data_source', "metrics"),
                                    'name': r.get('query', {}).get('name', "query1"),
                                    'query': r.get('query', {}).get('query_string', '')
                                }]
                            else:
                                queries = r.get('queries', [])
                            formulas = r.get('formulas', [])
                            response_type = r.get('response_format', "timeseries")
                            query_protos = [dict_to_proto(q, Struct) for q in queries]
                            formula_protos = [dict_to_proto(f, Struct) for f in formulas]
                            widget_protos.append(DatadogDashboardModel.Panel.Widget(
                                title=StringValue(value=title),
                                response_type=StringValue(value=response_type),
                                queries=query_protos,
                                formulas=formula_protos,
                                id=StringValue(value=str(widget_id)),
                                widget_type=StringValue(value=inner_widget_type)
                            ))
                            
                panels.append(DatadogDashboardModel.Panel(
                    title=StringValue(value=w.get('title', "")),
                    widgets=widget_protos,
                    id=StringValue(value=str(panel_id))
                ))

            if template_variables:
                template_variable_protos = [dict_to_proto(dict(tv), Struct) for tv in template_variables]
            else:
                template_variable_protos = []

            dd_asset_protos.append(DatadogAssetModel(
                id=UInt64Value(value=0),  # No database ID in real-time mode
                connector_type=connector.type,
                type=model_type,
                last_updated=int(timezone.utc.timestamp()),
                datadog_dashboard=DatadogDashboardModel(
                    id=StringValue(value=dashboard_id),
                    url=StringValue(value=dashboard_url),
                    title=StringValue(value=dashboard_title),
                    description=StringValue(value=dashboard_description),
                    panels=panels,
                    template_variables=template_variable_protos
                )
            ))
            
        return AccountConnectorAssets(datadog=DatadogAssets(assets=dd_asset_protos))

    def get_asset_options(self, model_type: SourceModelType, raw_data: Dict[str, Any]):
        """Get asset options for a specific model type."""
        if model_type == SourceModelType.DATADOG_SERVICE:
            return self.get_dd_service_options(raw_data)
        else:
            raise ValueError(f"Unsupported model type: {model_type}")

    def get_asset_values(self, connector: ConnectorProto, 
                        filters: AccountConnectorAssetsModelFilters,
                        model_type: SourceModelType,
                        raw_data: Dict[str, Any]) -> AccountConnectorAssets:
        """Get asset values for a specific model type."""
        if model_type == SourceModelType.DATADOG_SERVICE:
            return self.get_dd_service_values(connector, filters, model_type, raw_data)
        elif model_type == SourceModelType.DATADOG_DASHBOARD:
            return self.get_dd_dashboard_values(connector, filters, model_type, raw_data)
        else:
            raise ValueError(f"Unsupported model type: {model_type}") 