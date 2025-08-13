# -*- coding: utf-8 -*-
"""NewRelic Asset Manager for processing raw metadata extractor data."""

from datetime import timezone
from typing import Dict, Any, List

from google.protobuf.wrappers_pb2 import UInt64Value, StringValue

from core.protos.base_pb2 import Source, SourceModelType
from core.protos.assets.asset_pb2 import AccountConnectorAssets, AccountConnectorAssetsModelFilters, ConnectorModelTypeOptions
from core.protos.assets.newrelic_asset_pb2 import (
    NewRelicApplicationEntityAssetOptions, NewRelicApplicationEntityAssetModel,
    NewRelicAssetModel, NewRelicAssets, NewRelicDashboardEntityAssetOptions,
    NewRelicDashboardEntityAssetModel, NewRelicDashboardEntityAssetModelV2,
    NewRelicDashboardEntityAssetOptionsV2
)
from core.protos.connectors.connector_pb2 import Connector as ConnectorProto
from .asset_manager import SourceAssetManager

import logging

logger = logging.getLogger(__name__)


class NewRelicAssetManager(SourceAssetManager):
    """Asset manager for NewRelic that processes raw metadata extractor data."""

    def __init__(self):
        self.source = Source.NEW_RELIC
        self.asset_type_callable_map = {
            SourceModelType.NEW_RELIC_ENTITY_APPLICATION: {
                'options': self.get_nr_entity_application_options,
                'values': self.get_nr_entity_application_values,
            },
            SourceModelType.NEW_RELIC_ENTITY_DASHBOARD: {
                'options': self.get_nr_entity_dashboard_options,
                'values': self.get_nr_entity_dashboard_values,
            },
            SourceModelType.NEW_RELIC_ENTITY_DASHBOARD_V2: {
                'options': self.get_nr_entity_dashboard_options,
                'values': self.get_nr_entity_dashboard_values_v2,
            },
        }

    def get_nr_entity_application_options(self, raw_data: Dict[str, Any]):
        """Get NewRelic application entity options from raw metadata extractor data."""
        all_application_entities = []
        for app_guid, app_data in raw_data.items():
            app_name = app_data.get('name', '')
            if app_name:
                all_application_entities.append(app_name)
                
        options = NewRelicApplicationEntityAssetOptions(application_names=all_application_entities)
        return ConnectorModelTypeOptions(
            model_type=SourceModelType.NEW_RELIC_ENTITY_APPLICATION,
            new_relic_entity_application_model_options=options
        )

    def get_nr_entity_application_values(self, connector: ConnectorProto, 
                                        filters: AccountConnectorAssetsModelFilters,
                                        model_type: SourceModelType,
                                        raw_data: Dict[str, Any]) -> AccountConnectorAssets:
        """Get NewRelic application entity values from raw metadata extractor data."""
        which_one_of = filters.WhichOneOf('filters')
        if which_one_of and which_one_of != 'new_relic_entity_application_model_filters':
            raise ValueError(f"Invalid filter: {which_one_of}")

        options: NewRelicApplicationEntityAssetOptions = filters.new_relic_entity_application_model_filters
        filter_applications = options.application_names
        
        # Filter raw data based on requested applications
        filtered_data = {}
        if filter_applications:
            for app_guid, app_data in raw_data.items():
                app_name = app_data.get('name', '')
                if app_name in filter_applications:
                    filtered_data[app_guid] = app_data
        else:
            filtered_data = raw_data

        nr_asset_protos = []
        for app_guid, app_data in filtered_data.items():
            asset_golden_metrics: List[NewRelicApplicationEntityAssetModel.GoldenMetric] = []
            golden_metrics = app_data.get('goldenMetrics', {}).get('metrics', [])
            for metric in golden_metrics:
                asset_golden_metrics.append(
                    NewRelicApplicationEntityAssetModel.GoldenMetric(
                        golden_metric_name=StringValue(value=metric.get('name')),
                        golden_metric_unit=StringValue(value=metric.get('unit')),
                        golden_metric_nrql_expression=StringValue(value=metric.get('query'))
                    )
                )

            # Add APM metrics
            asset_apm_metrics: List[NewRelicApplicationEntityAssetModel.APMDashboard] = []
            apm_metrics = app_data.get('apm_summary', [])
            for metric in apm_metrics:
                asset_apm_metrics.append(
                    NewRelicApplicationEntityAssetModel.APMDashboard(
                        metric_name=StringValue(value=metric.get('name')),
                        metric_unit=StringValue(value=metric.get('unit')),
                        metric_nrql_expression=StringValue(value=metric.get('query'))
                    )
                )

            nr_asset_protos.append(NewRelicAssetModel(
                id=UInt64Value(value=0),  # No database ID in real-time mode
                connector_type=connector.type,
                type=model_type,
                last_updated=int(timezone.utc.timestamp()),
                new_relic_entity_application=NewRelicApplicationEntityAssetModel(
                    application_entity_guid=StringValue(value=app_guid),
                    application_name=StringValue(value=app_data.get('name')),
                    golden_metrics=asset_golden_metrics,
                    apm_metrics=asset_apm_metrics
                )
            ))

        return AccountConnectorAssets(new_relic=NewRelicAssets(assets=nr_asset_protos))

    def get_nr_entity_dashboard_options(self, raw_data: Dict[str, Any]):
        """Get NewRelic dashboard entity options from raw metadata extractor data."""
        all_dashboards = {}
        for dashboard_guid, dashboard_data in raw_data.items():
            dashboard = all_dashboards.get(dashboard_guid, {})
            pages = dashboard.get('pages', [])
            asset_pages = dashboard_data.get('pages', [])
            for page in asset_pages:
                pages.append({
                    'page_guid': page.get('guid', ''), 
                    'page_name': page.get('name', '')
                })
            dashboard['pages'] = pages
            dashboard['dashboard_name'] = dashboard_data.get('name', '')
            all_dashboards[dashboard_guid] = dashboard

        dashboard_options: List[NewRelicDashboardEntityAssetOptions.DashboardOptions] = []
        for dashboard_id, dict_items in all_dashboards.items():
            page_options: List[NewRelicDashboardEntityAssetOptions.DashboardOptions.DashboardPageOptions] = []
            for page in dict_items['pages']:
                page_options.append(NewRelicDashboardEntityAssetOptions.DashboardOptions.DashboardPageOptions(
                    page_guid=StringValue(value=page['page_guid']),
                    page_name=StringValue(value=page['page_name'])
                ))
            dashboard_options.append(NewRelicDashboardEntityAssetOptions.DashboardOptions(
                dashboard_guid=StringValue(value=dashboard_id),
                dashboard_name=StringValue(value=dict_items['dashboard_name']),
                page_options=page_options
            ))
            
        return ConnectorModelTypeOptions(
            model_type=SourceModelType.NEW_RELIC_ENTITY_DASHBOARD,
            new_relic_entity_dashboard_model_options=NewRelicDashboardEntityAssetOptions(
                dashboards=dashboard_options
            )
        )

    def get_nr_entity_dashboard_values(self, connector: ConnectorProto, 
                                      filters: AccountConnectorAssetsModelFilters,
                                      model_type: SourceModelType,
                                      raw_data: Dict[str, Any]) -> AccountConnectorAssets:
        """Get NewRelic dashboard entity values from raw metadata extractor data."""
        which_one_of = filters.WhichOneOf('filters')
        if which_one_of and which_one_of != 'new_relic_entity_dashboard_model_filters':
            raise ValueError(f"Invalid filter: {which_one_of}")

        dashboard_page_filters = {}

        options: NewRelicDashboardEntityAssetOptions = filters.new_relic_entity_dashboard_model_filters
        filter_dashboards = options.dashboards
        
        # Filter raw data based on requested dashboards
        filtered_data = {}
        if filter_dashboards:
            all_dashboard_guids = [x.dashboard_guid.value for x in filter_dashboards]
            for dashboard_guid, dashboard_data in raw_data.items():
                if dashboard_guid in all_dashboard_guids:
                    filtered_data[dashboard_guid] = dashboard_data
                    
            # Prepare page filters
            for db in filter_dashboards:
                filter_page_options = db.page_options
                if filter_page_options:
                    dashboard_page_filters[db.dashboard_guid.value] = [x.page_guid.value for x in filter_page_options]
        else:
            filtered_data = raw_data

        nr_asset_protos = []
        for dashboard_guid, dashboard_data in filtered_data.items():
            asset_pages = dashboard_data.get('pages', [])
            pages: List[NewRelicDashboardEntityAssetModel.DashboardPage] = []
            
            for page in asset_pages:
                if dashboard_page_filters and dashboard_guid in dashboard_page_filters and page.get('guid') not in dashboard_page_filters.get(dashboard_guid, []):
                    continue
                    
                widgets: List[NewRelicDashboardEntityAssetModel.PageWidget] = []
                page_widgets = page.get('widgets', [])
                
                for widget in page_widgets:
                    all_queries, widget_type = None, None
                    configuration = widget.get('configuration', {})
                    if not configuration:
                        continue
                        
                    bar = configuration.get('bar', {})
                    if bar:
                        widget_type = 'bar'
                        all_queries = [x.get('query', '') for x in bar.get('nrqlQueries', [])]
                    pie = configuration.get('pie', {})
                    if pie:
                        widget_type = 'pie'
                        all_queries = [x.get('query', '') for x in pie.get('nrqlQueries', [])]
                    area = configuration.get('area', {})
                    if area:
                        widget_type = 'area'
                        all_queries = [x.get('query', '') for x in area.get('nrqlQueries', [])]
                    line = configuration.get('line', {})
                    if line:
                        widget_type = 'line'
                        all_queries = [x.get('query', '') for x in line.get('nrqlQueries', [])]
                    table = configuration.get('table', {})
                    if table:
                        widget_type = 'table'
                        all_queries = [x.get('query', '') for x in table.get('nrqlQueries', [])]
                    markdown = configuration.get('markdown', {})
                    if markdown:
                        widget_type = 'markdown'
                        all_queries = [x.get('query', '') for x in markdown.get('nrqlQueries', [])]
                    billboard = configuration.get('billboard', {})
                    if billboard:
                        widget_type = 'billboard'
                        all_queries = [x.get('query', '') for x in billboard.get('nrqlQueries', [])]
                        
                    if not all_queries or not widget_type:
                        continue
                        
                    for query in all_queries:
                        widgets.append(NewRelicDashboardEntityAssetModel.PageWidget(
                            widget_id=StringValue(value=widget.get('id')),
                            widget_title=StringValue(value=widget.get('title')),
                            widget_type=StringValue(value=widget_type),
                            widget_nrql_expression=StringValue(value=query)
                        ))
                        
                pages.append(NewRelicDashboardEntityAssetModel.DashboardPage(
                    page_guid=StringValue(value=page.get('guid')),
                    page_name=StringValue(value=page.get('name')),
                    widgets=widgets
                ))
                
            nr_asset_protos.append(NewRelicAssetModel(
                id=UInt64Value(value=0),  # No database ID in real-time mode
                connector_type=connector.type,
                type=model_type,
                last_updated=int(timezone.utc.timestamp()),
                new_relic_entity_dashboard=NewRelicDashboardEntityAssetModel(
                    dashboard_guid=StringValue(value=dashboard_guid),
                    dashboard_name=StringValue(value=dashboard_data.get('name')),
                    pages=pages
                )
            ))

        return AccountConnectorAssets(new_relic=NewRelicAssets(assets=nr_asset_protos))

    def get_nr_entity_dashboard_values_v2(self, connector: ConnectorProto, 
                                         filters: AccountConnectorAssetsModelFilters,
                                         model_type: SourceModelType,
                                         raw_data: Dict[str, Any]) -> AccountConnectorAssets:
        """Get NewRelic dashboard entity V2 values from raw metadata extractor data."""
        dashboard_page_filters = {}
        options_v1: NewRelicDashboardEntityAssetOptions = filters.new_relic_entity_dashboard_model_filters

        # Filter raw data based on requested dashboards
        filtered_data = {}
        if options_v1 and options_v1.dashboards:
            filter_dashboards_v1 = options_v1.dashboards
            all_dashboard_guids = [x.dashboard_guid.value for x in filter_dashboards_v1 if x.dashboard_guid.value]
            if all_dashboard_guids:
                for dashboard_guid, dashboard_data in raw_data.items():
                    if dashboard_guid in all_dashboard_guids:
                        filtered_data[dashboard_guid] = dashboard_data

            # Prepare page filters keyed by dashboard GUID
            for db in filter_dashboards_v1:
                if db.page_options:
                    page_guids_for_db = [p.page_guid.value for p in db.page_options if p.page_guid.value]
                    if page_guids_for_db:
                        dashboard_page_filters[db.dashboard_guid.value] = page_guids_for_db
        else:
            filtered_data = raw_data

        nr_asset_protos = []
        for dashboard_guid, dashboard_data in filtered_data.items():
            asset_pages = dashboard_data.get('pages', [])
            pages: List[NewRelicDashboardEntityAssetModelV2.DashboardPageV2] = []
            
            for page in asset_pages:
                page_guid = page.get('guid')

                # Apply page filters if they exist for this dashboard
                if dashboard_guid in dashboard_page_filters and page_guid not in dashboard_page_filters[dashboard_guid]:
                    continue

                widgets_v2: List[NewRelicDashboardEntityAssetModelV2.PageWidgetV2] = []
                page_widgets_data = page.get('widgets', [])
                
                for widget_data in page_widgets_data:
                    all_queries, widget_type = [], None
                    configuration = widget_data.get('configuration', {})
                    if not configuration:
                        continue

                    widget_configs = [
                        ('bar', configuration.get('bar', {})),
                        ('pie', configuration.get('pie', {})),
                        ('area', configuration.get('area', {})),
                        ('line', configuration.get('line', {})),
                        ('table', configuration.get('table', {})),
                        ('markdown', configuration.get('markdown', {})),
                        ('billboard', configuration.get('billboard', {}))
                    ]

                    for type_name, config_data in widget_configs:
                        if config_data:
                            widget_type = type_name
                            queries_in_config = [q.get('query', '') for q in config_data.get('nrqlQueries', []) if q.get('query')]
                            all_queries.extend(queries_in_config)
                            break

                    if not widget_type:
                        continue

                    widget_id_val = widget_data.get('id')
                    widget_title_val = widget_data.get('title')

                    if widget_id_val and widget_type and all_queries:
                        widgets_v2.append(NewRelicDashboardEntityAssetModelV2.PageWidgetV2(
                            widget_id=StringValue(value=widget_id_val),
                            widget_title=StringValue(value=widget_title_val),
                            widget_type=StringValue(value=widget_type),
                            widget_nrql_expressions=[StringValue(value=q) for q in all_queries]
                        ))

                if widgets_v2:
                    pages.append(NewRelicDashboardEntityAssetModelV2.DashboardPageV2(
                        page_guid=StringValue(value=page_guid),
                        page_name=StringValue(value=page.get('name')),
                        widgets=widgets_v2
                    ))

            # Only create asset proto if the dashboard contains pages after filtering
            if pages:
                nr_asset_protos.append(NewRelicAssetModel(
                    id=UInt64Value(value=0),  # No database ID in real-time mode
                    connector_type=connector.type,
                    type=SourceModelType.NEW_RELIC_ENTITY_DASHBOARD_V2,
                    last_updated=int(timezone.utc.timestamp()),
                    new_relic_entity_dashboard_v2=NewRelicDashboardEntityAssetModelV2(
                        dashboard_guid=StringValue(value=dashboard_guid),
                        dashboard_name=StringValue(value=dashboard_data.get('name')),
                        pages=pages
                    )
                ))

        return AccountConnectorAssets(new_relic=NewRelicAssets(assets=nr_asset_protos))

    def get_asset_options(self, model_type: SourceModelType, raw_data: Dict[str, Any]):
        """Get asset options for a specific model type."""
        if model_type == SourceModelType.NEW_RELIC_ENTITY_APPLICATION:
            return self.get_nr_entity_application_options(raw_data)
        elif model_type in [SourceModelType.NEW_RELIC_ENTITY_DASHBOARD, SourceModelType.NEW_RELIC_ENTITY_DASHBOARD_V2]:
            return self.get_nr_entity_dashboard_options(raw_data)
        else:
            raise ValueError(f"Unsupported model type: {model_type}")

    def get_asset_values(self, connector: ConnectorProto, 
                        filters: AccountConnectorAssetsModelFilters,
                        model_type: SourceModelType,
                        raw_data: Dict[str, Any]) -> AccountConnectorAssets:
        """Get asset values for a specific model type."""
        if model_type == SourceModelType.NEW_RELIC_ENTITY_APPLICATION:
            return self.get_nr_entity_application_values(connector, filters, model_type, raw_data)
        elif model_type == SourceModelType.NEW_RELIC_ENTITY_DASHBOARD:
            return self.get_nr_entity_dashboard_values(connector, filters, model_type, raw_data)
        elif model_type == SourceModelType.NEW_RELIC_ENTITY_DASHBOARD_V2:
            return self.get_nr_entity_dashboard_values_v2(connector, filters, model_type, raw_data)
        else:
            raise ValueError(f"Unsupported model type: {model_type}") 