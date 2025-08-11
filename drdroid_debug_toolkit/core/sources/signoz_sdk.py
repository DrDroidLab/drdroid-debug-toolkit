"""
Signoz-specific SDK implementation
"""

import json
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone

from ...exceptions import TaskExecutionError
from ..protos.base_pb2 import Source, SourceKeyType, TimeRange
from ..integrations.source_managers.signoz_source_manager import SignozSourceManager
from ..sdk_base import BaseSDK

logger = logging.getLogger(__name__)


class SignozSDK(BaseSDK):
    """
    Signoz-specific SDK implementation
    Provides high-level methods for Signoz operations
    """
    
    def _initialize_source_managers(self) -> Dict[str, Any]:
        """Initialize Signoz source manager"""
        managers = {}
        
        if 'signoz' in self.credentials:
            managers['signoz'] = SignozSourceManager()
            
        return managers
    
    def _get_key_type_mapping(self) -> Dict[str, SourceKeyType]:
        """Get Signoz-specific key type mapping"""
        return {
            'signoz_api_url': SourceKeyType.SIGNOZ_API_URL,
            'signoz_api_token': SourceKeyType.SIGNOZ_API_TOKEN,
        }
    
    def _get_source_manager(self) -> SignozSourceManager:
        """Get the Signoz source manager"""
        if 'signoz' not in self.credentials:
            raise TaskExecutionError("Signoz credentials not found")
        
        return self.source_managers['signoz']
    
    def _get_connector(self) -> Dict[str, Any]:
        """Get the Signoz connector configuration"""
        if 'signoz' not in self.credentials:
            raise TaskExecutionError("Signoz credentials not found")
        
        return {
            'type': Source.SIGNOZ,
            'keys': [
                {
                    'key_type': SourceKeyType.SIGNOZ_API_URL,
                    'key_value': self.credentials['signoz']['signoz_api_url']
                },
                {
                    'key_type': SourceKeyType.SIGNOZ_API_TOKEN,
                    'key_value': self.credentials['signoz'].get('signoz_api_token', '')
                }
            ]
        }
    
    def _create_time_range(self, start_time: Optional[datetime] = None, 
                          end_time: Optional[datetime] = None, 
                          duration_minutes: Optional[int] = None) -> TimeRange:
        """Create time range"""
        if start_time and end_time:
            time_geq = int(start_time.timestamp())
            time_lt = int(end_time.timestamp())
        elif duration_minutes:
            end_dt = datetime.now(timezone.utc)
            start_dt = end_dt.replace(minute=end_dt.minute - duration_minutes)
            time_geq = int(start_dt.timestamp())
            time_lt = int(end_dt.timestamp())
        else:
            # Default to last 3 hours
            end_dt = datetime.now(timezone.utc)
            start_dt = end_dt.replace(hour=end_dt.hour - 3)
            time_geq = int(start_dt.timestamp())
            time_lt = int(end_dt.timestamp())
        
        return TimeRange(time_geq=time_geq, time_lt=time_lt)
    
    def clickhouse_query(self,
                        query: str,
                        start_time: Optional[datetime] = None,
                        end_time: Optional[datetime] = None,
                        duration_minutes: Optional[int] = None,
                        step: Optional[int] = None,
                        fill_gaps: bool = False,
                        panel_type: str = "table") -> Dict[str, Any]:
        """
        Execute a Clickhouse query via Signoz
        
        Args:
            query: Clickhouse SQL query
            start_time: Start time for the query
            end_time: End time for the query
            duration_minutes: Duration in minutes
            step: Step interval in seconds
            fill_gaps: Whether to fill gaps in time series
            panel_type: Panel type ('table', 'graph', 'value')
            
        Returns:
            Query results as dictionary
        """
        try:
            source_manager = self._get_source_manager()
            connector = self._get_connector()
            time_range = self._create_time_range(start_time, end_time, duration_minutes)
            
            # Create task dictionary instead of proto
            task = {
                'type': 'CLICKHOUSE_QUERY',
                'clickhouse_query': {
                    'query': query,
                    'step': step,
                    'fill_gaps': fill_gaps,
                    'panel_type': panel_type
                }
            }
            
            # Execute task using source manager's direct API processor
            api_processor = source_manager.get_connector_processor(connector)
            from_time = int(time_range.time_geq * 1000)
            to_time = int(time_range.time_lt * 1000)
            
            # Use the tool method from the API processor
            result = api_processor.execute_clickhouse_query_tool(
                query=query,
                time_geq=time_range.time_geq,
                time_lt=time_range.time_lt,
                panel_type=panel_type,
                fill_gaps=fill_gaps,
                step=step or 60
            )
            
            if result is None:
                return {"error": "No data returned from Signoz query"}
            
            return result
            
        except Exception as e:
            raise TaskExecutionError(f"Signoz Clickhouse query failed: {e}")
    
    def builder_query(self,
                     builder_queries: Dict[str, Any],
                     start_time: Optional[datetime] = None,
                     end_time: Optional[datetime] = None,
                     duration_minutes: Optional[int] = None,
                     step: Optional[int] = None,
                     panel_type: str = "table") -> Dict[str, Any]:
        """
        Execute a builder query via Signoz
        
        Args:
            builder_queries: Builder queries configuration
            start_time: Start time for the query
            end_time: End time for the query
            duration_minutes: Duration in minutes
            step: Step interval in seconds
            panel_type: Panel type ('table', 'graph', 'value')
            
        Returns:
            Query results as dictionary
        """
        try:
            source_manager = self._get_source_manager()
            connector = self._get_connector()
            time_range = self._create_time_range(start_time, end_time, duration_minutes)
            
            # Execute task using source manager's direct API processor
            api_processor = source_manager.get_connector_processor(connector)
            
            # Use the tool method from the API processor
            result = api_processor.execute_builder_query_tool(
                builder_queries=builder_queries,
                time_geq=time_range.time_geq,
                time_lt=time_range.time_lt,
                panel_type=panel_type,
                step=step or 60
            )
            
            if result is None:
                return {"error": "No data returned from Signoz builder query"}
            
            return result
            
        except Exception as e:
            raise TaskExecutionError(f"Signoz builder query failed: {e}")
    
    def dashboard_data(self,
                      dashboard_name: str,
                      start_time: Optional[datetime] = None,
                      end_time: Optional[datetime] = None,
                      duration_minutes: Optional[int] = None,
                      step: Optional[int] = None,
                      variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Get data from a Signoz dashboard
        
        Args:
            dashboard_name: Name of the dashboard
            start_time: Start time for the query
            end_time: End time for the query
            duration_minutes: Duration in minutes
            step: Step interval in seconds
            variables: Optional variables for the dashboard
            
        Returns:
            Dashboard data as dictionary
        """
        try:
            source_manager = self._get_source_manager()
            connector = self._get_connector()
            time_range = self._create_time_range(start_time, end_time, duration_minutes)
            
            # Execute task using source manager's direct API processor
            api_processor = source_manager.get_connector_processor(connector)
            
            # Convert datetime to string format expected by the API processor
            start_time_str = None
            end_time_str = None
            
            if start_time:
                start_time_str = start_time.isoformat()
            if end_time:
                end_time_str = end_time.isoformat()
            
            variables_json = None
            if variables:
                variables_json = json.dumps(variables)
            
            result = api_processor.fetch_dashboard_data(
                dashboard_name=dashboard_name,
                start_time=start_time_str,
                end_time=end_time_str,
                step=step,
                variables_json=variables_json,
                duration=None  # We're using start_time/end_time instead
            )
            
            if result is None:
                return {"error": "No data returned from Signoz dashboard query"}
            
            return result
            
        except Exception as e:
            raise TaskExecutionError(f"Signoz dashboard data query failed: {e}")
    
    def fetch_services(self,
                      start_time: Optional[datetime] = None,
                      end_time: Optional[datetime] = None,
                      duration_minutes: Optional[int] = None) -> Dict[str, Any]:
        """
        Fetch services from Signoz
        
        Args:
            start_time: Start time for the query
            end_time: End time for the query
            duration_minutes: Duration in minutes
            
        Returns:
            Services data as dictionary
        """
        try:
            source_manager = self._get_source_manager()
            connector = self._get_connector()
            
            # Execute task using source manager's direct API processor
            api_processor = source_manager.get_connector_processor(connector)
            
            # Convert datetime to string format expected by the API processor
            start_time_str = None
            end_time_str = None
            
            if start_time:
                start_time_str = start_time.isoformat()
            if end_time:
                end_time_str = end_time.isoformat()
            
            result = api_processor.fetch_services(
                start_time=start_time_str,
                end_time=end_time_str,
                duration=None  # We're using start_time/end_time instead
            )
            
            if result is None:
                return {"error": "No data returned from Signoz services query"}
            
            return result
            
        except Exception as e:
            raise TaskExecutionError(f"Signoz services fetch failed: {e}")
    
    def fetch_dashboards(self) -> Dict[str, Any]:
        """
        Fetch all dashboards from Signoz
        
        Returns:
            Dashboards data as dictionary
        """
        try:
            source_manager = self._get_source_manager()
            connector = self._get_connector()
            
            # Execute task using source manager's direct API processor
            api_processor = source_manager.get_connector_processor(connector)
            result = api_processor.fetch_dashboards()
            
            if result is None:
                return {"error": "No data returned from Signoz dashboards query"}
            
            return result
            
        except Exception as e:
            raise TaskExecutionError(f"Signoz dashboards fetch failed: {e}")
    
    def fetch_dashboard_details(self, dashboard_id: str) -> Dict[str, Any]:
        """
        Fetch details of a specific dashboard
        
        Args:
            dashboard_id: ID of the dashboard
            
        Returns:
            Dashboard details as dictionary
        """
        try:
            source_manager = self._get_source_manager()
            connector = self._get_connector()
            
            # Execute task using source manager's direct API processor
            api_processor = source_manager.get_connector_processor(connector)
            result = api_processor.fetch_dashboard_details(dashboard_id)
            
            if result is None:
                return {"error": f"No data returned for dashboard {dashboard_id}"}
            
            return result
            
        except Exception as e:
            raise TaskExecutionError(f"Signoz dashboard details fetch failed: {e}")
    
    def fetch_apm_metrics(self,
                   service_name: str,
                   start_time: Optional[datetime] = None,
                   end_time: Optional[datetime] = None,
                   duration_minutes: Optional[int] = None,
                   window: str = "5m",
                   operation_names: Optional[List[str]] = None,
                   metrics: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Fetch APM metrics for a service
        
        Args:
            service_name: Name of the service
            start_time: Start time for the query
            end_time: End time for the query
            duration_minutes: Duration in minutes
            window: Time window for aggregation
            operation_names: Optional list of operation names to filter by
            metrics: Optional list of metrics to fetch
            
        Returns:
            APM metrics as dictionary
        """
        try:
            source_manager = self._get_source_manager()
            connector = self._get_connector()
            
            # Execute task using source manager's direct API processor
            api_processor = source_manager.get_connector_processor(connector)
            
            # Convert datetime to string format expected by the API processor
            start_time_str = None
            end_time_str = None
            
            if start_time:
                start_time_str = start_time.isoformat()
            if end_time:
                end_time_str = end_time.isoformat()
            
            result = api_processor.fetch_apm_metrics(
                service_name=service_name,
                start_time=start_time_str,
                end_time=end_time_str,
                window=window,
                operation_names=operation_names,
                metrics=metrics,
                duration=None  # We're using start_time/end_time instead
            )
            
            if result is None:
                return {"error": "No data returned from Signoz APM metrics query"}
            
            return result
            
        except Exception as e:
            raise TaskExecutionError(f"Signoz APM metrics fetch failed: {e}")
    
    def apm_metrics(self,
                   service_name: str,
                   start_time: Optional[datetime] = None,
                   end_time: Optional[datetime] = None,
                   duration_minutes: Optional[int] = None,
                   window: str = "5m",
                   operation_names: Optional[List[str]] = None,
                   metrics: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Fetch APM metrics for a service (alias for fetch_apm_metrics)
        
        Args:
            service_name: Name of the service
            start_time: Start time for the query
            end_time: End time for the query
            duration_minutes: Duration in minutes
            window: Time window for aggregation
            operation_names: Optional list of operation names to filter by
            metrics: Optional list of metrics to fetch
            
        Returns:
            APM metrics as dictionary
        """
        return self.fetch_apm_metrics(
            service_name=service_name,
            start_time=start_time,
            end_time=end_time,
            duration_minutes=duration_minutes,
            window=window,
            operation_names=operation_names,
            metrics=metrics
        )
    
    def fetch_traces_or_logs(self,
                           data_type: str,
                           start_time: Optional[datetime] = None,
                           end_time: Optional[datetime] = None,
                           duration_minutes: Optional[int] = None,
                           service_name: Optional[str] = None,
                           limit: int = 100) -> Dict[str, Any]:
        """
        Fetch traces or logs from Signoz
        
        Args:
            data_type: Type of data to fetch ('traces' or 'logs')
            start_time: Start time for the query
            end_time: End time for the query
            duration_minutes: Duration in minutes
            service_name: Optional service name to filter by
            limit: Maximum number of records to return
            
        Returns:
            Traces or logs data as dictionary
        """
        try:
            source_manager = self._get_source_manager()
            connector = self._get_connector()
            
            # Execute task using source manager's direct API processor
            api_processor = source_manager.get_connector_processor(connector)
            
            # Convert datetime to string format expected by the API processor
            start_time_str = None
            end_time_str = None
            
            if start_time:
                start_time_str = start_time.isoformat()
            if end_time:
                end_time_str = end_time.isoformat()
            
            result = api_processor.fetch_traces_or_logs(
                data_type=data_type,
                start_time=start_time_str,
                end_time=end_time_str,
                duration=None,  # We're using start_time/end_time instead
                service_name=service_name,
                limit=limit
            )
            
            if result is None:
                return {"error": f"No data returned from Signoz {data_type} query"}
            
            return result
            
        except Exception as e:
            raise TaskExecutionError(f"Signoz {data_type} fetch failed: {e}")
    
    def fetch_alerts(self) -> Dict[str, Any]:
        """
        Fetch all alerts from Signoz
        
        Returns:
            Alerts data as dictionary
        """
        try:
            source_manager = self._get_source_manager()
            connector = self._get_connector()
            
            # Execute task using source manager's direct API processor
            api_processor = source_manager.get_connector_processor(connector)
            result = api_processor.fetch_alerts()
            
            if result is None:
                return {"error": "No data returned from Signoz alerts query"}
            
            return result
            
        except Exception as e:
            raise TaskExecutionError(f"Signoz alerts fetch failed: {e}")
    
    def fetch_alert_details(self, alert_id: str) -> Dict[str, Any]:
        """
        Fetch details of a specific alert
        
        Args:
            alert_id: ID of the alert
            
        Returns:
            Alert details as dictionary
        """
        try:
            source_manager = self._get_source_manager()
            connector = self._get_connector()
            
            # Execute task using source manager's direct API processor
            api_processor = source_manager.get_connector_processor(connector)
            result = api_processor.fetch_alert_details(alert_id)
            
            if result is None:
                return {"error": f"No data returned for alert {alert_id}"}
            
            return result
            
        except Exception as e:
            raise TaskExecutionError(f"Signoz alert details fetch failed: {e}")
    
    def test_connection(self) -> bool:
        """
        Test the connection to Signoz API
        
        Returns:
            True if connection is successful, False otherwise
        """
        try:
            source_manager = self._get_source_manager()
            connector = self._get_connector()
            api_processor = source_manager.get_connector_processor(connector)
            return api_processor.test_connection()
        except Exception as e:
            logger.error(f"Signoz connection test failed: {e}")
            return False 