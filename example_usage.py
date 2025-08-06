#!/usr/bin/env python3
"""
Example usage of the DroidSpace SDK

This file demonstrates how to use the SDK with Grafana and Signoz integrations.
"""

import json
from datetime import datetime, timedelta
from drdroid_sdk import DroidSDK, ConfigurationError, ConnectionError, TaskExecutionError


def main():
    """Main example function"""
    
    # Initialize SDK
    try:
        sdk = DroidSDK("credentials.yaml")
        print("✅ SDK initialized successfully")
    except ConfigurationError as e:
        print(f"❌ Configuration error: {e}")
        return
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return
    
    # Show configured sources
    print(f"📋 Configured sources: {sdk.get_configured_sources()}")
    print(f"🔧 Supported sources: {sdk.get_supported_sources()}")
    
    # Test connections
    print("\n🔍 Testing connections...")
    
    if "grafana" in sdk.get_configured_sources():
        try:
            if sdk.test_connection("grafana"):
                print("✅ Grafana connection successful")
            else:
                print("❌ Grafana connection failed")
        except ConnectionError as e:
            print(f"❌ Grafana connection error: {e}")
    
    if "signoz" in sdk.get_configured_sources():
        try:
            if sdk.test_connection("signoz"):
                print("✅ Signoz connection successful")
            else:
                print("❌ Signoz connection failed")
        except ConnectionError as e:
            print(f"❌ Signoz connection error: {e}")
    
    # Grafana Examples
    if "grafana" in sdk.get_configured_sources():
        print("\n📊 Grafana Examples:")
        
        # Example 1: Query Prometheus datasource
        try:
            print("\n1. Querying Prometheus datasource...")
            result = sdk.grafana_query_prometheus(
                datasource_uid="prometheus",
                query="up",
                duration_minutes=30,
                interval=60
            )
            print(f"✅ Prometheus query result: {json.dumps(result, indent=2)}")
        except TaskExecutionError as e:
            print(f"❌ Prometheus query failed: {e}")
        
        # Example 2: Query dashboard panel
        try:
            print("\n2. Querying dashboard panel...")
            result = sdk.grafana_query_dashboard_panel(
                dashboard_id="1",
                panel_id="2",
                datasource_uid="prometheus",
                queries=["up", "rate(http_requests_total[5m])"],
                duration_minutes=30
            )
            print(f"✅ Dashboard panel query result: {json.dumps(result, indent=2)}")
        except TaskExecutionError as e:
            print(f"❌ Dashboard panel query failed: {e}")
        
        # Example 3: Execute all dashboard panels
        try:
            print("\n3. Executing all dashboard panels...")
            results = sdk.grafana_execute_all_dashboard_panels(
                dashboard_uid="your-dashboard-uid",
                duration_minutes=30,
                interval=60,
                panel_ids=["1", "2"],  # Optional: specific panels
                template_variables={"instance": "server-1"}  # Optional: template variables
            )
            print(f"✅ Dashboard execution results: {json.dumps(results, indent=2)}")
        except TaskExecutionError as e:
            print(f"❌ Dashboard execution failed: {e}")
    
    # Signoz Examples
    if "signoz" in sdk.get_configured_sources():
        print("\n📈 Signoz Examples:")
        
        # Example 1: Clickhouse query
        try:
            print("\n1. Executing Clickhouse query...")
            result = sdk.signoz_clickhouse_query(
                query="SELECT * FROM traces LIMIT 10",
                duration_minutes=30,
                step=60,
                fill_gaps=True,
                panel_type="table"
            )
            print(f"✅ Clickhouse query result: {json.dumps(result, indent=2)}")
        except TaskExecutionError as e:
            print(f"❌ Clickhouse query failed: {e}")
        
        # Example 2: Builder query
        try:
            print("\n2. Executing builder query...")
            builder_queries = {
                "A": {
                    "queryName": "A",
                    "dataSource": "traces",
                    "aggregateOperator": "count",
                    "aggregateAttribute": {"key": "service_name"},
                    "groupBy": [{"key": "service_name"}],
                    "legend": "{{service_name}}",
                    "disabled": False
                }
            }
            
            result = sdk.signoz_builder_query(
                builder_queries=builder_queries,
                duration_minutes=30,
                step=60,
                panel_type="graph"
            )
            print(f"✅ Builder query result: {json.dumps(result, indent=2)}")
        except TaskExecutionError as e:
            print(f"❌ Builder query failed: {e}")
        
        # Example 3: Dashboard data
        try:
            print("\n3. Getting dashboard data...")
            results = sdk.signoz_dashboard_data(
                dashboard_name="My Dashboard",
                duration_minutes=30,
                step=60,
                variables={"service": "api-gateway"}
            )
            print(f"✅ Dashboard data results: {json.dumps(results, indent=2)}")
        except TaskExecutionError as e:
            print(f"❌ Dashboard data query failed: {e}")
    
    print("\n🎉 Example completed!")


def advanced_examples():
    """Advanced usage examples"""
    
    print("\n🚀 Advanced Examples:")
    
    try:
        sdk = DroidSDK("credentials.yaml")
        
        # Custom time ranges
        start_time = datetime.now() - timedelta(hours=2)
        end_time = datetime.now()
        
        # Grafana with custom time range
        if "grafana" in sdk.get_configured_sources():
            try:
                result = sdk.grafana_query_prometheus(
                    datasource_uid="prometheus",
                    query="rate(http_requests_total[5m])",
                    start_time=start_time,
                    end_time=end_time,
                    interval=120
                )
                print(f"✅ Custom time range query: {json.dumps(result, indent=2)}")
            except TaskExecutionError as e:
                print(f"❌ Custom time range query failed: {e}")
        
        # Signoz with complex builder query
        if "signoz" in sdk.get_configured_sources():
            try:
                complex_builder_queries = {
                    "A": {
                        "queryName": "A",
                        "dataSource": "traces",
                        "aggregateOperator": "avg",
                        "aggregateAttribute": {"key": "duration"},
                        "groupBy": [
                            {"key": "service_name"},
                            {"key": "operation_name"}
                        ],
                        "legend": "{{service_name}} - {{operation_name}}",
                        "disabled": False
                    },
                    "B": {
                        "queryName": "B",
                        "dataSource": "traces",
                        "aggregateOperator": "count",
                        "aggregateAttribute": {"key": "trace_id"},
                        "groupBy": [{"key": "service_name"}],
                        "legend": "{{service_name}} count",
                        "disabled": False
                    }
                }
                
                result = sdk.signoz_builder_query(
                    builder_queries=complex_builder_queries,
                    start_time=start_time,
                    end_time=end_time,
                    panel_type="graph"
                )
                print(f"✅ Complex builder query: {json.dumps(result, indent=2)}")
            except TaskExecutionError as e:
                print(f"❌ Complex builder query failed: {e}")
                
    except Exception as e:
        print(f"❌ Advanced examples failed: {e}")


if __name__ == "__main__":
    main()
    advanced_examples() 