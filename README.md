# DroidSpace SDK

A Python SDK for integrating with various monitoring and observability platforms like Grafana and Signoz.

## Features

- **Grafana Integration**: Query Prometheus datasources, dashboard panels, and execute all dashboard panels
- **Signoz Integration**: Execute Clickhouse queries, builder queries, and get dashboard data
- **Simplified API**: Abstract away protobuf complexity with simple Python methods
- **Type Safety**: Full type hints and validation
- **Error Handling**: Comprehensive error handling with custom exceptions

## Installation

1. Clone the repository
2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Quick Start

### 1. Create Credentials File

Create a YAML file with your credentials:

```yaml
# credentials.yaml
grafana:
  id: 1
  grafana_host: "https://your-grafana-instance.com"
  grafana_api_key: "your-grafana-api-key"
  ssl_verify: "true"

signoz:
  id: 1
  signoz_host: "https://your-signoz-instance.com"
  signoz_api_key: "your-signoz-api-key"
```

### 2. Initialize SDK

```python
from drdroid_sdk import DroidSDK

# Initialize SDK with credentials file
sdk = DroidSDK("credentials.yaml")

# Test connections
sdk.test_connection("grafana")
sdk.test_connection("signoz")
```

### 3. Use the SDK

#### Grafana Examples

```python
from datetime import datetime, timedelta

# Query Prometheus datasource
result = sdk.grafana_query_prometheus(
    datasource_uid="prometheus",
    query="up",
    duration_minutes=60
)
print(result)

# Query specific dashboard panel
result = sdk.grafana_query_dashboard_panel(
    dashboard_id="1",
    panel_id="2",
    datasource_uid="prometheus",
    queries=["up", "rate(http_requests_total[5m])"],
    duration_minutes=60
)
print(result)

# Execute all panels in a dashboard
results = sdk.grafana_execute_all_dashboard_panels(
    dashboard_uid="dashboard-uid",
    duration_minutes=60,
    panel_ids=["1", "2", "3"],  # Optional: specific panels
    template_variables={"instance": "server-1"}  # Optional: template variables
)
print(results)
```

#### Signoz Examples

```python
# Execute Clickhouse query
result = sdk.signoz_clickhouse_query(
    query="SELECT * FROM traces LIMIT 10",
    duration_minutes=60,
    panel_type="table"
)
print(result)

# Execute builder query
builder_queries = {
    "A": {
        "queryName": "A",
        "dataSource": "traces",
        "aggregateOperator": "count",
        "aggregateAttribute": {"key": "service_name"},
        "groupBy": [{"key": "service_name"}],
        "legend": "{{service_name}}"
    }
}

result = sdk.signoz_builder_query(
    builder_queries=builder_queries,
    duration_minutes=60,
    panel_type="graph"
)
print(result)

# Get dashboard data
results = sdk.signoz_dashboard_data(
    dashboard_name="My Dashboard",
    duration_minutes=60,
    variables={"service": "api-gateway"}
)
print(results)
```

## API Reference

### DroidSDK Class

#### Constructor
```python
DroidSDK(credentials_file_path: str)
```

#### Methods

##### Connection Testing
- `test_connection(source_name: str) -> bool`: Test connection to a source

##### Grafana Methods
- `grafana_query_prometheus(datasource_uid, query, **kwargs)`: Query Prometheus datasource
- `grafana_query_dashboard_panel(dashboard_id, panel_id, datasource_uid, queries, **kwargs)`: Query specific panel
- `grafana_execute_all_dashboard_panels(dashboard_uid, **kwargs)`: Execute all dashboard panels

##### Signoz Methods
- `signoz_clickhouse_query(query, **kwargs)`: Execute Clickhouse query
- `signoz_builder_query(builder_queries, **kwargs)`: Execute builder query
- `signoz_dashboard_data(dashboard_name, **kwargs)`: Get dashboard data

##### Utility Methods
- `get_supported_sources() -> List[str]`: Get list of supported sources
- `get_configured_sources() -> List[str]`: Get list of configured sources

### Common Parameters

All query methods support these common parameters:

- `start_time: Optional[datetime]`: Start time for the query
- `end_time: Optional[datetime]`: End time for the query  
- `duration_minutes: Optional[int]`: Duration in minutes (used if start_time not provided)

## Error Handling

The SDK provides custom exceptions:

- `DroidSDKError`: Base exception for all SDK errors
- `ConfigurationError`: Raised for configuration issues
- `ConnectionError`: Raised for connection issues
- `ValidationError`: Raised for validation failures
- `TaskExecutionError`: Raised when task execution fails

```python
from drdroid_sdk import DroidSDK, ConfigurationError, ConnectionError

try:
    sdk = DroidSDK("credentials.yaml")
    result = sdk.grafana_query_prometheus("prometheus", "up")
except ConfigurationError as e:
    print(f"Configuration error: {e}")
except ConnectionError as e:
    print(f"Connection error: {e}")
except Exception as e:
    print(f"Unexpected error: {e}")
```

## Examples

See the `examples/` directory for more detailed usage examples.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

This project is licensed under the MIT License. 