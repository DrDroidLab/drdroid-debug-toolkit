# DroidSpace SDK

A Python SDK for integrating with various monitoring and observability platforms like Grafana and Signoz.

## Features

- **Grafana Integration**: Query Prometheus datasources, dashboard panels, and execute all dashboard panels
- **Signoz Integration**: Execute Clickhouse queries, builder queries, and get dashboard data
- **Bash Integration**: Execute bash commands on local or remote servers
- **Kubernetes Integration**: Execute kubectl commands and write operations
- **CloudWatch Integration**: Query metrics, filter logs, manage ECS, and fetch S3 files
- **Sentry Integration**: Fetch issues, events, and error analytics
- **Simplified API**: Abstract away protobuf complexity with simple Python methods
- **Type Safety**: Full type hints and validation
- **Error Handling**: Comprehensive error handling with custom exceptions

## Installation

### From GitHub (Development)

1. Clone the repository:
```bash
git clone https://github.com/DrDroidLab/drdroid-debug-toolkit.git
cd drdroid-debug-toolkit
```

2. Install in development mode:
```bash
pip install -e .
```

### From PyPI (Production)

```bash
pip install drdroid-debug-toolkit
```

### Manual Installation

1. Clone the repository
2. Install dependencies:
```bash
pip install -r requirements.txt
```
3. Install the package:
```bash
pip install -e .
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

bash:
  id: 1
  remote_host: "localhost"  # or remote server
  remote_user: "your-username"
  remote_password: "your-password"  # or use remote_private_key

kubernetes:
  id: 1
  kubeconfig: "/path/to/kubeconfig"  # or inline kubeconfig
  context: "your-context"
  namespace: "default"

cloudwatch:
  id: 1
  aws_access_key_id: "your-aws-access-key"
  aws_secret_access_key: "your-aws-secret-key"
  aws_region: "us-west-2"

sentry:
  id: 1
  sentry_api_url: "https://sentry.io/api/0/"
  sentry_api_token: "your-sentry-api-token"
```

### 2. Initialize SDK

```python
from drdroid_debug_toolkit import DroidSDK

# Initialize SDK with credentials file
sdk = DroidSDK("credentials.yaml")

# Test connections
sdk.test_connection("grafana")
sdk.test_connection("signoz")
sdk.test_connection("bash")
sdk.test_connection("kubernetes")
sdk.test_connection("cloudwatch")
sdk.test_connection("sentry")
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

#### Bash Examples

```python
# Execute bash command on local machine
result = sdk.bash_execute_command(
    command="echo 'Hello from DrDroid SDK' && date",
    remote_server="localhost"
)
print(result)

# Execute bash command on remote server
result = sdk.bash_execute_command(
    command="ps aux | grep nginx",
    remote_server="server.example.com"
)
print(result)
```

#### Kubernetes Examples

```python
# Execute kubectl command
result = sdk.kubernetes_execute_command(
    command="kubectl get pods -A"
)
print(result)

# Execute write command (apply, scale, delete)
result = sdk.kubernetes_execute_write_command(
    command="kubectl scale deployment nginx --replicas=3"
)
print(result)
```

#### CloudWatch Examples

```python
# Query CloudWatch metrics
result = sdk.cloudwatch_execute_metric(
    namespace="AWS/EC2",
    metric_name="CPUUtilization",
    region="us-west-2",
    dimensions=[{"name": "InstanceId", "value": "i-1234567890abcdef0"}],
    statistic="Average",
    period=300,
    duration_minutes=60
)
print(result)

# Filter CloudWatch logs
result = sdk.cloudwatch_filter_log_events(
    log_group_name="/aws/lambda/my-function",
    region="us-west-2",
    filter_query="ERROR"
)
print(result)

# List ECS clusters
result = sdk.cloudwatch_ecs_list_clusters()
print(result)

# Get ECS task logs
result = sdk.cloudwatch_ecs_get_task_logs(
    cluster_name="my-cluster",
    task_definition="my-task:1",
    max_lines=1000
)
print(result)
```

#### Sentry Examples

```python
# Fetch issue information
result = sdk.sentry_fetch_issue_info_by_id(
    issue_id="1234567890"
)
print(result)

# Fetch event information
result = sdk.sentry_fetch_event_info_by_id(
    event_id="event-id-123",
    project_slug="my-project"
)
print(result)

# Fetch recent events with search query
result = sdk.sentry_fetch_list_of_recent_events_with_search_query(
    project_slug="my-project",
    query="is:unresolved",
    max_events_to_analyse=10
)
print(result)

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

##### Bash Methods
- `bash_execute_command(command, remote_server=None, **kwargs)`: Execute bash command

##### Kubernetes Methods
- `kubernetes_execute_command(command, **kwargs)`: Execute kubectl command
- `kubernetes_execute_write_command(command, **kwargs)`: Execute kubectl write command

##### CloudWatch Methods
- `cloudwatch_execute_metric(namespace, metric_name, region, **kwargs)`: Execute metric query
- `cloudwatch_filter_log_events(log_group_name, region, **kwargs)`: Filter log events
- `cloudwatch_ecs_list_clusters(**kwargs)`: List ECS clusters
- `cloudwatch_ecs_list_tasks(cluster_name, **kwargs)`: List ECS tasks
- `cloudwatch_ecs_get_task_logs(cluster_name, **kwargs)`: Get ECS task logs
- `cloudwatch_fetch_dashboard(dashboard_name, **kwargs)`: Fetch dashboard data
- `cloudwatch_fetch_s3_file(bucket_name, object_key, **kwargs)`: Fetch S3 file
- `cloudwatch_rds_get_sql_query_performance_stats(db_resource_uri, **kwargs)`: Get RDS performance stats

##### Sentry Methods
- `sentry_fetch_issue_info_by_id(issue_id, **kwargs)`: Fetch issue information
- `sentry_fetch_event_info_by_id(event_id, project_slug, **kwargs)`: Fetch event information
- `sentry_fetch_list_of_recent_events_with_search_query(project_slug, **kwargs)`: Fetch recent events

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

## Troubleshooting

### Import Issues

If you encounter import errors, try the following:

1. **Verify installation**: Make sure the package is installed correctly:
   ```bash
   pip list | grep drdroid-debug-toolkit
   ```

2. **Check Python path**: Ensure you're using the correct Python environment:
   ```bash
   python -c "import drdroid_debug_toolkit; print('Package found!')"
   ```

3. **Reinstall the package**: If issues persist, try reinstalling:
   ```bash
   pip uninstall drdroid-debug-toolkit
   pip install -e .
   ```

4. **Test imports**: Run the test script to verify imports:
   ```bash
   python test_import.py
   ```

### Common Issues

- **ModuleNotFoundError**: Make sure you're using the correct import name: `drdroid_debug_toolkit`
- **AttributeError**: Check that you're importing the correct classes from the package
- **ConfigurationError**: Verify your credentials file format and path

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

This project is licensed under the MIT License. 