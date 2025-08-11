# Doctor Droid Debug Toolkit

A Python SDK for integrating with various monitoring and observability platforms like Grafana, Signoz, Kubernetes, CloudWatch, and Sentry.

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

### Primary Installation Methods

#### Using uv (Recommended)
```bash
uv pip install git+https://github.com/DrDroidLab/drdroid-debug-toolkit.git
```

#### Using pip
```bash
pip install git+https://github.com/DrDroidLab/drdroid-debug-toolkit.git
```

#### Using uv add (for projects with pyproject.toml)
```bash
uv add git+https://github.com/DrDroidLab/drdroid-debug-toolkit.git
```

### Alternative: Clone and Install

```bash
git clone https://github.com/DrDroidLab/drdroid-debug-toolkit.git
cd drdroid-debug-toolkit
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
```

### 3. Use the SDK

#### Discovering Available Tasks

Before using specific methods, you can discover what tasks are available:

```python
# List all supported sources
supported_sources = sdk.get_supported_sources()
print(f"Supported sources: {supported_sources}")

# List configured sources
configured_sources = sdk.get_configured_sources()
print(f"Configured sources: {configured_sources}")

# List available tasks for a specific source
grafana_tasks = sdk.list_available_tasks("grafana")
print(f"Available Grafana tasks: {grafana_tasks}")

signoz_tasks = sdk.list_available_tasks("signoz")
print(f"Available Signoz tasks: {signoz_tasks}")
```

#### Grafana Examples

```python
# Query Prometheus datasource
result = sdk.grafana_query_prometheus(
    datasource_uid="prometheus",
    query="up",
    duration_minutes=60
)

# Execute all panels in a dashboard
results = sdk.grafana_execute_all_dashboard_panels(
    dashboard_uid="dashboard-uid",
    duration_minutes=60,
    template_variables={"instance": "server-1"}
)
```

#### Signoz Examples

```python
# Execute Clickhouse query
result = sdk.signoz_clickhouse_query(
    query="SELECT * FROM traces LIMIT 10",
    duration_minutes=60,
    panel_type="table"
)

# Get dashboard data
results = sdk.signoz_dashboard_data(
    dashboard_name="My Dashboard",
    duration_minutes=60,
    variables={"service": "api-gateway"}
)
```

#### Kubernetes Examples

```python
# Execute kubectl command
result = sdk.kubernetes_execute_command(
    command="kubectl get pods -A"
)

# Execute write command
result = sdk.kubernetes_execute_write_command(
    command="kubectl scale deployment nginx --replicas=3"
)
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

# Filter CloudWatch logs
result = sdk.cloudwatch_filter_log_events(
    log_group_name="/aws/lambda/my-function",
    region="us-west-2",
    filter_query="ERROR"
)
```

#### Sentry Examples

```python
# Fetch issue information
result = sdk.sentry_fetch_issue_info_by_id(
    issue_id="1234567890"
)

# Fetch recent events
result = sdk.sentry_fetch_list_of_recent_events_with_search_query(
    project_slug="my-project",
    query="is:unresolved",
    max_events_to_analyse=10
)
```

## API Reference

### DroidSDK Class

#### Constructor
```python
DroidSDK(credentials_file_path: str)
```

#### Key Methods

##### Task Discovery
- `get_supported_sources() -> List[str]`: Get list of all supported sources
- `get_configured_sources() -> List[str]`: Get list of configured sources
- `list_available_tasks(source_name: str) -> List[str]`: List available tasks for a specific source

##### Grafana
- `grafana_query_prometheus(datasource_uid, query, **kwargs)`: Query Prometheus datasource
- `grafana_execute_all_dashboard_panels(dashboard_uid, **kwargs)`: Execute all dashboard panels

##### Signoz
- `signoz_clickhouse_query(query, **kwargs)`: Execute Clickhouse query
- `signoz_dashboard_data(dashboard_name, **kwargs)`: Get dashboard data

##### Kubernetes
- `kubernetes_execute_command(command, **kwargs)`: Execute kubectl command
- `kubernetes_execute_write_command(command, **kwargs)`: Execute kubectl write command

##### CloudWatch
- `cloudwatch_execute_metric(namespace, metric_name, region, **kwargs)`: Execute metric query
- `cloudwatch_filter_log_events(log_group_name, region, **kwargs)`: Filter log events

##### Sentry
- `sentry_fetch_issue_info_by_id(issue_id, **kwargs)`: Fetch issue information
- `sentry_fetch_list_of_recent_events_with_search_query(project_slug, **kwargs)`: Fetch recent events

### Common Parameters

All query methods support:
- `start_time: Optional[datetime]`: Start time for the query
- `end_time: Optional[datetime]`: End time for the query  
- `duration_minutes: Optional[int]`: Duration in minutes (used if start_time not provided)

## Error Handling

```python
from drdroid_debug_toolkit import DroidSDK, ConfigurationError, ConnectionError

try:
    sdk = DroidSDK("credentials.yaml")
    result = sdk.grafana_query_prometheus("prometheus", "up")
except ConfigurationError as e:
    print(f"Configuration error: {e}")
except ConnectionError as e:
    print(f"Connection error: {e}")
```

## Troubleshooting

### Import Issues

If you encounter import errors:

1. **Verify installation**:
   ```bash
   pip list | grep drdroid-debug-toolkit
   ```

2. **Check Python path**:
   ```bash
   python -c "import drdroid_debug_toolkit; print('Package found!')"
   ```

3. **Reinstall the package**:
   ```bash
   pip uninstall drdroid-debug-toolkit
   uv pip install git+https://github.com/DrDroidLab/drdroid-debug-toolkit.git@fix/setup-fixes
   ```

### Common Issues

- **ModuleNotFoundError**: Use the correct import name: `drdroid_debug_toolkit`
- **ConfigurationError**: Verify your credentials file format and path

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

This project is licensed under the MIT License. 