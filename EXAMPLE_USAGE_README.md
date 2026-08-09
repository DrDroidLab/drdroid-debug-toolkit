# Running the Example Usage Script

This guide explains how to get the `example_usage.py` script working.

## Prerequisites

- Python 3.8+
- pip

## Setup Steps

### 1. Install the Package

```bash
cd /path/to/drdroid-debug-toolkit
pip install -e .
```

### 2. Create credentials.yaml

Create a `credentials.yaml` file in the project root with your Grafana credentials:

```yaml
grafana:
  id: 1
  type: GRAFANA
  grafana_host: "https://your-grafana-instance.grafana.net"
  grafana_api_key: "your-grafana-api-key-here"
  ssl_verify: "true"
```

**Note:** Replace the placeholder values with your actual Grafana host and API key.

### 3. Run the Script

```bash
python example_usage.py
```

## Available Integrations

The following integrations are available in the SDK registry:

| Integration | Credentials Required |
|-------------|---------------------|
| grafana | `grafana_host`, `grafana_api_key`, `ssl_verify` |
| signoz | `signoz_api_url`, `signoz_api_token` |
| postgres | `host`, `user`, `password`, `database`, `port` |
| clickhouse | `host`, `user`, `password`, `port`, `interface` |
| cloudwatch | `region`, `aws_access_key`, `aws_secret_key` |
| newrelic | `api_key`, `app_id`, `api_domain` |
| posthog | `api_key`, `app_host`, `project_id` |
| bash | `remote_host`, `remote_user`, `remote_password` or `remote_pem` |
| kubernetes | `cluster_api_server`, `cluster_token` |
| sql_database_connection | `connection_string` |

Each integration block in `credentials.yaml` must include a `type` field matching the integration name in uppercase (e.g., `type: GRAFANA`).

## Troubleshooting

### Django ImproperlyConfigured Error

The example script already includes Django configuration. If you see this error, ensure you're running the updated `example_usage.py`.

### Module Not Found Error

```bash
pip install -e .
```

### Connection Errors

- Verify your credentials in `credentials.yaml`
- Check that your API endpoints are accessible
- For self-signed certificates, set `ssl_verify: "false"`
