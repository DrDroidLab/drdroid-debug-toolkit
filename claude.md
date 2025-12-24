# DrDroid Debug Toolkit

**Unified integration toolkit for DrDroid platform with 40+ service integrations.**

## Architecture

**Two-layer design:**
- **Source Managers** (41): Task orchestrators that map task types to execution methods and expose tasks to agents
- **API Processors** (43): Network handlers for empirical APIs and external service connections

```
DroidSDK → SourceManager → APIProcessor → External Service
```

## Core Components

### DroidSDK (Entry Point)
```python
from drdroid_debug_toolkit import DroidSDK

sdk = DroidSDK("credentials.yaml")
sdk.test_connection("grafana")
sdk.get_supported_sources()  # List all 40+ integrations
sdk.grafana.query_prometheus("up")  # Direct source access
```

### Source Managers
**Task executors** that define:
- Available task types per integration
- Form field schemas for UI
- Variable resolution logic
- Result transformation

### API Processors
**Network layer** handling:
- Authentication & credentials
- API calls & response parsing
- Connection testing
- Error handling

## Available Integrations

**Monitoring & Observability:**
Grafana, Grafana Loki, Grafana Mimir, Datadog, New Relic, Signoz, Coralogix, Sentry, Victoria Logs, Prometheus

**Cloud & Infrastructure:**
AWS CloudWatch, EKS, GKE, Azure, Kubernetes, ArgoCD, Render

**Databases:**
Postgres, ClickHouse, MongoDB, OpenSearch, Elasticsearch, BigQuery, SQL Database

**DevOps & Collaboration:**
Jenkins, GitHub, GitHub Actions, Jira, Slack, SMTP

**Utilities:**
Bash, HTTP API, Documentation

## Configuration

YAML-based credentials:
```yaml
grafana:
  grafana_host: "https://your-grafana.com"
  grafana_api_key: "your-key"

datadog:
  datadog_api_key: "your-key"
  datadog_app_key: "your-app-key"
```

## Key Features

- **Multi-source SDK management** via factory pattern
- **Protocol buffer** definitions for type safety
- **Credential management** with YAML configuration
- **Connection testing** across all integrations
- **Task execution** with variable resolution
- **CLI interface** for debugging
- **Extensible architecture** for new integrations

## Quick Start

```bash
# Install
pip install drdroid-debug-toolkit

# Test connections
drdroid-debug-toolkit --test-connection

# Python usage
from drdroid_debug_toolkit import DroidSDK
sdk = DroidSDK("creds.yaml")
result = sdk.grafana.query_prometheus("cpu_usage")
```

## Adding New Integrations

Requires 3 components:
1. **SourceManager** - Task definition & execution logic
2. **APIProcessor** - Network communication layer
3. **Proto definitions** - Task types & result schemas