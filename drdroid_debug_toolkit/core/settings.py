"""
Settings and configuration constants for the DroidSpace SDK.

This module contains global configuration constants used throughout the SDK.
"""

# External API call timeout in seconds
# This should be less than the VPC agent timeout (typically 100s)
EXTERNAL_CALL_TIMEOUT = 90  # 90 seconds to be safely under the 100s VPC timeout

# Log-query calls (Loki/VictoriaLogs/SigNoz-logs/etc.) get a dedicated, uniform budget.
# Logs are frequently slower than metric/config calls, and on the VPC-agent path the
# prototype proxy-poll cap is set just above this (see playbook_source_facade) so a full
# LOG_QUERY_TIMEOUT of work can complete and be written back before prototype gives up.
LOG_QUERY_TIMEOUT = 60