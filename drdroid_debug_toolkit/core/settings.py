"""
Settings and configuration constants for the DroidSpace SDK.

This module contains global configuration constants used throughout the SDK.
"""

# External API call timeout in seconds
# This should be less than the VPC agent timeout (typically 100s)
EXTERNAL_CALL_TIMEOUT = 90  # 90 seconds to be safely under the 100s VPC timeout