"""
DroidSpace SDK for integrations

This SDK provides a simplified interface for working with various integrations
like Grafana, Signoz, and other monitoring platforms.
"""

import sys
import os

# Add the current directory to the Python path so absolute imports work
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

from .exceptions import DroidSDKError, ConfigurationError, ConnectionError
from .sdk_v2 import DroidSDK

__version__ = "1.0.0"
__all__ = ["DroidSDK", "DroidSDKError", "ConfigurationError", "ConnectionError"] 