"""
Custom exceptions for the DroidSpace SDK
"""


class DroidSDKError(Exception):
    """Base exception for all SDK errors"""
    pass


class ConfigurationError(DroidSDKError):
    """Raised when there's an issue with SDK configuration"""
    pass


class ConnectionError(DroidSDKError):
    """Raised when there's an issue connecting to the integration"""
    pass


class ValidationError(DroidSDKError):
    """Raised when input validation fails"""
    pass


class TaskExecutionError(DroidSDKError):
    """Raised when task execution fails"""
    pass 