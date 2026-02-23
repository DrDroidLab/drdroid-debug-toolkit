#!/usr/bin/env python3
"""
Example usage of the DroidSpace SDK

This file demonstrates how to use the toolkit with integrations like Sentry,
using the same approach as the prototype backend.
"""

# Configure Django settings BEFORE importing toolkit modules
from django.conf import settings

if not settings.configured:
    settings.configure(
        DEBUG=True,
        IS_PROD_ENV=False,
        MASTER_ASSETS_TOKEN=None,
        DRD_CLOUD_API_TOKEN=None,
        SECRET_KEY='dummy-secret-key-for-standalone-usage',
        INSTALLED_APPS=[],
    )

import yaml
from drdroid_debug_toolkit.core.utils.credentilal_utils import credential_yaml_to_connector_proto
from drdroid_debug_toolkit.core.integrations.source_managers.sentry_source_manager import SentrySourceManager
from drdroid_debug_toolkit.core.integrations.source_managers.datadog_source_manager import DatadogSourceManager


def load_credentials(credentials_file: str) -> dict:
    """Load credentials from YAML file"""
    with open(credentials_file, 'r') as f:
        return yaml.safe_load(f)


def test_sentry_connection(credentials: dict) -> bool:
    """
    Test Sentry connection using the prototype backend approach.

    This directly uses credential_yaml_to_connector_proto and the source manager,
    exactly like the prototype backend does.
    """
    if 'sentry' not in credentials:
        print("No Sentry configuration found in credentials")
        return False

    # Convert YAML credentials to Connector proto (prototype approach)
    connector_proto = credential_yaml_to_connector_proto('sentry', credentials['sentry'])

    # Use the source manager directly to test connection
    source_manager = SentrySourceManager()
    return source_manager.test_connector_processor(connector_proto)


def test_datadog_connection(credentials: dict) -> bool:
    """
    Test Datadog connection using the prototype backend approach.

    This directly uses credential_yaml_to_connector_proto and the source manager,
    exactly like the prototype backend does.
    """
    if 'datadog' not in credentials:
        print("No Datadog configuration found in credentials")
        return False

    # Convert YAML credentials to Connector proto (prototype approach)
    connector_proto = credential_yaml_to_connector_proto('datadog', credentials['datadog'])

    # Use the source manager directly to test connection
    source_manager = DatadogSourceManager()
    return source_manager.test_connector_processor(connector_proto)


def main():
    """Main example function"""

    # Load credentials
    try:
        credentials = load_credentials("credentials.yaml")
        print("Credentials loaded successfully")
    except Exception as e:
        print(f"Failed to load credentials: {e}")
        return

    # Show configured sources
    print(f"Configured sources: {list(credentials.keys())}")

    # Test Sentry connection
    if "sentry" in credentials:
        print("\nTesting Sentry connection...")
        try:
            if test_sentry_connection(credentials):
                print("Sentry connection successful")
            else:
                print("Sentry connection failed")
        except Exception as e:
            print(f"Sentry connection error: {e}")

    # Test Datadog connection
    if "datadog" in credentials:
        print("\nTesting Datadog connection...")
        try:
            if test_datadog_connection(credentials):
                print("Datadog connection successful")
            else:
                print("Datadog connection failed")
        except Exception as e:
            print(f"Datadog connection error: {e}")

if __name__ == "__main__":
    main()