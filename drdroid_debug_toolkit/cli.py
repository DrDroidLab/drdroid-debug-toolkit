#!/usr/bin/env python3
"""
Command Line Interface for DroidSpace SDK
"""

import sys
import argparse
from .sdk_v2 import DroidSDK


def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(
        description="DroidSpace SDK - A Python SDK for integrating with monitoring and observability platforms"
    )
    parser.add_argument(
        "--version", 
        action="version", 
        version="drdroid-debug-toolkit 1.0.0"
    )
    parser.add_argument(
        "--test-connection",
        help="Test connection to a specific source (e.g., grafana, signoz)",
        metavar="SOURCE"
    )
    parser.add_argument(
        "--credentials",
        help="Path to credentials file",
        default="credentials.yaml"
    )
    
    args = parser.parse_args()
    
    if args.test_connection:
        try:
            sdk = DroidSDK(args.credentials)
            if sdk.test_connection(args.test_connection):
                print(f"✅ Connection to {args.test_connection} successful!")
            else:
                print(f"❌ Connection to {args.test_connection} failed!")
                sys.exit(1)
        except Exception as e:
            print(f"❌ Error testing connection: {e}")
            sys.exit(1)
    else:
        parser.print_help()


if __name__ == "__main__":
    main() 