#!/usr/bin/env python3
"""
Script to regenerate protobuf files for drdroid-debug-toolkit
"""

import os
import subprocess
import sys

def main():
    print("🔧 Regenerating protobuf files for drdroid-debug-toolkit...")
    
    # Get the script directory (project root)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    proto_script = os.path.join(script_dir, "drdroid_debug_toolkit", "core", "proto_codegen.sh")
    
    if not os.path.exists(proto_script):
        print(f"❌ Proto codegen script not found at {proto_script}")
        sys.exit(1)
    
    try:
        # Run the proto codegen script from the project root
        result = subprocess.run(
            ["bash", proto_script],
            cwd=script_dir,
            check=True,
            capture_output=True,
            text=True
        )
        print("✅ Protobuf files regenerated successfully!")
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to regenerate protobuf files: {e}")
        print(f"Error output: {e.stderr}")
        sys.exit(1)

if __name__ == "__main__":
    main() 