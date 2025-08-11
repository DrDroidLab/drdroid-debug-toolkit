#!/usr/bin/env python3
"""
Setup script for drdroid-debug-toolkit
Handles protobuf generation during installation
"""

import os
import subprocess
import sys
from setuptools import setup, find_packages

def generate_protobuf_files():
    """Generate protobuf files using Python-based script"""
    try:
        # Get the current directory (project root)
        project_root = os.getcwd()
        python_script = os.path.join(project_root, "generate_protos_python.py")
        
        if os.path.exists(python_script):
            print("🔄 Generating protobuf files...")
            
            # Run the Python-based generation script
            result = subprocess.run(
                [sys.executable, python_script],
                cwd=project_root,
                capture_output=True,
                text=True,
                check=True
            )
            print("✅ Protobuf files generated successfully")
            return True
        else:
            print(f"⚠️  Python generation script not found at {python_script}")
            return False
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to generate protobuf files: {e}")
        print(f"Error output: {e.stderr}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error during protobuf generation: {e}")
        return False

def main():
    # Generate protobuf files before setup
    if os.path.exists("drdroid_debug_toolkit"):
        generate_protobuf_files()
    
    # Read the long description from README
    with open("README.md", "r", encoding="utf-8") as fh:
        long_description = fh.read()
    
    # Read requirements
    with open("requirements.txt", "r", encoding="utf-8") as fh:
        requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]
    
    setup(
        name="drdroid-debug-toolkit",
        version="1.0.0",
        author="DroidSpace",
        author_email="support@droidspace.com",
        description="A Python SDK for integrating with monitoring and observability platforms",
        long_description=long_description,
        long_description_content_type="text/markdown",
        url="https://github.com/DrDroidLab/drdroid-debug-toolkit",
        packages=find_packages(),
        classifiers=[
            "Development Status :: 4 - Beta",
            "Intended Audience :: Developers",
            "License :: OSI Approved :: MIT License",
            "Operating System :: OS Independent",
            "Programming Language :: Python :: 3",
            "Programming Language :: Python :: 3.8",
            "Programming Language :: Python :: 3.9",
            "Programming Language :: Python :: 3.10",
            "Programming Language :: Python :: 3.11",
            "Topic :: Software Development :: Libraries :: Python Modules",
            "Topic :: System :: Monitoring",
        ],
        python_requires=">=3.8",
        install_requires=requirements,
        extras_require={
            "dev": [
                "pytest>=7.0.0",
                "pytest-cov>=4.0.0",
                "black>=22.0.0",
                "flake8>=5.0.0",
                "mypy>=1.0.0",
            ],
        },
        entry_points={
            "console_scripts": [
                "drdroid-debug-toolkit=drdroid_debug_toolkit.cli:main",
            ],
        },
        include_package_data=True,
        package_data={
            "drdroid_debug_toolkit": ["*.yaml", "*.yml"],
        },
    )

if __name__ == "__main__":
    main() 