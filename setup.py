#!/usr/bin/env python3
"""
Setup script for DroidSpace SDK
"""

from setuptools import setup, find_packages
import os

# Read the README file
def read_readme():
    readme_path = os.path.join(os.path.dirname(__file__), 'README.md')
    if os.path.exists(readme_path):
        with open(readme_path, 'r', encoding='utf-8') as f:
            return f.read()
    return "DroidSpace SDK for integrations"

# Read requirements
def read_requirements():
    requirements_path = os.path.join(os.path.dirname(__file__), 'requirements.txt')
    if os.path.exists(requirements_path):
        with open(requirements_path, 'r', encoding='utf-8') as f:
            return [line.strip() for line in f if line.strip() and not line.startswith('#')]
    return []

setup(
    name="drdroid-sdk",
    version="1.0.0",
    description="A Python SDK for integrating with monitoring and observability platforms",
    long_description=read_readme(),
    long_description_content_type="text/markdown",
    author="DroidSpace",
    author_email="support@droidspace.com",
    url="https://github.com/droidspace/drdroid-sdk",
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
    install_requires=read_requirements(),
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "black>=22.0.0",
            "flake8>=5.0.0",
            "mypy>=1.0.0",
        ],
    },
    include_package_data=True,
    package_data={
        "drdroid_sdk": ["*.yaml", "*.yml"],
    },
    entry_points={
        "console_scripts": [
            "drdroid-sdk=drdroid_sdk.cli:main",
        ],
    },
    keywords="monitoring observability grafana signoz prometheus clickhouse",
    project_urls={
        "Bug Reports": "https://github.com/droidspace/drdroid-sdk/issues",
        "Source": "https://github.com/droidspace/drdroid-sdk",
        "Documentation": "https://github.com/droidspace/drdroid-sdk/blob/main/README.md",
    },
) 