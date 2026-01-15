"""
DrDroid Debug Toolkit

Integration toolkit for DrDroid platform with 40+ service integrations.
Provides source managers, API processors, and metadata extractors.
"""

import sys
import os

# Add the current directory to the Python path so absolute imports work
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

__version__ = "1.0.0" 