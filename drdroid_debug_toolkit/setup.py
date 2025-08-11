"""
Custom setup commands for drdroid-debug-toolkit
Handles protobuf code generation during installation
"""

import os
import subprocess
import sys
from setuptools import Command
from setuptools.command.develop import develop
from setuptools.command.install import install


def generate_protobuf_files():
    """Generate protobuf files using the proto_codegen.sh script"""
    try:
        # Get the package directory
        package_dir = os.path.dirname(os.path.abspath(__file__))
        # Get the project root (two levels up from the package)
        project_root = os.path.dirname(os.path.dirname(package_dir))
        proto_script = os.path.join(package_dir, "core", "proto_codegen.sh")
        
        if os.path.exists(proto_script):
            print("🔄 Generating protobuf files...")
            
            # Run the script from the project root (where git is available)
            result = subprocess.run(
                ["bash", proto_script],
                cwd=project_root,
                capture_output=True,
                text=True,
                check=True
            )
            print("✅ Protobuf files generated successfully")
            return True
        else:
            print(f"⚠️  Proto codegen script not found at {proto_script}")
            return False
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to generate protobuf files: {e}")
        print(f"Error output: {e.stderr}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error during protobuf generation: {e}")
        return False


class DevelopCommand(develop):
    """Custom develop command that generates protobuf files"""
    
    def run(self):
        # First run the normal develop command
        develop.run(self)
        
        # Then generate protobuf files
        generate_protobuf_files()


class InstallCommand(install):
    """Custom install command that generates protobuf files"""
    
    def run(self):
        # First run the normal install command
        install.run(self)
        
        # Then generate protobuf files
        generate_protobuf_files()


class GenerateProtobufCommand(Command):
    """Custom command to generate protobuf files"""
    
    description = "Generate protobuf files from .proto definitions"
    user_options = []
    
    def initialize_options(self):
        pass
    
    def finalize_options(self):
        pass
    
    def run(self):
        success = generate_protobuf_files()
        if not success:
            sys.exit(1) 