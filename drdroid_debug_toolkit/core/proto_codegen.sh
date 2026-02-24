#!/bin/bash
# This has been picked from https://github.com/open-telemetry/opentelemetry-python/blob/e4a4410dc046f011194eff78e801fb230961eec8/scripts/proto_codegen.sh
# This doesn't generate the grpc stubs
set -ex

repo_root="$(git rev-parse --show-toplevel)"
PROTO_REPO_DIR="$repo_root/drdroid_debug_toolkit/core/protos"
venv_dir="$(git rev-parse --show-toplevel)/venv"


cd "$repo_root/drdroid_debug_toolkit/core/"

# clean up old generated code
find "$repo_root/drdroid_debug_toolkit/core/protos/" -regex ".*_pb2.*\.pyi?" -exec rm {} +

# generate proto code for all protos
all_protos=$(find "$PROTO_REPO_DIR" -iname "*.proto")

python -m grpc_tools.protoc \
    --proto_path="$repo_root/drdroid_debug_toolkit/" \
    --python_out="$repo_root/drdroid_debug_toolkit/" \
    --mypy_out="$repo_root/drdroid_debug_toolkit/" \
    $all_protos

echo "Latest proto generation done."