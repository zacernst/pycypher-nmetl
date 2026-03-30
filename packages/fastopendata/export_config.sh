#!/usr/bin/env bash
# Export configuration values from config.toml as environment variables.
#
# Usage:
#   source export_config.sh
#   # OR
#   eval $(./export_config.sh)

set -euo pipefail

# Get the directory containing this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Export DATA_DIR from the centralized configuration
export DATA_DIR=$(cd "$SCRIPT_DIR" && uv run python -c "
import sys
sys.path.insert(0, 'src')
from fastopendata.config import config
print(config.data_dir)
")

# Export other useful configuration values
export SCRIPTS_DIR=$(cd "$SCRIPT_DIR" && uv run python -c "
import sys
sys.path.insert(0, 'src')
from fastopendata.config import config
print(config.scripts_path)
")

export MAX_WORKERS=$(cd "$SCRIPT_DIR" && uv run python -c "
import sys
sys.path.insert(0, 'src')
from fastopendata.config import config
print(config.max_workers)
")

# Print exported variables for verification
echo "Exported configuration variables:"
echo "  DATA_DIR=$DATA_DIR"
echo "  SCRIPTS_DIR=$SCRIPTS_DIR"
echo "  MAX_WORKERS=$MAX_WORKERS"