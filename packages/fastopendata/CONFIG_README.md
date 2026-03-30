# FastOpenData Centralized Configuration

This document describes the centralized configuration system for the fastopendata project.

## Overview

Configuration is centralized in `config.toml` and accessed through the `fastopendata.config` module. This replaces the previous scattered configuration across multiple files and provides a consistent interface for all components.

## Configuration File

**Location**: `packages/fastopendata/config.toml`

**Format**: TOML (human-readable, supports comments)

**Sections**:
- `[paths]` - File and directory paths
- `[downloads]` - Download configuration (timeouts, retries, user agents)
- `[datasets.*]` - Individual dataset metadata and URLs
- `[geography]` - Geographic data (state FIPS codes)
- `[api]` - API server configuration
- `[processing]` - Processing pipeline configuration
- `[logging]` - Logging configuration

## Usage

### Python Code

```python
from fastopendata.config import config

# Access configuration values
data_dir = config.data_dir  # Supports DATA_DIR environment override
datasets = config.datasets
state_fips = config.state_fips

# Get resolved paths
data_path = config.data_path  # Resolved Path object
output_path = config.get_dataset_path("acs_pums_1yr_persons")

# Get dataset metadata
dataset = config.get_dataset("tiger_puma")
url = dataset.url
description = dataset.description
```

### Snakefile

The Snakefile automatically imports the centralized config:

```python
# Configuration is imported automatically
DATA_DIR = fod_config.data_dir  # From centralized config
STATE_FIPS = fod_config.state_fips  # From centralized config
CENSUS_WGET = fod_config.get_census_wget_flags()  # From centralized config
```

### Shell Scripts

Use the provided helper script to export configuration as environment variables:

```bash
# Source the config export script
cd packages/fastopendata/
source export_config.sh

# Now environment variables are available
echo $DATA_DIR
echo $SCRIPTS_DIR
echo $MAX_WORKERS
```

## Environment Variable Overrides

The most important environment variable override is `DATA_DIR`:

```bash
# Override data directory for all components
export DATA_DIR=/path/to/custom/data/directory

# Use with Snakemake
DATA_DIR=/mnt/data/fastopendata snakemake --cores 4

# Use with Python scripts
DATA_DIR=/mnt/data/fastopendata uv run python process_script.py

# Use with shell scripts
DATA_DIR=/mnt/data/fastopendata source export_config.sh
```

## Migration Guide

### From Environment Variables

**Before**:
```python
import os
DATA_DIR = os.environ["DATA_DIR"]  # Required, no fallback
```

**After**:
```python
from fastopendata.config import config
data_dir = config.data_dir  # Uses config.toml default, DATA_DIR override
```

### From Shell Script Defaults

**Before**:
```bash
DATA_DIR="${DATA_DIR:-$(dirname "$0")/../../../../raw_data}"
```

**After**:
```bash
source export_config.sh  # Exports DATA_DIR from centralized config
# Or manually:
# export DATA_DIR=$(uv run python -c "from fastopendata.config import config; print(config.data_dir)")
```

### From Hardcoded Values

**Before**:
```python
# Snakefile
STATE_FIPS = ["01", "02", "04", ...]  # Hardcoded list
CENSUS_WGET = "wget --no-check-certificate ..."  # Hardcoded flags
```

**After**:
```python
# Snakefile (automatically imported)
STATE_FIPS = fod_config.state_fips  # From config.toml
CENSUS_WGET = fod_config.get_census_wget_flags()  # From config.toml
```

## Benefits

1. **Single Source of Truth**: All configuration in one file
2. **Environment Overrides**: Important paths can still be overridden
3. **Type Safety**: Configuration values are typed and validated
4. **Documentation**: Self-documenting with comments in TOML
5. **Consistency**: Same interface for all components
6. **Maintainability**: Easy to update URLs, paths, and settings

## Adding New Configuration

To add new configuration options:

1. **Update `config.toml`**: Add your setting in the appropriate section
2. **Update `config.py`**: Add a property to access the new setting
3. **Update code**: Use `config.your_new_setting` instead of hardcoded values

Example:

```toml
# config.toml
[processing]
timeout_seconds = 300
```

```python
# config.py
@property
def processing_timeout(self) -> int:
    return self._data["processing"]["timeout_seconds"]
```

```python
# Usage
from fastopendata.config import config
timeout = config.processing_timeout
```

## Testing

The configuration system is designed for easy testing:

```python
# Test with custom config
from fastopendata.config import Config
test_config = Config(config_file="test_config.toml")
```

## File Structure

```
packages/fastopendata/
├── config.toml                 # Main configuration file
├── export_config.sh           # Shell helper script
├── CONFIG_README.md           # This documentation
├── src/fastopendata/
│   ├── config.py              # Configuration module
│   ├── api.py                 # Updated to use config
│   └── processing/
│       └── *.py              # Updated scripts use config
└── Snakefile                  # Updated to use config
```