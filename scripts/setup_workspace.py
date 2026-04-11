#!/usr/bin/env python3
"""Setup workspace directories based on workspace.toml configuration.

This script creates the directory structure and sets up environment
variables for the pycypher-nmetl data processing pipeline.

Usage:
    python scripts/setup_workspace.py [--config workspace.toml] [--env development|production]
"""

import argparse
import os
import tomllib
from pathlib import Path


def load_workspace_config(config_path: Path) -> dict:
    """Load workspace configuration from TOML file."""
    with open(config_path, "rb") as f:
        return tomllib.load(f)


def create_directories(config: dict, environment: str = "development") -> None:
    """Create workspace directory structure."""
    data_root = Path(config["workspace"]["data_root"])

    # Create base directories
    directories = [
        data_root / config["fastopendata"]["raw_data_dir"],
        data_root / config["fastopendata"]["processed_data_dir"],
        data_root / config["fastopendata"]["cache_dir"],
        data_root / config["fastopendata"]["logs_dir"],
        data_root / config["pycypher"]["pipeline_configs"],
        data_root / config["pycypher"]["graph_data"],
        data_root / config["pycypher"]["query_cache"],
        data_root / config["pycypher"]["performance_logs"],
        data_root / config["nmetl"]["project_configs"],
        data_root / config["nmetl"]["output_exports"],
        data_root / config["nmetl"]["validation_reports"],
    ]

    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)
        print(f"Created: {directory}")


def generate_env_file(config: dict, environment: str = "development") -> None:
    """Generate .env file with workspace environment variables."""
    data_root = Path(config["workspace"]["data_root"])
    base_dir = Path(config["workspace"]["base_dir"])

    env_content = f"""# pycypher-nmetl Workspace Environment Variables
# Generated from workspace.toml for {environment} environment

# Base paths
WORKSPACE_BASE_DIR={base_dir}
DATA_ROOT_DIR={data_root}

# fastopendata configuration
DATA_DIR={data_root / config["fastopendata"]["raw_data_dir"]}
FASTOPENDATA_OUTPUT_DIR={data_root / config["fastopendata"]["processed_data_dir"]}
FASTOPENDATA_CACHE_DIR={data_root / config["fastopendata"]["cache_dir"]}
FASTOPENDATA_LOGS_DIR={data_root / config["fastopendata"]["logs_dir"]}

# pycypher configuration
PYCYPHER_PIPELINE_CONFIGS={data_root / config["pycypher"]["pipeline_configs"]}
PYCYPHER_GRAPH_DATA={data_root / config["pycypher"]["graph_data"]}
PYCYPHER_QUERY_CACHE={data_root / config["pycypher"]["query_cache"]}
PYCYPHER_PERFORMANCE_LOGS={data_root / config["pycypher"]["performance_logs"]}

# nmetl configuration
NMETL_PROJECT_CONFIGS={data_root / config["nmetl"]["project_configs"]}
NMETL_OUTPUT_EXPORTS={data_root / config["nmetl"]["output_exports"]}
NMETL_VALIDATION_REPORTS={data_root / config["nmetl"]["validation_reports"]}

# Environment-specific settings
ENVIRONMENT={environment}
"""

    if environment in config:
        env_config = config[environment]
        env_content += f"""
# {environment.title()} environment settings
MAX_DOWNLOAD_SIZE={env_config.get('max_download_size', 'unlimited')}
SAMPLE_DATA={str(env_config.get('sample_data', False)).lower()}
DEBUG_LOGGING={str(env_config.get('debug_logging', False)).lower()}
"""
        if 'parallel_workers' in env_config:
            env_content += f"PARALLEL_WORKERS={env_config['parallel_workers']}\n"

    env_file = base_dir / ".env"
    with open(env_file, "w") as f:
        f.write(env_content)

    print(f"Generated: {env_file}")


def main():
    parser = argparse.ArgumentParser(description="Setup pycypher-nmetl workspace")
    parser.add_argument(
        "--config",
        type=Path,
        default="workspace.toml",
        help="Path to workspace configuration file"
    )
    parser.add_argument(
        "--env",
        choices=["development", "production"],
        default="development",
        help="Environment configuration to use"
    )

    args = parser.parse_args()

    if not args.config.exists():
        print(f"Error: Configuration file {args.config} not found")
        return 1

    try:
        config = load_workspace_config(args.config)
        create_directories(config, args.env)
        generate_env_file(config, args.env)

        print(f"\nWorkspace setup complete for {args.env} environment!")
        print(f"Source environment variables: source .env")
        print(f"Run Snakefile: cd packages/fastopendata && snakemake --cores 4")

    except Exception as e:
        print(f"Error setting up workspace: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())