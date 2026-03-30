"""FastOpenData configuration management.

This module provides centralized configuration for the fastopendata project,
reading from config.toml and handling environment variable overrides.

Usage:
    from fastopendata.config import config

    # Access configuration values
    data_dir = config.data_dir
    datasets = config.datasets
    state_fips = config.state_fips

    # Get resolved paths
    output_path = config.get_dataset_path("acs_pums_1yr_persons")
    scripts_path = config.scripts_path
"""

from __future__ import annotations

import functools
import os
import tomllib
from pathlib import Path
from typing import Any

__all__ = ["Config", "DatasetConfig", "config"]


class DatasetConfig:
    """Configuration for a single dataset.

    Wraps the raw TOML dict for a ``[datasets.<name>]`` section and
    exposes typed, cached properties for each field.  Optional fields
    return *None* when the key is absent from the configuration.
    """

    def __init__(self, name: str, data: dict[str, Any]) -> None:
        self.name = name
        self._data = data

    @property
    def zips(self) -> list[str]:
        """ZIP archive filenames associated with this dataset."""
        return self._data.get("zips", [])

    @property
    def id(self) -> int:
        """Numeric identifier for ordering in the dataset catalogue."""
        return self._data["id"]

    @property
    def display_name(self) -> str:
        """Human-readable dataset title from config."""
        return self._data["name"]

    @property
    def output_file(self) -> str | None:
        """Relative path to the single output file, or *None*."""
        return self._data.get("output_file")

    @property
    def output_dir(self) -> str | None:
        """Relative path to the output directory, or *None* for single-file datasets."""
        return self._data.get("output_dir")

    @property
    def output_pattern(self) -> str | None:
        """Glob pattern matching output files (e.g. per-state shapefiles)."""
        return self._data.get("output_pattern")

    @property
    def url(self) -> str | None:
        """Direct download URL, or *None* if the dataset uses a URL pattern."""
        return self._data.get("url")

    @property
    def url_pattern(self) -> str | None:
        """Parameterised URL template, or *None* for fixed URLs."""
        return self._data.get("url_pattern")

    @property
    def data_url(self) -> str | None:
        """Primary data file URL for datasets with separate data/labels downloads."""
        return self._data.get("data_url")

    @property
    def labels_url(self) -> str | None:
        """Value-labels file URL, or *None* if the dataset has no separate labels."""
        return self._data.get("labels_url")

    @property
    def input_file(self) -> str | None:
        """Path to an upstream input file consumed by a processing script."""
        return self._data.get("input_file")

    @property
    def format(self) -> str:
        """Primary file format of the raw data (e.g. ``'csv'``, ``'shapefile'``)."""
        return self._data["format"]

    @property
    def source(self) -> str:
        """Originating organisation or project (e.g. ``'U.S. Census Bureau'``)."""
        return self._data["source"]

    @property
    def year(self) -> int | None:
        """Reference year for the dataset, or *None* if unversioned."""
        return self._data.get("year")

    @property
    def license(self) -> str | None:
        """License identifier (e.g. ``'public domain'``, ``'ODbL-1.0'``)."""
        return self._data.get("license")

    @property
    def approx_size(self) -> str:
        """Human-readable approximate download size (e.g. ``'~300 MB'``)."""
        return self._data["approx_size"]

    @property
    def description(self) -> str:
        """One-line description of the dataset contents."""
        return self._data["description"]

    def is_parametric(self) -> bool:
        """Return *True* if this dataset uses a URL pattern instead of a fixed URL."""
        return self.url_pattern is not None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary with all metadata fields for API responses.

        Optional fields that are *None* are omitted from the result.
        """
        result: dict[str, Any] = {
            "name": self.name,
            "id": self.id,
            "display_name": self.display_name,
            "description": self.description,
            "format": self.format,
            "source": self.source,
            "approx_size": self.approx_size,
        }
        for key in (
            "url",
            "url_pattern",
            "data_url",
            "labels_url",
            "output_file",
            "output_dir",
            "output_pattern",
            "input_file",
            "year",
            "license",
        ):
            value = self._data.get(key)
            if value is not None:
                result[key] = value
        result["is_parametric"] = self.is_parametric()
        return result


class Config:
    """FastOpenData configuration manager."""

    def __init__(self, config_file: Path | None = None) -> None:
        if config_file is None:
            # Default to config.toml in the fastopendata package directory
            package_root = Path(__file__).parent.parent.parent
            config_file = package_root / "config.toml"

        self._config_file = Path(config_file)
        self._data = self._load_config()

    def _load_config(self) -> dict[str, Any]:
        """Load configuration from TOML file."""
        if not self._config_file.exists():
            msg = f"Configuration file not found: {self._config_file}"
            raise FileNotFoundError(msg)

        with self._config_file.open("rb") as f:
            try:
                data = tomllib.load(f)
            except tomllib.TOMLDecodeError as e:
                msg = f"Malformed TOML in {self._config_file}: {e}"
                raise ValueError(msg) from e

        self._validate_config(data)
        return data

    def _validate_config(self, data: dict[str, Any]) -> None:
        """Validate required sections, keys, types, and value constraints."""
        self._validate_required_structure(data)
        self._validate_value_constraints(data)

    @staticmethod
    def _validate_required_structure(data: dict[str, Any]) -> None:
        """Check that all required sections and keys are present."""
        required_sections = [
            "paths", "downloads", "datasets", "api", "processing", "logging",
        ]
        missing = [s for s in required_sections if s not in data]
        if missing:
            msg = f"Missing required config sections: {', '.join(missing)}"
            raise ValueError(msg)

        required_paths = ["data_dir", "scripts_dir", "temp_dir", "static_dir"]
        missing_paths = [k for k in required_paths if k not in data.get("paths", {})]
        if missing_paths:
            msg = f"Missing required paths keys: {', '.join(missing_paths)}"
            raise ValueError(msg)

        for key in required_paths:
            val = data["paths"][key]
            if not isinstance(val, str) or not val.strip():
                msg = f"paths.{key} must be a non-empty string, got {val!r}"
                raise ValueError(msg)

        required_downloads = ["max_concurrent", "max_retries", "timeout"]
        missing_dl = [
            k for k in required_downloads if k not in data.get("downloads", {})
        ]
        if missing_dl:
            msg = f"Missing required downloads keys: {', '.join(missing_dl)}"
            raise ValueError(msg)

    def _validate_value_constraints(self, data: dict[str, Any]) -> None:
        """Check numeric bounds, port range, and log level validity."""
        self._validate_positive_int(data["downloads"], "max_concurrent", "downloads")
        self._validate_positive_int(data["downloads"], "max_retries", "downloads")
        self._validate_positive_int(data["downloads"], "timeout", "downloads")

        if "processing" in data:
            proc = data["processing"]
            self._validate_positive_int(proc, "max_memory_gb", "processing")
            self._validate_positive_int(proc, "max_workers", "processing")
            self._validate_positive_int(proc, "chunk_size", "processing")

        if "api" in data and "port" in data["api"]:
            port = data["api"]["port"]
            if not isinstance(port, int) or port < 1 or port > 65535:
                msg = f"api.port must be an integer between 1 and 65535, got {port!r}"
                raise ValueError(msg)

        if "logging" in data and "level" in data["logging"]:
            valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
            level = data["logging"]["level"]
            if not isinstance(level, str) or level.upper() not in valid_levels:
                msg = f"logging.level must be one of {valid_levels}, got {level!r}"
                raise ValueError(msg)

    @staticmethod
    def _validate_positive_int(
        section: dict[str, Any],
        key: str,
        section_name: str,
    ) -> None:
        """Raise ValueError if section[key] is not a positive integer."""
        if key not in section:
            return
        val = section[key]
        if not isinstance(val, int) or val <= 0:
            msg = f"{section_name}.{key} must be a positive integer, got {val!r}"
            raise ValueError(msg)

    # Path configuration with environment variable overrides

    # Directories that should never be used as a data directory.
    _FORBIDDEN_DATA_DIRS: frozenset[str] = frozenset({
        "/", "/bin", "/boot", "/dev", "/etc", "/lib", "/lib64",
        "/proc", "/root", "/sbin", "/sys", "/usr", "/var",
    })

    # Characters that are dangerous when interpolated into shell commands.
    _SHELL_METACHARACTERS: frozenset[str] = frozenset(";&|`$(){}!#~")

    @staticmethod
    def _validate_data_dir(path_str: str) -> str:
        """Validate that a data directory path does not escape expected bounds.

        Resolves the path (expanding ~, symlinks, ..) and rejects system-critical
        directories and paths containing shell metacharacters to prevent injection
        when the value is interpolated into shell commands (e.g. Snakefile rules).

        Args:
            path_str: The raw data directory path string.

        Returns:
            The resolved, validated path as a string.

        Raises:
            ValueError: If the resolved path is a forbidden system directory or
                contains dangerous shell metacharacters.
        """
        bad_chars = Config._SHELL_METACHARACTERS.intersection(path_str)
        if bad_chars:
            msg = (
                f"DATA_DIR contains shell metacharacters {bad_chars!r}: {path_str!r}. "
                "Paths must not contain characters that could enable shell injection."
            )
            raise ValueError(msg)
        resolved = str(Path(path_str).expanduser().resolve())
        if resolved in Config._FORBIDDEN_DATA_DIRS:
            msg = (
                f"DATA_DIR resolved to forbidden system directory: {resolved!r}. "
                f"Choose a directory outside system paths."
            )
            raise ValueError(msg)
        for forbidden in Config._FORBIDDEN_DATA_DIRS:
            # Block direct children of critical system dirs (e.g. /etc/shadow_dir)
            # but allow deeper user-controlled paths (e.g. /var/lib/myapp/data)
            if forbidden != "/" and resolved.startswith(forbidden + "/"):
                parts_after = resolved[len(forbidden) + 1:]
                if "/" not in parts_after:
                    msg = (
                        f"DATA_DIR resolved to a direct child of system directory "
                        f"{forbidden!r}: {resolved!r}. Use a deeper subdirectory."
                    )
                    raise ValueError(msg)
        return resolved

    @property
    def data_dir(self) -> str:
        """Data directory path (can be overridden by DATA_DIR environment variable).

        The path is resolved and validated to prevent path traversal into
        system-critical directories.
        """
        raw = os.environ.get("DATA_DIR", self._data["paths"]["data_dir"])
        return self._validate_data_dir(raw)

    @property
    def short_timeout_seconds(self) -> int:
        return self._data["downloads"]["short_timeout_seconds"]

    @property
    def long_timeout_seconds(self) -> int:
        return self._data["downloads"]["long_timeout_seconds"]


    @property
    def scripts_dir(self) -> str:
        """Scripts directory path."""
        return self._data["paths"]["scripts_dir"]

    @property
    def temp_dir(self) -> str:
        """Temporary directory path."""
        return self._data["paths"]["temp_dir"]

    @property
    def static_dir(self) -> str:
        """Static files directory path."""
        return self._data["paths"]["static_dir"]

    # Resolved path properties

    @property
    def data_path(self) -> Path:
        """Resolved data directory path."""
        return Path(self.data_dir).resolve()

    @property
    def scripts_path(self) -> Path:
        """Resolved scripts directory path."""
        package_root = Path(__file__).parent.parent.parent
        return (package_root / self.scripts_dir).resolve()

    @property
    def temp_path(self) -> Path:
        """Resolved temporary directory path."""
        return (self.data_path / self.temp_dir).resolve()

    # Download configuration

    @property
    def max_concurrent_downloads(self) -> int:
        """Maximum concurrent downloads."""
        return self._data["downloads"]["max_concurrent"]

    @property
    def max_download_retries(self) -> int:
        """Maximum download retry attempts."""
        return self._data["downloads"]["max_retries"]

    @property
    def download_timeout(self) -> int:
        """Download timeout in seconds."""
        return self._data["downloads"]["timeout"]

    @property
    def census_user_agent(self) -> str:
        """User agent for Census Bureau endpoints."""
        return self._data["downloads"]["census_user_agent"]

    @property
    def census_referer(self) -> str:
        """Referer header for Census Bureau endpoints."""
        return self._data["downloads"]["census_referer"]

    # Dataset configuration

    @functools.cached_property
    def datasets(self) -> dict[str, DatasetConfig]:
        """Dataset configurations."""
        return {
            name: DatasetConfig(name, data)
            for name, data in self._data["datasets"].items()
        }

    def get_dataset(self, name: str) -> DatasetConfig:
        """Get dataset configuration by name."""
        if name not in self.datasets:
            msg = f"Unknown dataset: {name}"
            raise KeyError(msg)
        return self.datasets[name]

    def get_dataset_path(self, dataset_name: str) -> Path:
        """Get full path for a dataset's output file."""
        dataset = self.get_dataset(dataset_name)
        if dataset.output_file:
            return self.data_path / dataset.output_file
        if dataset.output_dir:
            return self.data_path / dataset.output_dir
        msg = f"Dataset {dataset_name} has no output_file or output_dir"
        raise ValueError(msg)

    def get_dataset_url(self, dataset_name: str, **kwargs: str) -> str:
        """Get URL for a dataset, with optional formatting."""
        dataset = self.get_dataset(dataset_name)
        if dataset.url:
            return dataset.url
        if dataset.url_pattern:
            return dataset.url_pattern.format(**kwargs)
        if dataset.data_url:
            return dataset.data_url
        msg = f"Dataset {dataset_name} has no URL configured"
        raise ValueError(msg)

    # Geography configuration

    @property
    def state_fips(self) -> list[str]:
        """List of state and territory FIPS codes."""
        return self._data["geography"]["state_fips"]

    @property
    def zips(self) -> list[str]:
        """List of county-level FIPS codes for TIGER/Line address feature downloads."""
        return self._data["geography"]["zips"]

    # API configuration

    @property
    def api_title(self) -> str:
        """Display title for the FastAPI application."""
        return self._data["api"]["title"]

    @property
    def api_description(self) -> str:
        """Description string shown in the OpenAPI docs."""
        return self._data["api"]["description"]

    @property
    def api_version(self) -> str:
        """Semantic version reported by the API health endpoint."""
        return self._data["api"]["version"]

    @property
    def api_host(self) -> str:
        """Bind address for the API server (e.g. ``'0.0.0.0'``)."""
        return self._data["api"]["host"]

    @property
    def api_port(self) -> int:
        """TCP port for the API server (1-65535)."""
        return self._data["api"]["port"]

    @property
    def api_debug(self) -> bool:
        """Whether the API runs in debug mode with auto-reload."""
        return self._data["api"]["debug"]

    # Processing configuration

    @property
    def max_memory_gb(self) -> int:
        """Memory cap in GiB for data-processing tasks."""
        return self._data["processing"]["max_memory_gb"]

    @property
    def cleanup_temp_files(self) -> bool:
        """Whether to delete intermediate files after pipeline completion."""
        return self._data["processing"]["cleanup_temp_files"]

    @property
    def max_workers(self) -> int:
        """Maximum parallel workers for multiprocessing pipelines."""
        return self._data["processing"]["max_workers"]

    @property
    def chunk_size(self) -> int:
        """Row-level chunk size for streaming CSV reads."""
        return self._data["processing"]["chunk_size"]

    # Logging configuration

    @property
    def log_level(self) -> str:
        """Python logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)."""
        return self._data["logging"]["level"]

    @property
    def log_format(self) -> str:
        """Format string for log messages."""
        return self._data["logging"]["format"]

    @property
    def log_file(self) -> str:
        """Path to the log output file."""
        return self._data["logging"]["file"]

    # Utility methods

    def get_census_wget_flags(self) -> str:
        """Get wget flags for Census Bureau downloads."""
        return (
            f"wget"
            f" --wait=10"
            f" --user-agent='{self.census_user_agent}'"
            f" --no-cache"
            f" --header='referer: {self.census_referer}'"
        )

    def create_data_directory(self) -> None:
        """Create the data directory if it doesn't exist."""
        self.data_path.mkdir(parents=True, exist_ok=True)

    def create_temp_directory(self) -> None:
        """Create the temporary directory if it doesn't exist."""
        self.temp_path.mkdir(parents=True, exist_ok=True)


# Global configuration instance
config = Config()
