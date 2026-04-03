"""Tests for fastopendata.config module."""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from fastopendata.config import Config


@pytest.fixture
def config_file(tmp_path: Path) -> Path:
    """Write a minimal valid config.toml and return its path."""
    toml = tmp_path / "config.toml"
    toml.write_text(
        """\
[paths]
data_dir = "raw_data"
scripts_dir = "src/fastopendata/processing"
temp_dir = "tmp"
static_dir = "static"

[downloads]
max_concurrent = 4
max_retries = 3
timeout = 300
census_user_agent = "TestAgent/1.0"
census_referer = "https://example.com"

[datasets]

[datasets.test_dataset]
id = 1
name = "Test Dataset"
output_file = "test.csv"
format = "CSV"
source = "Test"
approx_size = "1 MB"
description = "A test dataset"

[datasets.test_dir_dataset]
id = 2
name = "Dir Dataset"
output_dir = "test_output"
format = "Parquet"
source = "Test"
approx_size = "5 MB"
description = "A dataset with output_dir"

[datasets.test_url_pattern]
id = 3
name = "URL Pattern Dataset"
output_file = "pattern.csv"
url_pattern = "https://example.com/{fips}/data.csv"
format = "CSV"
source = "Test"
approx_size = "10 MB"
description = "A dataset with url_pattern"

[datasets.test_data_url]
id = 4
name = "Data URL Dataset"
output_file = "data_url.csv"
data_url = "https://example.com/data.csv"
format = "CSV"
source = "Test"
approx_size = "2 MB"
description = "A dataset with data_url"

[geography]
state_fips = ["01", "02", "04"]
zips = ["10001", "10002"]

[api]
title = "Test API"
description = "Test API description"
version = "0.0.1"
host = "0.0.0.0"
port = 8000
debug = true

[processing]
max_memory_gb = 8
cleanup_temp_files = true
max_workers = 4
chunk_size = 10000

[logging]
level = "INFO"
format = "%(asctime)s %(levelname)s %(message)s"
file = "fastopendata.log"
""",
        encoding="utf-8",
    )
    return toml


@pytest.fixture
def cfg(config_file: Path) -> Config:
    """Return a Config loaded from the test fixture."""
    return Config(config_file)


# ── Basic loading ──────────────────────────────────────────────────────


class TestConfigLoading:
    def test_load_valid_config(self, cfg: Config) -> None:
        assert cfg.data_dir.endswith("/raw_data")
        assert cfg.scripts_dir == "src/fastopendata/processing"

    def test_missing_file_raises(self, tmp_path: Path) -> None:
        with pytest.raises(
            FileNotFoundError, match="Configuration file not found"
        ):
            Config(tmp_path / "nonexistent.toml")

    def test_malformed_toml_raises(self, tmp_path: Path) -> None:
        bad = tmp_path / "bad.toml"
        bad.write_text("[[invalid toml = }", encoding="utf-8")
        with pytest.raises(ValueError, match="Malformed TOML"):
            Config(bad)

    def test_missing_required_section_raises(self, tmp_path: Path) -> None:
        incomplete = tmp_path / "incomplete.toml"
        incomplete.write_text(
            """\
[paths]
data_dir = "x"
scripts_dir = "x"
temp_dir = "x"
static_dir = "x"
""",
            encoding="utf-8",
        )
        with pytest.raises(
            ValueError, match="Missing required config sections"
        ):
            Config(incomplete)

    def test_missing_paths_keys_raises(self, tmp_path: Path) -> None:
        bad_paths = tmp_path / "bad_paths.toml"
        bad_paths.write_text(
            """\
[paths]
data_dir = "x"

[downloads]
max_concurrent = 1
max_retries = 1
timeout = 10

[datasets]
[api]
title = ""
description = ""
version = ""
host = ""
port = 0
debug = false

[processing]
max_memory_gb = 1
cleanup_temp_files = false
max_workers = 1
chunk_size = 100

[logging]
level = "INFO"
format = ""
file = ""
""",
            encoding="utf-8",
        )
        with pytest.raises(ValueError, match="Missing required paths keys"):
            Config(bad_paths)

    def test_missing_downloads_keys_raises(self, tmp_path: Path) -> None:
        bad_dl = tmp_path / "bad_dl.toml"
        bad_dl.write_text(
            """\
[paths]
data_dir = "x"
scripts_dir = "x"
temp_dir = "x"
static_dir = "x"

[downloads]

[datasets]
[api]
title = ""
description = ""
version = ""
host = ""
port = 0
debug = false

[processing]
max_memory_gb = 1
cleanup_temp_files = false
max_workers = 1
chunk_size = 100

[logging]
level = "INFO"
format = ""
file = ""
""",
            encoding="utf-8",
        )
        with pytest.raises(
            ValueError, match="Missing required downloads keys"
        ):
            Config(bad_dl)


# ── Value constraint validation ────────────────────────────────────────

# Helper: valid TOML base that can be modified per-test
_VALID_BASE = """\
[paths]
data_dir = "raw_data"
scripts_dir = "src/fastopendata/processing"
temp_dir = "tmp"
static_dir = "static"

[downloads]
max_concurrent = 4
max_retries = 3
timeout = 300
census_user_agent = "TestAgent/1.0"
census_referer = "https://example.com"

[datasets]

[geography]
state_fips = ["01"]
zips = ["10001"]

[api]
title = "T"
description = "D"
version = "0.0.1"
host = "0.0.0.0"
port = 8000
debug = false

[processing]
max_memory_gb = 8
cleanup_temp_files = true
max_workers = 4
chunk_size = 10000

[logging]
level = "INFO"
format = "%(message)s"
file = "test.log"
"""


class TestValueConstraints:
    def test_zero_max_concurrent_raises(self, tmp_path: Path) -> None:
        toml = tmp_path / "c.toml"
        toml.write_text(
            _VALID_BASE.replace("max_concurrent = 4", "max_concurrent = 0"),
            encoding="utf-8",
        )
        with pytest.raises(
            ValueError,
            match="downloads.max_concurrent must be a positive integer",
        ):
            Config(toml)

    def test_negative_timeout_raises(self, tmp_path: Path) -> None:
        toml = tmp_path / "c.toml"
        toml.write_text(
            _VALID_BASE.replace("timeout = 300", "timeout = -1"),
            encoding="utf-8",
        )
        with pytest.raises(
            ValueError,
            match="downloads.timeout must be a positive integer",
        ):
            Config(toml)

    def test_zero_max_workers_raises(self, tmp_path: Path) -> None:
        toml = tmp_path / "c.toml"
        toml.write_text(
            _VALID_BASE.replace("max_workers = 4", "max_workers = 0"),
            encoding="utf-8",
        )
        with pytest.raises(
            ValueError,
            match="processing.max_workers must be a positive integer",
        ):
            Config(toml)

    def test_invalid_port_too_high_raises(self, tmp_path: Path) -> None:
        toml = tmp_path / "c.toml"
        toml.write_text(
            _VALID_BASE.replace("port = 8000", "port = 70000"),
            encoding="utf-8",
        )
        with pytest.raises(
            ValueError,
            match="api.port must be an integer between 1 and 65535",
        ):
            Config(toml)

    def test_invalid_port_zero_raises(self, tmp_path: Path) -> None:
        toml = tmp_path / "c.toml"
        toml.write_text(
            _VALID_BASE.replace("port = 8000", "port = 0"),
            encoding="utf-8",
        )
        with pytest.raises(
            ValueError,
            match="api.port must be an integer between 1 and 65535",
        ):
            Config(toml)

    def test_invalid_log_level_raises(self, tmp_path: Path) -> None:
        toml = tmp_path / "c.toml"
        toml.write_text(
            _VALID_BASE.replace('level = "INFO"', 'level = "VERBOSE"'),
            encoding="utf-8",
        )
        with pytest.raises(ValueError, match="logging.level must be one of"):
            Config(toml)

    def test_empty_path_raises(self, tmp_path: Path) -> None:
        toml = tmp_path / "c.toml"
        toml.write_text(
            _VALID_BASE.replace('data_dir = "raw_data"', 'data_dir = ""'),
            encoding="utf-8",
        )
        with pytest.raises(
            ValueError,
            match="paths.data_dir must be a non-empty string",
        ):
            Config(toml)

    def test_valid_config_passes_all_constraints(self, tmp_path: Path) -> None:
        toml = tmp_path / "c.toml"
        toml.write_text(_VALID_BASE, encoding="utf-8")
        cfg = Config(toml)
        assert cfg.max_concurrent_downloads == 4
        assert cfg.api_port == 8000
        assert cfg.log_level == "INFO"


# ── Path properties ────────────────────────────────────────────────────


class TestPaths:
    def test_data_path_is_resolved(self, cfg: Config) -> None:
        assert cfg.data_path.is_absolute()

    def test_scripts_path_is_resolved(self, cfg: Config) -> None:
        assert cfg.scripts_path.is_absolute()

    def test_temp_path_is_resolved(self, cfg: Config) -> None:
        assert cfg.temp_path.is_absolute()
        assert "tmp" in str(cfg.temp_path)

    def test_data_dir_env_override(self, config_file: Path) -> None:
        os.environ["DATA_DIR"] = "/tmp/override"
        try:
            c = Config(config_file)
            assert c.data_dir.endswith("/override")
        finally:
            del os.environ["DATA_DIR"]


# ── Download properties ────────────────────────────────────────────────


class TestDownloads:
    def test_max_concurrent(self, cfg: Config) -> None:
        assert cfg.max_concurrent_downloads == 4

    def test_max_retries(self, cfg: Config) -> None:
        assert cfg.max_download_retries == 3

    def test_timeout(self, cfg: Config) -> None:
        assert cfg.download_timeout == 300

    def test_census_user_agent(self, cfg: Config) -> None:
        assert cfg.census_user_agent == "TestAgent/1.0"

    def test_census_referer(self, cfg: Config) -> None:
        assert cfg.census_referer == "https://example.com"

    def test_wget_flags(self, cfg: Config) -> None:
        flags = cfg.get_census_wget_flags()
        assert "wget" in flags
        assert "TestAgent/1.0" in flags
        assert "https://example.com" in flags


# ── Dataset configuration ──────────────────────────────────────────────


class TestDatasets:
    def test_datasets_returns_all(self, cfg: Config) -> None:
        datasets = cfg.datasets
        assert len(datasets) == 4
        assert "test_dataset" in datasets

    def test_get_dataset(self, cfg: Config) -> None:
        ds = cfg.get_dataset("test_dataset")
        assert ds.display_name == "Test Dataset"
        assert ds.format == "CSV"
        assert ds.source == "Test"
        assert ds.id == 1

    def test_get_dataset_unknown_raises(self, cfg: Config) -> None:
        with pytest.raises(KeyError, match="Unknown dataset"):
            cfg.get_dataset("nonexistent")

    def test_get_dataset_path_output_file(self, cfg: Config) -> None:
        path = cfg.get_dataset_path("test_dataset")
        assert path.name == "test.csv"

    def test_get_dataset_path_output_dir(self, cfg: Config) -> None:
        path = cfg.get_dataset_path("test_dir_dataset")
        assert path.name == "test_output"

    def test_get_dataset_url_direct(self, cfg: Config) -> None:
        # test_data_url has data_url set
        url = cfg.get_dataset_url("test_data_url")
        assert url == "https://example.com/data.csv"

    def test_get_dataset_url_pattern(self, cfg: Config) -> None:
        url = cfg.get_dataset_url("test_url_pattern", fips="01")
        assert url == "https://example.com/01/data.csv"

    def test_get_dataset_url_missing_raises(self, cfg: Config) -> None:
        # test_dir_dataset has no URL configured
        with pytest.raises(ValueError, match="has no URL configured"):
            cfg.get_dataset_url("test_dir_dataset")

    def test_dataset_to_dict(self, cfg: Config) -> None:
        ds = cfg.get_dataset("test_dataset")
        d = ds.to_dict()
        assert d["name"] == "test_dataset"
        assert d["id"] == 1
        assert d["display_name"] == "Test Dataset"
        assert d["format"] == "CSV"
        assert d["source"] == "Test"
        assert d["approx_size"] == "1 MB"
        assert d["description"] == "A test dataset"
        assert d["output_file"] == "test.csv"
        assert d["is_parametric"] is False
        # Optional fields that are None should be omitted
        assert "url" not in d
        assert "url_pattern" not in d
        assert "year" not in d
        assert "license" not in d

    def test_dataset_to_dict_with_url_pattern(self, cfg: Config) -> None:
        ds = cfg.get_dataset("test_url_pattern")
        d = ds.to_dict()
        assert d["url_pattern"] == "https://example.com/{fips}/data.csv"
        assert d["is_parametric"] is True
        assert "url" not in d

    def test_dataset_is_parametric(self, cfg: Config) -> None:
        assert not cfg.get_dataset("test_dataset").is_parametric()
        assert cfg.get_dataset("test_url_pattern").is_parametric()

    def test_dataset_optional_fields(self, cfg: Config) -> None:
        ds = cfg.get_dataset("test_dataset")
        assert ds.output_file == "test.csv"
        assert ds.output_dir is None
        assert ds.url_pattern is None
        assert ds.year is None
        assert ds.license is None
        assert ds.input_file is None


# ── Geography ──────────────────────────────────────────────────────────


class TestGeography:
    def test_state_fips(self, cfg: Config) -> None:
        assert cfg.state_fips == ["01", "02", "04"]

    def test_zips(self, cfg: Config) -> None:
        assert cfg.zips == ["10001", "10002"]


# ── API configuration ─────────────────────────────────────────────────


class TestAPI:
    def test_api_title(self, cfg: Config) -> None:
        assert cfg.api_title == "Test API"

    def test_api_port(self, cfg: Config) -> None:
        assert cfg.api_port == 8000

    def test_api_debug(self, cfg: Config) -> None:
        assert cfg.api_debug is True


# ── Processing configuration ──────────────────────────────────────────


class TestProcessing:
    def test_max_memory(self, cfg: Config) -> None:
        assert cfg.max_memory_gb == 8

    def test_cleanup(self, cfg: Config) -> None:
        assert cfg.cleanup_temp_files is True

    def test_max_workers(self, cfg: Config) -> None:
        assert cfg.max_workers == 4

    def test_chunk_size(self, cfg: Config) -> None:
        assert cfg.chunk_size == 10000


# ── Logging ────────────────────────────────────────────────────────────


class TestLogging:
    def test_log_level(self, cfg: Config) -> None:
        assert cfg.log_level == "INFO"

    def test_log_file(self, cfg: Config) -> None:
        assert cfg.log_file == "fastopendata.log"


# ── Directory creation ─────────────────────────────────────────────────


class TestDirectoryCreation:
    def test_create_data_directory(
        self,
        cfg: Config,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("DATA_DIR", str(tmp_path / "new_data"))
        c = Config(cfg._config_file)
        c.create_data_directory()
        assert (tmp_path / "new_data").is_dir()

    def test_create_temp_directory(
        self,
        cfg: Config,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("DATA_DIR", str(tmp_path / "data"))
        c = Config(cfg._config_file)
        c.create_data_directory()
        c.create_temp_directory()
        assert (tmp_path / "data" / "tmp").is_dir()


# ── Security: _validate_data_dir ──────────────────────────────────────


class TestValidateDataDir:
    """Tests for Config._validate_data_dir shell-injection and path-traversal guards."""

    # -- Shell metacharacter rejection --

    @pytest.mark.parametrize(
        "bad_path",
        [
            "/tmp/data;rm -rf /",
            "/tmp/data|cat /etc/passwd",
            "/tmp/data`whoami`",
            "/tmp/$HOME/data",
            "/tmp/data$(id)",
            "/tmp/{evil}",
            "/tmp/data#comment",
            "/tmp/data!bang",
            "/tmp/data~expand",
            "/tmp/(subshell)",
            "/tmp/data&bg",
        ],
        ids=[
            "semicolon",
            "pipe",
            "backtick",
            "dollar",
            "dollar-paren",
            "braces",
            "hash",
            "bang",
            "tilde",
            "paren",
            "ampersand",
        ],
    )
    def test_rejects_shell_metacharacters(self, bad_path: str) -> None:
        with pytest.raises(ValueError, match="shell metacharacters"):
            Config._validate_data_dir(bad_path)

    # -- Forbidden system directory rejection --
    # Note: on macOS, /etc → /private/etc and /var → /private/var via symlinks,
    # so Path.resolve() produces paths outside the forbidden set. We only test
    # directories whose resolved form matches the input (stable across platforms).

    @pytest.mark.parametrize(
        "forbidden",
        [
            p
            for p in [
                "/",
                "/bin",
                "/boot",
                "/dev",
                "/etc",
                "/lib",
                "/lib64",
                "/proc",
                "/root",
                "/sbin",
                "/sys",
                "/usr",
                "/var",
            ]
            if str(Path(p).resolve()) == p
        ],
    )
    def test_rejects_forbidden_system_directories(
        self, forbidden: str
    ) -> None:
        with pytest.raises(ValueError, match="forbidden system directory"):
            Config._validate_data_dir(forbidden)

    # -- Direct child of system directory rejection --

    @pytest.mark.parametrize(
        "direct_child",
        [
            p
            for p in ["/usr/scratch", "/sbin/staging", "/boot/mydata"]
            if str(Path(p).parent.resolve()) == str(Path(p).parent)
        ],
    )
    def test_rejects_direct_children_of_system_dirs(
        self, direct_child: str
    ) -> None:
        with pytest.raises(
            ValueError, match="direct child of system directory"
        ):
            Config._validate_data_dir(direct_child)

    # -- Deeper subdirectories are allowed --

    def test_allows_deeper_subdirectories(self, tmp_path: Path) -> None:
        deep_path = str(tmp_path / "subdir" / "data")
        result = Config._validate_data_dir(deep_path)
        assert result == str(Path(deep_path).resolve())

    def test_allows_deep_path_under_system_dir(self) -> None:
        # Two segments deep under a system dir → allowed by the direct-child check
        test_path = "/usr/local/share/myapp"
        result = Config._validate_data_dir(test_path)
        assert result == str(Path(test_path).resolve())

    # -- Path resolution --

    def test_resolves_relative_paths(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.chdir(tmp_path)
        result = Config._validate_data_dir("relative/data/dir")
        assert result == str((tmp_path / "relative/data/dir").resolve())

    def test_returns_resolved_string(self, tmp_path: Path) -> None:
        result = Config._validate_data_dir(str(tmp_path))
        assert isinstance(result, str)
        assert result == str(tmp_path.resolve())
