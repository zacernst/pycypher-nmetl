"""Shared test fixtures for pycypher-tui tests.

Provides pre-configured PyCypherTUI app instances loaded with
template configs, avoiding repetitive setup across test files.
"""

from __future__ import annotations

import pytest

from pycypher_tui.app import PyCypherTUI
from pycypher_tui.config.pipeline import ConfigManager
from pycypher_tui.config.templates import get_template


@pytest.fixture
def ecommerce_app() -> PyCypherTUI:
    """PyCypherTUI app pre-loaded with the ecommerce template config."""
    t = get_template("ecommerce_pipeline")
    config = t.instantiate(project_name="test_shop", data_dir="data")
    app = PyCypherTUI()
    app._config_manager = ConfigManager.from_config(config)
    return app


@pytest.fixture
def csv_analytics_app() -> PyCypherTUI:
    """PyCypherTUI app pre-loaded with the csv_analytics template config."""
    t = get_template("csv_analytics")
    config = t.instantiate(project_name="test_analytics", data_dir="data")
    app = PyCypherTUI()
    app._config_manager = ConfigManager.from_config(config)
    return app


@pytest.fixture
def social_network_app() -> PyCypherTUI:
    """PyCypherTUI app pre-loaded with the social_network template config."""
    t = get_template("social_network")
    config = t.instantiate(project_name="test_social", data_dir="data")
    app = PyCypherTUI()
    app._config_manager = ConfigManager.from_config(config)
    return app


@pytest.fixture
def empty_app() -> PyCypherTUI:
    """PyCypherTUI app with empty config."""
    app = PyCypherTUI()
    app._config_manager = ConfigManager()
    return app
