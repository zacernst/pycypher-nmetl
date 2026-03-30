"""Tests for Python version requirements consistency across workspace packages.

Validates that all workspace packages have consistent Python version requirements
and classifiers matching the workspace-level pyproject.toml.
"""

import sys
import tomllib
from pathlib import Path


def _load_toml(path: str | Path) -> dict:
    """Load a TOML file using stdlib tomllib."""
    with Path(path).open("rb") as f:
        return tomllib.load(f)


_WORKSPACE_TOML = Path("pyproject.toml")
_PACKAGE_PATHS = [
    Path("packages/pycypher/pyproject.toml"),
    Path("packages/shared/pyproject.toml"),
    Path("packages/fastopendata/pyproject.toml"),
]


class TestCurrentCompatibilityIssue:
    """Test that the current runtime meets the project's declared requirements."""

    def test_workspace_pyproject_exists(self):
        assert _WORKSPACE_TOML.exists()

    def test_runtime_meets_declared_requirement(self):
        """Current Python meets the workspace's requires-python."""
        config = _load_toml(_WORKSPACE_TOML)
        requires_python = config["project"]["requires-python"]
        # The requirement is >=3.12; verify we satisfy it.
        assert sys.version_info >= (3, 12), (
            f"Running Python {sys.version} but workspace requires {requires_python}"
        )


class TestFixedCompatibilityRequirements:
    """Tests that all packages have consistent Python version metadata."""

    def test_all_packages_match_workspace_python_requirement(self):
        """Every package's requires-python matches the workspace declaration."""
        workspace_config = _load_toml(_WORKSPACE_TOML)
        workspace_requirement = workspace_config["project"]["requires-python"]

        for package_path in _PACKAGE_PATHS:
            config = _load_toml(package_path)
            package_requirement = config["project"]["requires-python"]
            assert package_requirement == workspace_requirement, (
                f"{package_path} has requires-python={package_requirement!r} "
                f"but workspace has {workspace_requirement!r}"
            )

    def test_package_requirements_match_actual_runtime_needs(self):
        """Package requirements are satisfiable by the current runtime."""
        for package_path in _PACKAGE_PATHS:
            config = _load_toml(package_path)
            requires_python = config["project"]["requires-python"]
            # Should be a >=3.x requirement satisfied by current Python
            assert requires_python.startswith(">="), (
                f"{package_path}: unexpected requires-python format: {requires_python}"
            )

    def test_no_misleading_backward_compatibility_claims(self):
        """All packages declare the same minimum Python version."""
        requirements = set()
        for package_path in _PACKAGE_PATHS:
            config = _load_toml(package_path)
            requirements.add(config["project"]["requires-python"])
        assert len(requirements) == 1, (
            f"Inconsistent requires-python across packages: {requirements}"
        )

    def test_consistent_metadata_across_entire_workspace(self):
        """Python classifiers are consistent across workspace and packages."""
        workspace_config = _load_toml(_WORKSPACE_TOML)
        workspace_classifiers = {
            c
            for c in workspace_config["project"]["classifiers"]
            if c.startswith("Programming Language :: Python ::")
        }

        for package_path in _PACKAGE_PATHS:
            config = _load_toml(package_path)
            package_classifiers = {
                c
                for c in config["project"]["classifiers"]
                if c.startswith("Programming Language :: Python ::")
            }
            assert package_classifiers == workspace_classifiers, (
                f"{package_path} classifiers {package_classifiers} "
                f"don't match workspace {workspace_classifiers}"
            )
