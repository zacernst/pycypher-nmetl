"""TDD tests for Compatibility Loop 255: Python version requirements consistency.

Validates that all workspace packages have consistent Python version requirements
(>=3.14.0) and classifiers (Python 3.14).
"""

import sys
import tomllib
from pathlib import Path


def _load_toml(path: str | Path) -> dict:
    """Load a TOML file using stdlib tomllib."""
    with Path(path).open("rb") as f:
        return tomllib.load(f)


class TestCurrentCompatibilityIssue:
    """Test the current Python version requirements inconsistency."""

    def test_workspace_requires_python_314(self):
        """Test that workspace requires Python >=3.14.0."""
        workspace_toml = Path("pyproject.toml")
        assert workspace_toml.exists(), "Workspace pyproject.toml should exist"

        config = _load_toml(workspace_toml)
        requires_python = config["project"]["requires-python"]

        assert requires_python == ">=3.14.0"

        # Check classifier consistency
        classifiers = config["project"]["classifiers"]
        python_classifiers = [
            c
            for c in classifiers
            if c.startswith("Programming Language :: Python ::")
        ]
        assert "Programming Language :: Python :: 3.14" in python_classifiers

    def test_packages_incorrectly_claim_broader_python_support(self):
        """Test that packages correctly require Python 3.14.0+ after Loop 255 fix."""
        package_paths = [
            Path("packages/pycypher/pyproject.toml"),
            Path("packages/shared/pyproject.toml"),
            Path("packages/fastopendata/pyproject.toml"),
        ]

        for package_path in package_paths:
            assert package_path.exists(), f"{package_path} should exist"
            config = _load_toml(package_path)

            # Verify the Loop 255 fix is in place
            requires_python = config["project"]["requires-python"]
            assert requires_python == ">=3.14.0", (
                f"{package_path} should require >=3.14.0 after Loop 255 fix"
            )

            # Verify Python classifiers are fixed after Loop 255
            classifiers = config["project"]["classifiers"]
            python_classifiers = [
                c
                for c in classifiers
                if c.startswith("Programming Language :: Python ::")
            ]

            if "pycypher" in str(package_path) or "shared" in str(
                package_path
            ):
                # After Loop 255, these should correctly have Python 3.14 classifiers
                assert (
                    "Programming Language :: Python :: 3.14"
                    in python_classifiers
                ), (
                    f"{package_path} should have Python 3.14 classifier after Loop 255 fix"
                )
                # Should NOT have incorrect older version classifiers
                assert (
                    "Programming Language :: Python :: 3.11"
                    not in python_classifiers
                ), (
                    f"{package_path} should not claim Python 3.11 support after Loop 255 fix"
                )
                assert (
                    "Programming Language :: Python :: 3.12"
                    not in python_classifiers
                ), (
                    f"{package_path} should not claim Python 3.12 support after Loop 255 fix"
                )

    def test_current_runtime_python_version_compatibility(self):
        """Test current runtime Python version against package claims."""
        # We're running on Python 3.14 but packages claim to support older versions
        current_version = sys.version_info

        # Document what we're actually running on
        assert current_version >= (3, 14), "Should be running on Python 3.14+"

        # This demonstrates the compatibility issue: packages claim >=3.12 support
        # but actually require 3.14.0a6+freethreaded

    def test_version_consistency_across_workspace(self):
        """Test that version consistency is maintained — workspace and packages agree."""
        package_config = _load_toml("packages/pycypher/pyproject.toml")
        advertised_requirement = package_config["project"]["requires-python"]

        workspace_config = _load_toml("pyproject.toml")
        actual_requirement = workspace_config["project"]["requires-python"]

        # Both workspace and packages should require >=3.14.0
        assert advertised_requirement == ">=3.14.0", (
            "Package should require >=3.14.0"
        )
        assert actual_requirement == ">=3.14.0", (
            "Workspace should require >=3.14.0"
        )


class TestFixedCompatibilityRequirements:
    """Tests for the corrected Python version requirements.

    These tests define the expected behavior after fixing the compatibility issues.
    Initially these will fail (red phase), then pass after implementation (green phase).
    """

    def test_all_packages_match_workspace_python_requirement(self):
        """Test that all packages require compatible Python versions with workspace."""
        workspace_config = _load_toml("pyproject.toml")
        workspace_requirement = workspace_config["project"]["requires-python"]

        package_paths = [
            Path("packages/pycypher/pyproject.toml"),
            Path("packages/shared/pyproject.toml"),
            Path("packages/fastopendata/pyproject.toml"),
        ]

        for package_path in package_paths:
            config = _load_toml(package_path)
            package_requirement = config["project"]["requires-python"]

            # After fix, packages should require >=3.14.0 which is compatible
            # with workspace requirement ==3.14.0a6+freethreaded
            assert package_requirement == ">=3.14.0", (
                f"{package_path} should require >=3.14.0 for compatibility"
            )

    def test_all_packages_have_consistent_python_classifiers(self):
        """Test that all packages have consistent Python version classifiers."""
        package_paths = [
            Path("packages/pycypher/pyproject.toml"),
            Path("packages/shared/pyproject.toml"),
            Path("packages/fastopendata/pyproject.toml"),
        ]

        for package_path in package_paths:
            config = _load_toml(package_path)
            classifiers = config["project"]["classifiers"]
            python_classifiers = [
                c
                for c in classifiers
                if c.startswith("Programming Language :: Python ::")
            ]

            # After fix, should only claim 3.14 support
            assert (
                "Programming Language :: Python :: 3.14" in python_classifiers
            ), f"{package_path} should claim Python 3.14 support"

            # Should not claim older version support that doesn't match requirements
            assert (
                "Programming Language :: Python :: 3.11"
                not in python_classifiers
            ), (
                f"{package_path} should not claim Python 3.11 support when requiring 3.14"
            )
            assert (
                "Programming Language :: Python :: 3.12"
                not in python_classifiers
            ), (
                f"{package_path} should not claim Python 3.12 support when requiring 3.14"
            )

    def test_package_requirements_match_actual_runtime_needs(self):
        """Test that package requirements reflect actual runtime needs."""
        # We know the code requires 3.14.0a6+freethreaded to run
        current_version = sys.version_info
        assert current_version >= (3, 14), "Running on Python 3.14+"

        # After fix, packages should declare this accurately
        package_paths = [
            Path("packages/pycypher/pyproject.toml"),
            Path("packages/shared/pyproject.toml"),
            Path("packages/fastopendata/pyproject.toml"),
        ]

        for package_path in package_paths:
            config = _load_toml(package_path)
            requires_python = config["project"]["requires-python"]

            # Should require the actual Python version needed
            assert "3.14" in requires_python, (
                f"{package_path} should require Python 3.14 to match runtime needs"
            )

    def test_no_misleading_backward_compatibility_claims(self):
        """Test that packages don't mislead users about backward compatibility."""
        package_paths = [
            Path("packages/pycypher/pyproject.toml"),
            Path("packages/shared/pyproject.toml"),
            Path("packages/fastopendata/pyproject.toml"),
        ]

        for package_path in package_paths:
            config = _load_toml(package_path)
            package_requirement = config["project"]["requires-python"]

            # Packages should not claim support for older versions
            assert "3.14" in package_requirement, (
                f"{package_path} should require Python 3.14+ to avoid misleading users"
            )
            assert not package_requirement.startswith(">=3.12"), (
                f"{package_path} should not claim Python 3.12 compatibility"
            )
            assert not package_requirement.startswith(">=3.11"), (
                f"{package_path} should not claim Python 3.11 compatibility"
            )

    def test_consistent_metadata_across_entire_workspace(self):
        """Test that Python version metadata is consistent across workspace."""
        workspace_config = _load_toml("pyproject.toml")
        workspace_classifiers = workspace_config["project"]["classifiers"]
        workspace_python_classifiers = [
            c
            for c in workspace_classifiers
            if c.startswith("Programming Language :: Python ::")
        ]

        package_paths = [
            Path("packages/pycypher/pyproject.toml"),
            Path("packages/shared/pyproject.toml"),
            Path("packages/fastopendata/pyproject.toml"),
        ]

        for package_path in package_paths:
            config = _load_toml(package_path)

            # Python requirements should be compatible (>=3.14.0)
            package_requirement = config["project"]["requires-python"]
            assert package_requirement == ">=3.14.0"

            # Python classifiers should be consistent with workspace
            package_classifiers = config["project"]["classifiers"]
            package_python_classifiers = [
                c
                for c in package_classifiers
                if c.startswith("Programming Language :: Python ::")
            ]

            # Should have same Python version classifiers as workspace (3.14)
            assert set(package_python_classifiers) == set(
                workspace_python_classifiers
            ), (
                f"{package_path} should have same Python classifiers as workspace"
            )
