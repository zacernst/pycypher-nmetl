"""Utility for bumping the release and publishing it to Github"""

import pathlib
import subprocess
import sys

import click
import toml
from git import Repo

PYPROJECT_TOML = pathlib.Path(__file__).parent / "pyproject.toml"
INIT_FILE = pathlib.Path(__file__).parent / "src" / "pycypher" / "__init__.py"


class Version:
    """Simple data class"""

    def __init__(self, version_string: str):
        self.major, self.minor, self.micro = map(
            int, version_string.split(".")
        )

    def __str__(self) -> str:
        return f"{self.major}.{self.minor}.{self.micro}"

    def increment(self, increment) -> None:
        """Increment the version"""
        if increment == "major":
            self.major += 1
            self.minor = 0
            self.micro = 0
        elif increment == "minor":
            self.minor += 1
            self.micro = 0
        elif increment == "micro":
            self.micro += 1
        else:
            raise ValueError("Invalid increment value")


def get_version() -> Version:
    """Return the current version from the pyproject file"""
    pyproject_toml = toml.load(PYPROJECT_TOML)
    current_version = pyproject_toml["project"]["version"]

    return Version(current_version)


def write_version(version: Version) -> None:
    """Rewrite the pyproject and __init__.py files with the new version"""
    pyproject_toml = toml.load(PYPROJECT_TOML)
    pyproject_toml["project"]["version"] = str(version)

    with open(PYPROJECT_TOML, "w", encoding="utf8") as f:
        toml.dump(pyproject_toml, f)

    with open(INIT_FILE, "w", encoding="utf8") as f:
        f.write(f'__version__ = "{version}"\n')

    click.echo(f"Version updated to {version}")


@click.command()
@click.option("--increment", default="micro", help="Increment version")
@click.option("--version", default=None, help="Set to specific version")
@click.option(
    "--dry-run", is_flag=True, help="Bump and build, but do not publish."
)
@click.option("--confirm", is_flag=True, help="Confirm before publishing.")
def release(increment, dry_run, confirm, version) -> None:
    """Bump the release version"""
    if increment not in ["major", "minor", "micro"]:
        click.echo(
            f"Invalid increment value: {increment}. Must be major, minor, or micro"
        )
        sys.exit(1)

    if version:
        next_version = Version(version)
    else:
        current_version = get_version()
        next_version = Version(str(current_version))
        next_version.increment(increment)

    click.echo(f"Bumping version from {current_version} --> {next_version}")

    if confirm and not click.confirm("Continue?"):
        sys.exit(0)

    write_version(next_version)

    if dry_run:
        click.echo("Dry run complete. Version updated, but not published.")
        sys.exit(0)

    click.echo("Building and publishing to Github")
    result = subprocess.call("make build", shell=True)
    if result != 0:
        click.echo("Build failed. Aborting.")
        sys.exit(1)

    # Publish to Github
    repo = Repo(".")
    repo.git.add(".")
    repo.git.add("./dist")
    repo.index.commit(f"Release {next_version}")
    repo.create_tag(f"v{next_version}", message=f"Release {next_version}")
    repo.remote().push()


# from github import Github

# Authenticate with GitHub
# g = Github("YOUR_GITHUB_ACCESS_TOKEN")
#
# # Get the repository
# repo = g.get_repo("YOUR_USERNAME/YOUR_REPOSITORY")
#
# # Create the release
# release = repo.create_git_release(
#     tag_name="v1.0.0",  # Replace with your tag name
#     name="Release v1.0.0",  # Replace with your release title
#     body="Release notes",  # Replace with your release description
#     draft=False,  # Set to True for a draft release
#     prerelease=False  # Set to True for a pre-release
# )
#
# # Optionally, upload assets
# release.upload_asset("path/to/asset.zip")

if __name__ == "__main__":
    release()  # pylint: disable=no-value-for-parameter
