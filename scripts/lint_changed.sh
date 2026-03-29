#!/usr/bin/env bash
# lint_changed.sh — Lint only files changed relative to a base ref.
#
# Usage:
#   ./scripts/lint_changed.sh [base_ref]
#
# If base_ref is omitted, defaults to origin/main.
# Exit code is non-zero if any changed .py file has lint violations.

set -euo pipefail

BASE_REF="${1:-origin/main}"

# Get list of changed Python files (Added, Modified, Renamed)
changed_files=$(git diff --name-only --diff-filter=AMR "$BASE_REF" -- '*.py' || true)

if [ -z "$changed_files" ]; then
    echo "No changed Python files to lint."
    exit 0
fi

file_count=$(echo "$changed_files" | wc -l | tr -d ' ')
echo "Linting $file_count changed Python file(s) vs $BASE_REF..."

# Run ruff on changed files only (blocking — must pass for PRs)
# shellcheck disable=SC2086
uv run ruff check $changed_files
