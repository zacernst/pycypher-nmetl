#!/usr/bin/env bash
# lint_changed.sh — Lint and format-check only files changed relative to a base ref.
#
# Usage:
#   ./scripts/lint_changed.sh [base_ref]
#
# If base_ref is omitted, defaults to origin/main.
# Exit code is non-zero if any changed .py file has lint or format violations.

set -euo pipefail

BASE_REF="${1:-origin/main}"

# Get list of changed Python files (Added, Modified, Renamed)
changed_files=$(git diff --name-only --diff-filter=AMR "$BASE_REF" -- '*.py' || true)

if [ -z "$changed_files" ]; then
    echo "No changed Python files to lint."
    exit 0
fi

file_count=$(echo "$changed_files" | wc -l | tr -d ' ')
echo "Checking $file_count changed Python file(s) vs $BASE_REF..."

exit_code=0

# Check formatting (blocking)
echo "--- Format check ---"
# shellcheck disable=SC2086
if ! uv run ruff format --check $changed_files; then
    echo "FAIL: Format violations found. Run 'make format' to fix."
    exit_code=1
fi

# Lint check (blocking)
echo "--- Lint check ---"
# shellcheck disable=SC2086
if ! uv run ruff check $changed_files; then
    echo "FAIL: Lint violations found."
    exit_code=1
fi

if [ $exit_code -eq 0 ]; then
    echo "OK: All $file_count changed file(s) pass lint and format checks."
fi

exit $exit_code
