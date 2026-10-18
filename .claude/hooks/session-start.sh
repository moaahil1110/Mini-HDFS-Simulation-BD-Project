#!/bin/bash
# SessionStart hook: installs Python dependencies for Claude Code cloud sessions.
set -euo pipefail

# Only run in remote (cloud) environments
if [ "${CLAUDE_CODE_REMOTE:-}" != "true" ]; then
  exit 0
fi

cd "${CLAUDE_PROJECT_DIR:-$(dirname "$0")/../..}"

# Use a project virtualenv so pip doesn't clash with Debian-managed system packages
if [ ! -x .venv/bin/python ]; then
  python3 -m venv .venv
fi

PIP=(.venv/bin/python -m pip install --quiet --disable-pip-version-check)

# Runtime dependencies (Flask for the client web dashboard)
"${PIP[@]}" -r requirements.txt

# Dev tooling: test runner and linter
"${PIP[@]}" pytest ruff

# Put the venv on PATH for the rest of the session
if [ -n "${CLAUDE_ENV_FILE:-}" ]; then
  echo "export VIRTUAL_ENV=\"$PWD/.venv\"" >> "$CLAUDE_ENV_FILE"
  echo "export PATH=\"$PWD/.venv/bin:\$PATH\"" >> "$CLAUDE_ENV_FILE"
fi
