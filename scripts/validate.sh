#!/usr/bin/env bash
# validate.sh — Compatibility wrapper around the typed Python validator.
set -euo pipefail

exec uv run python scripts/validate.py "$@"
