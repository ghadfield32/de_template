"""
tests/conftest.py — Shared fixtures and path setup for all tests.

Adds the project root to sys.path so unit tests can import:
    from config.settings import Settings
    from scripts.render_sql import render_template, load_env
    from scripts.health import CheckResult
"""
import pathlib
import sys

# Make project root importable without installing as a package
PROJECT_ROOT = pathlib.Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
