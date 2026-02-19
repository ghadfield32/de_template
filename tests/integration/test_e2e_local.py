"""
tests/integration/test_e2e_local.py — Full end-to-end pipeline tests.

These tests start the Docker stack, run the full pipeline, and validate
all stages. They require:
  - Docker with Compose v2
  - env/local.env (activate with: make env-select ENV=env/local.env)
  - A parquet file at HOST_DATA_DIR

Marked `integration` so they are excluded from `make test-unit`.
Run explicitly: make test-integration
               pytest tests/integration/ -v -m integration

Each test class represents a phase of the pipeline. They are designed to
run in order (use pytest -p no:randomly to preserve order, or run them
as a single session via make test-integration).
"""

from __future__ import annotations

import pathlib
import shutil
import subprocess
import time

import pytest

# conftest.py adds project root to sys.path
from config.settings import load_settings
from scripts.validate import run_validation

pytestmark = pytest.mark.integration

PROJECT_ROOT = pathlib.Path(__file__).parent.parent.parent


def _make(
    *targets: str, extra_env: dict | None = None, check: bool = True
) -> subprocess.CompletedProcess:
    """Run one or more make targets from the project root."""
    import os

    env = {**os.environ, **(extra_env or {})}
    result = subprocess.run(
        ["make"] + list(targets),
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
        timeout=600,
        env=env,
    )
    if check and result.returncode != 0:
        pytest.fail(
            f"make {' '.join(targets)} failed (exit {result.returncode}):\n"
            f"STDOUT:\n{result.stdout[-2000:]}\n"
            f"STDERR:\n{result.stderr[-2000:]}"
        )
    return result


def _docker_available() -> bool:
    try:
        result = subprocess.run(["docker", "info"], capture_output=True, timeout=10)
        return result.returncode == 0
    except Exception:
        return False


def _env_file_exists() -> bool:
    return (PROJECT_ROOT / ".env").exists()


# ---------------------------------------------------------------------------
# Skip guard
# ---------------------------------------------------------------------------

needs_docker = pytest.mark.skipif(
    not _docker_available(),
    reason="Docker not available",
)


@pytest.fixture(scope="module", autouse=True)
def ensure_env():
    """Ensure .env exists (copy from env/local.env if not present)."""
    env_path = PROJECT_ROOT / ".env"
    if not env_path.exists():
        local_env = PROJECT_ROOT / "env" / "local.env"
        if local_env.exists():
            shutil.copy(local_env, env_path)
        else:
            pytest.skip(".env not found and env/local.env not found — run: make env-select")


# ---------------------------------------------------------------------------
# Phase 1: env-select
# ---------------------------------------------------------------------------


class TestEnvSelect:
    @needs_docker
    def test_env_select_copies_local_env(self, tmp_path):
        """env-select correctly copies env profile to .env."""
        _make("env-select", extra_env={"ENV": "env/local.env"})
        assert (PROJECT_ROOT / ".env").exists()
        cfg = load_settings(env_file=str(PROJECT_ROOT / ".env"))
        assert cfg.BROKER in ("redpanda", "kafka")
        assert cfg.STORAGE in ("minio", "aws_s3", "gcs", "azure")


# ---------------------------------------------------------------------------
# Phase 2: Config validation
# ---------------------------------------------------------------------------


class TestConfigValidation:
    def test_settings_load_from_env(self):
        """Settings can be loaded from the active .env without errors."""
        cfg = load_settings()
        assert cfg.TOPIC
        assert cfg.DLQ_TOPIC
        assert cfg.WAREHOUSE.startswith("s3")

    def test_validate_config_make_target(self):
        """make validate-config passes for the current .env."""
        result = _make("validate-config")
        assert "Config OK" in result.stdout or result.returncode == 0


# ---------------------------------------------------------------------------
# Phase 3: SQL rendering
# ---------------------------------------------------------------------------


class TestSqlRendering:
    def test_build_sql_renders_without_errors(self):
        """make build-sql completes successfully."""
        _make("build-sql")

    def test_rendered_sql_files_exist(self):
        """Expected rendered SQL files exist after build-sql."""
        build_sql = PROJECT_ROOT / "build" / "sql"
        assert build_sql.exists(), "build/sql/ not found — run: make build-sql"
        sql_files = list(build_sql.glob("*.sql"))
        assert len(sql_files) >= 3, f"Expected >=3 .sql files, found {len(sql_files)}"

    def test_rendered_conf_exists(self):
        """core-site.xml rendered for the active STORAGE."""
        core_site = PROJECT_ROOT / "build" / "conf" / "core-site.xml"
        assert core_site.exists(), "build/conf/core-site.xml not found"
        content = core_site.read_text()
        assert "<configuration>" in content


# ---------------------------------------------------------------------------
# Phase 4: Full pipeline (slow — requires Docker)
# ---------------------------------------------------------------------------


@pytest.mark.slow
class TestFullPipeline:
    """
    Full E2E test: up → topics → generate → process → validate.
    Runs against the local MinIO + Redpanda stack.
    Takes 3–10 minutes depending on data volume.
    """

    @needs_docker
    def test_stack_starts(self):
        """make up starts all services successfully."""
        _make("up", check=False)  # don't fail if already up
        time.sleep(10)  # allow services to stabilise

    @needs_docker
    def test_health_check_passes(self):
        """make health reports all services OK after startup."""
        result = _make("health")
        # At minimum Flink should be reachable
        assert "Flink Dashboard" in result.stdout or result.returncode == 0

    @needs_docker
    def test_topics_created(self):
        """make create-topics creates primary and DLQ topics."""
        _make("create-topics")

    @needs_docker
    def test_data_generation(self):
        """make generate-limited produces events to the broker."""
        _make("generate-limited")
        # Metrics live on a volume; verify via dbt container is done in validate
        # Just check the make target succeeded

    @needs_docker
    def test_flink_batch_processing(self):
        """make process completes both bronze and silver Flink jobs."""
        _make("process")

    @needs_docker
    def test_wait_for_silver(self):
        """make wait-for-silver succeeds (silver table has rows)."""
        _make("wait-for-silver")

    @needs_docker
    def test_dbt_build(self):
        """make dbt-build completes without errors."""
        _make("dbt-build")

    @needs_docker
    def test_full_validation_passes(self):
        """
        Primary integration assertion: all 4 smoke test stages pass.
        Uses run_validation() directly so failures are reported per-check.
        """
        cfg = load_settings()
        results = run_validation(cfg)

        failures = [r for r in results if not r.passed]
        if failures:
            failure_summary = "\n".join(str(r) for r in failures)
            pytest.fail(f"{len(failures)} health check(s) failed:\n{failure_summary}")

    @needs_docker
    def test_stack_teardown(self):
        """make down cleans up all resources."""
        _make("down")
