"""
tests/unit/test_render_sql.py — Unit tests for scripts/render_sql.py.

Tests template rendering logic with no Docker or filesystem side-effects
beyond a tmp_path fixture.

Run: make test-unit
     pytest tests/unit/test_render_sql.py -v
"""

import pathlib

import pytest

# conftest.py adds project root to sys.path
from scripts.render_sql import assert_no_unresolved_placeholders, load_env, render_template

# ---------------------------------------------------------------------------
# load_env
# ---------------------------------------------------------------------------


class TestLoadEnv:
    def test_loads_key_value_pairs(self, tmp_path):
        env_file = tmp_path / ".env"
        env_file.write_text("BROKER=redpanda\nSTORAGE=minio\n")
        env = load_env(str(env_file))
        assert env["BROKER"] == "redpanda"
        assert env["STORAGE"] == "minio"

    def test_strips_inline_comments(self, tmp_path):
        env_file = tmp_path / ".env"
        env_file.write_text("BROKER=redpanda  # this is a comment\n")
        env = load_env(str(env_file))
        assert env["BROKER"] == "redpanda"

    def test_skips_blank_lines(self, tmp_path):
        env_file = tmp_path / ".env"
        env_file.write_text("\n\nBROKER=kafka\n\n")
        env = load_env(str(env_file))
        assert env == {"BROKER": "kafka"}

    def test_skips_comment_lines(self, tmp_path):
        env_file = tmp_path / ".env"
        env_file.write_text("# full-line comment\nBROKER=kafka\n")
        env = load_env(str(env_file))
        assert "BROKER" in env
        assert len(env) == 1

    def test_missing_file_returns_empty_dict(self, tmp_path):
        env = load_env(str(tmp_path / ".env.nonexistent"))
        assert env == {}

    def test_handles_values_with_equals_sign(self, tmp_path):
        """Values that contain '=' should not be split further."""
        env_file = tmp_path / ".env"
        env_file.write_text("URL=http://host:9000/path?a=b\n")
        env = load_env(str(env_file))
        assert env["URL"] == "http://host:9000/path?a=b"


# ---------------------------------------------------------------------------
# render_template
# ---------------------------------------------------------------------------


class TestRenderTemplate:
    def test_substitutes_all_vars(self, tmp_path):
        tmpl = tmp_path / "query.sql.tmpl"
        tmpl.write_text("SELECT * FROM ${TABLE} WHERE dt = '${DATE}';")
        out_dir = tmp_path / "out"
        out_dir.mkdir()

        render_template(tmpl, {"TABLE": "trips", "DATE": "2024-01-01"}, "query.sql", out_dir)

        result = (out_dir / "query.sql").read_text()
        assert result == "SELECT * FROM trips WHERE dt = '2024-01-01';"

    def test_writes_output_file(self, tmp_path):
        tmpl = tmp_path / "a.sql.tmpl"
        tmpl.write_text("SELECT 1;")
        out_dir = tmp_path / "out"
        out_dir.mkdir()

        render_template(tmpl, {}, "a.sql", out_dir)

        assert (out_dir / "a.sql").exists()

    def test_exits_on_unsubstituted_placeholder(self, tmp_path):
        tmpl = tmp_path / "b.sql.tmpl"
        tmpl.write_text("SELECT * FROM ${TABLE};")
        out_dir = tmp_path / "out"
        out_dir.mkdir()

        with pytest.raises(SystemExit) as exc_info:
            render_template(tmpl, {}, "b.sql", out_dir)  # TABLE not in env

        assert exc_info.value.code != 0  # non-zero exit

    def test_exits_on_partial_substitution(self, tmp_path):
        tmpl = tmp_path / "c.sql.tmpl"
        tmpl.write_text("SELECT ${COL} FROM ${TABLE};")
        out_dir = tmp_path / "out"
        out_dir.mkdir()

        with pytest.raises(SystemExit):
            render_template(tmpl, {"COL": "id"}, "c.sql", out_dir)  # TABLE missing

    def test_output_name_can_differ_from_template_name(self, tmp_path):
        tmpl = tmp_path / "00_catalog.sql.tmpl"
        tmpl.write_text("CREATE CATALOG iceberg_catalog;")
        out_dir = tmp_path / "out"
        out_dir.mkdir()

        render_template(tmpl, {}, "00_catalog.sql", out_dir)

        assert (out_dir / "00_catalog.sql").exists()
        assert not (out_dir / "00_catalog.sql.tmpl").exists()

    def test_no_dollar_braces_passes_through(self, tmp_path):
        """SQL with no ${} placeholders renders unchanged."""
        sql = "SELECT vendor_id, COUNT(*) FROM bronze.raw_trips GROUP BY 1;"
        tmpl = tmp_path / "plain.sql.tmpl"
        tmpl.write_text(sql)
        out_dir = tmp_path / "out"
        out_dir.mkdir()

        render_template(tmpl, {}, "plain.sql", out_dir)

        assert (out_dir / "plain.sql").read_text() == sql

    def test_multiline_template(self, tmp_path):
        tmpl = tmp_path / "multi.sql.tmpl"
        tmpl.write_text(
            "CREATE TABLE ${CATALOG}.bronze.raw_trips (\n"
            "  vendor_id STRING\n"
            ") WITH (\n"
            "  'connector' = 'iceberg',\n"
            "  'catalog-name' = '${CATALOG}'\n"
            ");\n"
        )
        out_dir = tmp_path / "out"
        out_dir.mkdir()

        render_template(tmpl, {"CATALOG": "iceberg_catalog"}, "multi.sql", out_dir)

        result = (out_dir / "multi.sql").read_text()
        assert "iceberg_catalog" in result
        assert "${CATALOG}" not in result


class TestPlaceholderScan:
    def test_detects_unresolved_shell_placeholder(self, tmp_path):
        out_file = tmp_path / "build.sql"
        out_file.write_text("SELECT * FROM ${MISSING_TABLE};", encoding="utf-8")

        with pytest.raises(SystemExit):
            assert_no_unresolved_placeholders([out_file])

    def test_detects_unresolved_jinja_placeholder(self, tmp_path):
        out_file = tmp_path / "build.sql"
        out_file.write_text("SELECT * FROM {{ unresolved_var }};", encoding="utf-8")

        with pytest.raises(SystemExit):
            assert_no_unresolved_placeholders([out_file])


# ---------------------------------------------------------------------------
# Integration: render actual project templates (if they exist)
# ---------------------------------------------------------------------------


class TestRealTemplates:
    """Smoke-render the actual SQL templates with a minimal env dict."""

    MINIMAL_ENV = {
        "BROKER": "redpanda",
        "CATALOG": "hadoop",
        "STORAGE": "minio",
        "MODE": "batch",
        "TOPIC": "taxi.raw_trips",
        "DLQ_TOPIC": "taxi.raw_trips.dlq",
        "WAREHOUSE": "s3a://warehouse/",
        "S3_ENDPOINT": "http://minio:9000",
        "S3_USE_SSL": "false",
        "S3_PATH_STYLE": "true",
        "AWS_ACCESS_KEY_ID": "minioadmin",
        "AWS_SECRET_ACCESS_KEY": "minioadmin",
        "MINIO_ROOT_USER": "minioadmin",
        "MINIO_ROOT_PASSWORD": "minioadmin",
        "DUCKDB_S3_ENDPOINT": "minio:9000",
    }

    def _template_path(self, name: str) -> pathlib.Path:
        return pathlib.Path(__file__).parent.parent.parent / "flink" / "sql" / name

    def test_catalog_template_renders(self, tmp_path):
        tmpl = self._template_path("00_catalog.sql.tmpl")
        if not tmpl.exists():
            pytest.skip(f"Template not found: {tmpl}")
        out_dir = tmp_path / "sql"
        out_dir.mkdir()
        render_template(tmpl, self.MINIMAL_ENV, "00_catalog.sql", out_dir)
        result = (out_dir / "00_catalog.sql").read_text()
        assert "iceberg_catalog" in result.lower() or "catalog" in result.lower()

    def test_bronze_batch_template_renders(self, tmp_path):
        tmpl = self._template_path("05_bronze_batch.sql.tmpl")
        if not tmpl.exists():
            pytest.skip(f"Template not found: {tmpl}")
        out_dir = tmp_path / "sql"
        out_dir.mkdir()
        render_template(tmpl, self.MINIMAL_ENV, "05_bronze_batch.sql", out_dir)
        result = (out_dir / "05_bronze_batch.sql").read_text()
        assert "taxi.raw_trips" in result

    def test_silver_template_renders(self, tmp_path):
        tmpl = self._template_path("06_silver.sql.tmpl")
        if not tmpl.exists():
            pytest.skip(f"Template not found: {tmpl}")
        out_dir = tmp_path / "sql"
        out_dir.mkdir()
        render_template(tmpl, self.MINIMAL_ENV, "06_silver.sql", out_dir)
        result = (out_dir / "06_silver.sql").read_text()
        assert len(result) > 100  # non-trivial output
