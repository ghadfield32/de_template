#!/usr/bin/env python3
"""
render_sql.py — Strict SQL template renderer for de_template.

Reads .env, renders flink/sql/*.sql.tmpl → build/sql/*.sql using ${VAR} substitution.
Selects the active catalog and source templates based on CATALOG and MODE axes.
Fails fast if any ${VAR} placeholder remains unsubstituted after rendering.

Usage (called by Makefile build-sql target):
    python3 scripts/render_sql.py
"""
import pathlib
import re
import sys
from string import Template


def load_env(path=".env") -> dict:
    """Load .env file into a dict. Skips comments and blank lines."""
    env = {}
    try:
        for line in open(path):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            k, _, v = line.partition("=")
            k = k.strip()
            v = v.strip()
            if k:
                env[k] = v
    except FileNotFoundError:
        print(f"WARNING: {path} not found — using environment variables only")
    return env


def render_template(tmpl_path: pathlib.Path, env: dict, out_name: str, out_dir: pathlib.Path):
    """Render a single template file and write to out_dir/out_name."""
    raw = tmpl_path.read_text(encoding="utf-8")
    rendered = Template(raw).safe_substitute(env)

    # Strict check: fail if any ${VAR} placeholder was not substituted
    remaining = re.findall(r"\$\{(\w+)\}", rendered)
    if remaining:
        missing = sorted(set(remaining))
        sys.exit(
            f"\nERROR: Unsubstituted placeholders in {tmpl_path.name}: {missing}\n"
            f"  Add these variables to your .env file.\n"
            f"  See .env.example for reference.\n"
        )

    out_path = out_dir / out_name
    out_path.write_text(rendered, encoding="utf-8")
    print(f"  rendered: {tmpl_path.name}  →  build/sql/{out_name}")


def main():
    # Load .env + override with actual environment variables (env wins)
    import os
    env = load_env(".env")
    env.update(os.environ)  # OS environment takes precedence

    catalog = env.get("CATALOG", "hadoop").strip().lower()
    mode = env.get("MODE", "batch").strip().lower()

    tmpl_dir = pathlib.Path("flink/sql")
    out_dir = pathlib.Path("build/sql")
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"\nRendering SQL templates (CATALOG={catalog}, MODE={mode})...")

    # --- Active catalog template ---
    if catalog == "rest":
        catalog_tmpl = tmpl_dir / "00_catalog_rest.sql.tmpl"
    else:
        catalog_tmpl = tmpl_dir / "00_catalog.sql.tmpl"
    render_template(catalog_tmpl, env, "00_catalog.sql", out_dir)

    # --- Active source template (selects batch vs streaming session settings) ---
    if mode == "streaming_bronze":
        source_tmpl = tmpl_dir / "01_source_streaming.sql.tmpl"
    else:
        source_tmpl = tmpl_dir / "01_source.sql.tmpl"
    render_template(source_tmpl, env, "01_source.sql", out_dir)

    # --- Fixed templates (always rendered) ---
    for name in ["05_bronze.sql.tmpl", "06_silver.sql.tmpl", "07_streaming_bronze.sql.tmpl"]:
        tmpl_path = tmpl_dir / name
        if tmpl_path.exists():
            render_template(tmpl_path, env, name.replace(".tmpl", ""), out_dir)
        else:
            print(f"  skipped (not found): {name}")

    print(f"\nSQL rendered to build/sql/  ({len(list(out_dir.glob('*.sql')))} files)")


if __name__ == "__main__":
    main()
