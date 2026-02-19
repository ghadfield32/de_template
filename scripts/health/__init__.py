"""
scripts/health — Composable health check modules for de_template.

Each module exposes a run_checks(cfg, compose_args) function that returns
a list of CheckResult objects. validate.py aggregates them all.

Usage:
    from scripts.health import CheckResult
    from scripts.health.flink import run_checks as flink_checks
"""

from dataclasses import dataclass


@dataclass
class CheckResult:
    stage: str
    name: str
    passed: bool
    message: str
    detail: str | None = None

    def __str__(self) -> str:
        status = "PASS" if self.passed else "FAIL"
        line = f"  [{status}] {self.name}: {self.message}"
        if self.detail and not self.passed:
            line += f"\n         {self.detail}"
        return line
