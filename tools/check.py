from __future__ import annotations

import compileall
import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def run_step(title: str, command: list[str]) -> None:
    print(f"[check] {title}")
    completed = subprocess.run(command, cwd=PROJECT_ROOT)
    if completed.returncode != 0:
        raise SystemExit(completed.returncode)


def compile_python() -> None:
    print("[check] py_compile")
    ok = compileall.compile_dir(
        PROJECT_ROOT / "garmin_dashboard",
        force=True,
        quiet=1,
    )
    run_dashboard_ok = compileall.compile_file(
        PROJECT_ROOT / "run_dashboard.py",
        force=True,
        quiet=1,
    )
    serve_dashboard_ok = compileall.compile_file(
        PROJECT_ROOT / "serve_dashboard.py",
        force=True,
        quiet=1,
    )
    ingest_dashboard_ok = compileall.compile_file(
        PROJECT_ROOT / "ingest_dashboard.py",
        force=True,
        quiet=1,
    )
    garmin_local_ok = compileall.compile_file(
        PROJECT_ROOT / "garmin_local.py",
        force=True,
        quiet=1,
    )
    if not ok or not run_dashboard_ok or not serve_dashboard_ok or not ingest_dashboard_ok or not garmin_local_ok:
        raise SystemExit(1)


def main() -> None:
    compile_python()
    run_step("tests", [sys.executable, "-m", "unittest", "discover", "-s", "tests", "-p", "test_*.py"])


if __name__ == "__main__":
    main()
