from __future__ import annotations

import compileall
import os
import shutil
import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def build_env() -> dict[str, str]:
    env = dict(os.environ)
    existing_pythonpath = env.get("PYTHONPATH", "")
    pythonpath_parts = [str(PROJECT_ROOT)]
    if existing_pythonpath:
        pythonpath_parts.append(existing_pythonpath)
    env["PYTHONPATH"] = ":".join(pythonpath_parts)
    return env


def run_step(title: str, command: list[str]) -> None:
    print(f"[check] {title}")
    completed = subprocess.run(command, cwd=PROJECT_ROOT, env=build_env())
    if completed.returncode != 0:
        raise SystemExit(completed.returncode)


def find_pytest_command() -> list[str]:
    candidates: list[list[str]] = [[sys.executable, "-m", "pytest"]]
    python3_path = shutil.which("python3")
    if python3_path:
        candidates.append([python3_path, "-m", "pytest"])
    pytest_path = shutil.which("pytest")
    if pytest_path:
        candidates.append([pytest_path])
    pyenv_pytest = Path.home() / ".pyenv" / "shims" / "pytest"
    if pyenv_pytest.exists():
        candidates.append([str(pyenv_pytest)])
    for command in candidates:
        completed = subprocess.run(
            [*command, "--version"],
            cwd=PROJECT_ROOT,
            env=build_env(),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        if completed.returncode == 0:
            return command
    raise SystemExit("pytest не найден ни в текущем интерпретаторе, ни среди доступных python3/pytest команд")


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
    run_step("tests", [*find_pytest_command(), "-vv", "tests"])


if __name__ == "__main__":
    main()
