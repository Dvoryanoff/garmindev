from __future__ import annotations

import os
import stat
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
HOOKS_DIR = PROJECT_ROOT / ".git" / "hooks"
PRE_PUSH = HOOKS_DIR / "pre-push"

PRE_PUSH_SCRIPT = """#!/bin/sh
set -e
cd "$(dirname "$0")/../.."
echo "[pre-push] Running local checks..."
python3 tools/check.py
"""


def main() -> None:
    HOOKS_DIR.mkdir(parents=True, exist_ok=True)
    PRE_PUSH.write_text(PRE_PUSH_SCRIPT, encoding="utf-8")
    current_mode = PRE_PUSH.stat().st_mode
    PRE_PUSH.chmod(current_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    print(f"Installed pre-push hook: {PRE_PUSH}")


if __name__ == "__main__":
    main()
