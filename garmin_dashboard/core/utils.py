import sys
from datetime import date, datetime
from pathlib import Path

from .config import PROJECT_ROOT


def ensure_local_venv_packages():
    venv_root = PROJECT_ROOT / ".venv"
    if not venv_root.exists():
        return

    version_tag = f"python{sys.version_info.major}.{sys.version_info.minor}"
    candidates = [
        venv_root / "lib" / version_tag / "site-packages",
        venv_root / "Lib" / "site-packages",
    ]

    for candidate in candidates:
        if candidate.exists():
            candidate_str = str(candidate)
            if candidate_str not in sys.path:
                sys.path.insert(0, candidate_str)


def norm(value) -> str:
    return str(value).strip().lower().replace(" ", "_") if value is not None else ""


def to_datetime(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except Exception:
            return None
    return None


def format_duration(seconds: float) -> str:
    seconds = int(round(seconds))
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60

    if h > 0:
        return f"{h}:{m:02d}:{s:02d}"
    return f"{m}:{s:02d}"


def pace_str(seconds_per_100m: float) -> str:
    return format_duration(seconds_per_100m) + "/100m"


def pace_str_precise(seconds_per_100m: float) -> str:
    total_tenths = int(round(seconds_per_100m * 10))
    minutes = total_tenths // 600
    seconds_tenths = total_tenths % 600
    seconds = seconds_tenths // 10
    tenths = seconds_tenths % 10
    return f"{minutes}:{seconds:02d}.{tenths}/100m"


def format_elapsed(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.2f} сек"

    total = int(round(seconds))
    h = total // 3600
    m = (total % 3600) // 60
    s = total % 60

    if h > 0:
        return f"{h} ч {m} мин {s} сек"
    return f"{m} мин {s} сек"
