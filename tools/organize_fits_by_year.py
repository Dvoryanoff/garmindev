from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from hashlib import sha1
from multiprocessing import freeze_support
from pathlib import Path
import shutil
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from garmin_dashboard.core.fit_parser import decode_fit_file, get_activity_datetime
from garmin_dashboard.core.utils import to_datetime


SOURCE_DIR = PROJECT_ROOT / "resources" / "fits"
UNKNOWN_YEAR_DIRNAME = "unknown_year"
MAX_WORKERS = 16


def iter_fit_files(root: Path) -> list[Path]:
    return sorted(
        path
        for path in root.rglob("*")
        if path.is_file() and path.suffix.lower() == ".fit"
    )


def file_sha1(path: Path) -> str:
    digest = sha1()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def classify_fit(path: Path) -> tuple[Path, str]:
    try:
        messages = decode_fit_file(path)
    except Exception as exc:
        return path, f"error:{exc}"

    dt = get_activity_datetime(messages)
    if not dt:
        file_id_mesgs = messages.get("file_id_mesgs", []) or []
        if file_id_mesgs:
            file_id = file_id_mesgs[0]
            dt = to_datetime(file_id.get("time_created"))
    if not dt:
        stat = path.stat()
        birthtime = getattr(stat, "st_birthtime", None)
        if birthtime:
            dt = datetime.fromtimestamp(birthtime, tz=timezone.utc)
        else:
            dt = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
    if not dt:
        return path, UNKNOWN_YEAR_DIRNAME
    return path, str(dt.year)


def unique_target_path(target_dir: Path, source_name: str, source_path: Path) -> Path:
    target = target_dir / source_name
    if not target.exists():
        return target

    try:
        if file_sha1(target) == file_sha1(source_path):
            return target
    except Exception:
        pass

    stem = Path(source_name).stem
    suffix = Path(source_name).suffix
    counter = 1
    while True:
        candidate = target_dir / f"{stem}__dup{counter}{suffix}"
        if not candidate.exists():
            return candidate
        counter += 1


def organize(source_dir: Path) -> dict[str, int]:
    files = iter_fit_files(source_dir)
    stats = {
        "total": len(files),
        "moved": 0,
        "already_in_place": 0,
        "unsupported": 0,
        "unknown_year": 0,
        "errors": 0,
    }

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for source_path, bucket in executor.map(classify_fit, files):
            if bucket == "unsupported":
                stats["unsupported"] += 1
                continue
            if bucket.startswith("error:"):
                stats["errors"] += 1
                continue

            year_dir = source_dir / bucket
            if bucket == UNKNOWN_YEAR_DIRNAME:
                stats["unknown_year"] += 1

            if source_path.parent == year_dir:
                stats["already_in_place"] += 1
                continue

            year_dir.mkdir(parents=True, exist_ok=True)
            target_path = unique_target_path(year_dir, source_path.name, source_path)

            if target_path == source_path:
                stats["already_in_place"] += 1
                continue
            if target_path.exists():
                stats["already_in_place"] += 1
                continue

            shutil.move(str(source_path), str(target_path))
            stats["moved"] += 1

    return stats


def main() -> None:
    source_dir = SOURCE_DIR
    if not source_dir.exists():
        raise SystemExit(f"Source folder not found: {source_dir}")

    stats = organize(source_dir)
    print(f"source={source_dir}")
    for key in ("total", "moved", "already_in_place", "unsupported", "unknown_year", "errors"):
        print(f"{key}={stats[key]}")


if __name__ == "__main__":
    freeze_support()
    main()
