import os
from dataclasses import dataclass
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
RESOURCES_DIR = PROJECT_ROOT / "resources"
FIT_DIR = RESOURCES_DIR / "FIT"
DETAIL_CSV = PROJECT_ROOT / "garmin_swim_intervals_details.csv"
SUMMARY_CSV = PROJECT_ROOT / "garmin_swim_intervals_summary.csv"
CACHE_FILE = PROJECT_ROOT / "garmin_swim_fit_cache.pkl"
MONTHLY_HISTORY_DIR = PROJECT_ROOT / "monthly_history"

MONTHLY_FIXED_DISTANCES = (50, 100, 200, 400, 800, 1000, 1200, 1500, 1800)

CACHE_VERSION = 7

POOL_SUBSPORTS = {"lap_swimming", "pool_swimming", "lap", "pool", ""}
OPEN_WATER_SUBSPORTS = {"open_water", "open_water_swimming", "open_water_swim"}
ALL_SUPPORTED_SUBSPORTS = POOL_SUBSPORTS | OPEN_WATER_SUBSPORTS

DEVICE_CPU_COUNT = max(1, os.cpu_count() or 1)
# Локальный benchmark на этом arm64 Mac показал лучший результат на 300 FIT:
# workers=4, batch_size=100.
DEFAULT_MAX_WORKERS = min(4, DEVICE_CPU_COUNT)
DEFAULT_BATCH_SIZE = 100


@dataclass(frozen=True)
class IntervalConfig:
    target_distances: tuple[int, ...] = (50, 100, 150, 200, 300, 400, 500, 600, 800, 1000)
    distance_tolerance_m: float = 0.5
    long_freestyle_min_distance_m: float = 1000.0
    allow_open_water_long: bool = True
    allow_pool_long_freestyle: bool = True


@dataclass(frozen=True)
class RuntimeConfig:
    fit_dir: Path = FIT_DIR
    detail_csv: Path = DETAIL_CSV
    summary_csv: Path = SUMMARY_CSV
    cache_file: Path = CACHE_FILE
    max_workers: int = DEFAULT_MAX_WORKERS
    batch_size: int = DEFAULT_BATCH_SIZE


@dataclass(frozen=True)
class ReportRequest:
    swim_mode: str = "all"
    period: str = "current_year"
    days: int | None = None
    persist_csv: bool = False
    interval_config: IntervalConfig = IntervalConfig()
    runtime_config: RuntimeConfig = RuntimeConfig()


def list_resource_dirs(root: Path | None = None) -> list[Path]:
    root = root or RESOURCES_DIR
    if not root.exists():
        return []
    candidates = []
    for child in sorted(root.iterdir(), key=lambda p: str(p).lower()):
        if not child.is_dir():
            continue
        if child.name.startswith("."):
            continue
        candidates.append(child)
    return candidates


def resolve_resource_dir(resource_name: str | None) -> Path:
    resources = list_resource_dirs(RESOURCES_DIR)
    if not resources:
        return FIT_DIR

    if resource_name:
        normalized = str(resource_name).strip()
        for path in resources:
            relative_name = str(path.relative_to(RESOURCES_DIR))
            if relative_name == normalized:
                return path
        raise ValueError(f"Неизвестная папка ресурса: {resource_name}")

    for path in resources:
        if path.name == FIT_DIR.name:
            return path
    for path in resources:
        if path.name.lower() == "fit":
            return path
    return resources[0]


def parse_distances(value: str | None, fallback: tuple[int, ...] | None = None) -> tuple[int, ...]:
    fallback = fallback or IntervalConfig().target_distances
    if not value or not str(value).strip():
        return fallback

    parts = [part.strip() for part in str(value).split(",")]
    distances = []
    for part in parts:
        if not part:
            continue
        number = int(part)
        if number <= 0:
            raise ValueError("Дистанции должны быть положительными числами")
        distances.append(number)

    unique = tuple(sorted(set(distances)))
    if not unique:
        raise ValueError("Нужна хотя бы одна дистанция")
    return unique
