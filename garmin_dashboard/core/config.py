import os
from dataclasses import dataclass
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _env_path(name: str, default: Path) -> Path:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    path = Path(raw).expanduser()
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value > 0 else default


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name, "").strip().lower()
    if not raw:
        return default
    return raw not in {"0", "false", "no", "off"}


RESOURCES_DIR = _env_path("GARMIN_RESOURCES_DIR", PROJECT_ROOT / "resources")
FIT_DIR = _env_path("GARMIN_FIT_DIR", RESOURCES_DIR / "FIT")
DETAIL_CSV = _env_path("GARMIN_DETAIL_CSV", PROJECT_ROOT / "garmin_swim_intervals_details.csv")
SUMMARY_CSV = _env_path("GARMIN_SUMMARY_CSV", PROJECT_ROOT / "garmin_swim_intervals_summary.csv")
CACHE_FILE = _env_path("GARMIN_CACHE_FILE", PROJECT_ROOT / "garmin_swim_fit_cache.pkl")
MONTHLY_HISTORY_DIR = _env_path("GARMIN_MONTHLY_HISTORY_DIR", PROJECT_ROOT / "monthly_history")
DATABASE_URL = os.getenv("DATABASE_URL", f"sqlite:///{PROJECT_ROOT / 'garmin_dashboard.db'}")
DB_AUTO_INGEST = os.getenv("GARMIN_DB_AUTO_INGEST", "1").strip().lower() not in {"0", "false", "no", "off"}
UPLOAD_DIR = _env_path("GARMIN_UPLOAD_DIR", PROJECT_ROOT / "uploads")
SESSION_TTL_DAYS = _env_int("GARMIN_SESSION_TTL_DAYS", 30)
SESSION_IDLE_TIMEOUT_MINUTES = _env_int("GARMIN_SESSION_IDLE_TIMEOUT_MINUTES", 120)
BOOTSTRAP_ADMIN_EMAIL = os.getenv("GARMIN_BOOTSTRAP_ADMIN_EMAIL", "dvoryanoff@mail.ru").strip().lower()
ENABLE_DEMO_ACCOUNTS = _env_bool("GARMIN_ENABLE_DEMO_ACCOUNTS", False)
DEMO_USER_PASSWORD = os.getenv("GARMIN_DEMO_USER_PASSWORD", "demo-demo").strip() or "demo-demo"
UPLOAD_MAX_FILES = _env_int("GARMIN_UPLOAD_MAX_FILES", 100000)
UPLOAD_MAX_BATCH_BYTES = _env_int("GARMIN_UPLOAD_MAX_BATCH_BYTES", 32 * 1024 * 1024)
UPLOAD_MAX_FILE_BYTES = _env_int("GARMIN_UPLOAD_MAX_FILE_BYTES", 10 * 1024 * 1024)
REST_LONG_PAUSE_THRESHOLD_SECONDS = _env_int("GARMIN_REST_LONG_PAUSE_THRESHOLD_SECONDS", 120)
MIN_FREE_DISK_MB = _env_int("GARMIN_MIN_FREE_DISK_MB", 512)
AUTH_CODE_TTL_MINUTES = _env_int("GARMIN_AUTH_CODE_TTL_MINUTES", 15)
LOGIN_RATE_LIMIT_WINDOW_MINUTES = _env_int("GARMIN_LOGIN_RATE_LIMIT_WINDOW_MINUTES", 15)
LOGIN_RATE_LIMIT_ATTEMPTS = _env_int("GARMIN_LOGIN_RATE_LIMIT_ATTEMPTS", 8)
SMTP_HOST = os.getenv("SMTP_HOST", "").strip()
SMTP_PORT = _env_int("SMTP_PORT", 465)
SMTP_USERNAME = os.getenv("SMTP_USERNAME", "").strip()
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "").strip()
SMTP_FROM_EMAIL = os.getenv("SMTP_FROM_EMAIL", "").strip()
SMTP_USE_SSL = os.getenv("SMTP_USE_SSL", "1").strip().lower() not in {"0", "false", "no", "off"}
SMTP_USE_TLS = os.getenv("SMTP_USE_TLS", "0").strip().lower() not in {"0", "false", "no", "off"}

MONTHLY_FIXED_DISTANCES = (50, 100, 200, 400, 600, 800, 1000, 1200, 1500, 1800)

CACHE_VERSION = 7
PARSER_VERSION = CACHE_VERSION + 1001

POOL_SUBSPORTS = {"lap_swimming", "pool_swimming", "lap", "pool", ""}
OPEN_WATER_SUBSPORTS = {"open_water", "open_water_swimming", "open_water_swim"}
ALL_SUPPORTED_SUBSPORTS = POOL_SUBSPORTS | OPEN_WATER_SUBSPORTS

DEVICE_CPU_COUNT = max(1, os.cpu_count() or 1)
# Локальный benchmark на этом arm64 Mac показал лучший результат на 300 FIT:
# workers=4, batch_size=100.
DEFAULT_MAX_WORKERS = _env_int("GARMIN_MAX_WORKERS", min(4, DEVICE_CPU_COUNT))
DEFAULT_BATCH_SIZE = _env_int("GARMIN_BATCH_SIZE", 100)


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
    database_url: str = DATABASE_URL
    db_auto_ingest: bool = DB_AUTO_INGEST
    upload_dir: Path = UPLOAD_DIR
    max_workers: int = DEFAULT_MAX_WORKERS
    batch_size: int = DEFAULT_BATCH_SIZE


@dataclass(frozen=True)
class ReportRequest:
    swim_mode: str = "all"
    period: str = "current_year"
    days: int | None = None
    persist_csv: bool = False
    owner_account_id: int | None = None
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
