import csv
import pickle
import sys
import tempfile
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from time import perf_counter

from .config import CACHE_VERSION, IntervalConfig, RESOURCES_DIR, RuntimeConfig
from .fit_parser import (
    decode_fit_file,
    get_activity_datetime,
    get_activity_key,
    get_user_id,
    get_user_name,
    is_supported_swim,
    iter_target_swim_laps,
)
from .utils import format_elapsed


def find_fit_files(root: Path) -> list[Path]:
    return sorted(p for p in root.rglob("*") if p.is_file() and p.suffix.lower() == ".fit")


def cache_matches_root(files_cache: dict, expected_root: Path | None = None) -> bool:
    if not isinstance(files_cache, dict):
        return False
    if not files_cache:
        return True

    expected_root = (expected_root or RESOURCES_DIR).resolve()
    for key in files_cache.keys():
        try:
            path = Path(str(key)).resolve()
        except Exception:
            return False
        if expected_root not in path.parents and path != expected_root:
            return False
    return True


def load_cache(cache_file: Path, expected_root: Path | None = None) -> dict:
    if not cache_file.exists():
        return {}

    try:
        with cache_file.open("rb") as f:
            payload = pickle.load(f)

        if not isinstance(payload, dict):
            return {}

        if payload.get("cache_version") != CACHE_VERSION:
            print("Кэш найден, но версия логики изменилась — кэш будет пересобран.")
            return {}

        files_cache = payload.get("files", {})
        if not cache_matches_root(files_cache, expected_root=expected_root):
            print("Кэш найден, но ссылается на старую структуру папок — кэш будет пересобран.")
            return {}
        return files_cache if isinstance(files_cache, dict) else {}

    except Exception as e:
        print(f"WARNING: не удалось прочитать кэш {cache_file.name}: {e}", file=sys.stderr)
        return {}


def clear_cache_file(cache_file: Path):
    try:
        if cache_file.exists():
            cache_file.unlink()
    except Exception as e:
        print(f"WARNING: не удалось удалить кэш {cache_file.name}: {e}", file=sys.stderr)


def save_cache(cache_file: Path, files_cache: dict):
    payload = {
        "cache_version": CACHE_VERSION,
        "files": files_cache,
    }

    cache_file.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="wb",
        dir=cache_file.parent,
        prefix=f"{cache_file.name}.",
        suffix=".tmp",
        delete=False,
    ) as f:
        pickle.dump(payload, f, protocol=pickle.HIGHEST_PROTOCOL)
        tmp_path = Path(f.name)

    tmp_path.replace(cache_file)


def process_fit_file(fit_path_str: str, interval_config: IntervalConfig):
    fit_path = Path(fit_path_str)

    try:
        messages = decode_fit_file(fit_path)
        if not is_supported_swim(messages):
            return fit_path_str, []

        activity_dt = get_activity_datetime(messages)
        activity_dt_text = activity_dt.isoformat(sep=" ") if activity_dt else ""
        activity_key = get_activity_key(messages, fit_path)
        user_id = get_user_id(messages) or "unknown"
        user_name = get_user_name(messages, fit_path)

        out = []
        for lap in iter_target_swim_laps(messages, interval_config=interval_config):
            out.append({
                "file_name": fit_path.name,
                "activity_key": activity_key,
                "activity_date": activity_dt_text,
                "user_id": user_id,
                "user_name": user_name,
                "lap_start": lap["lap_start"].isoformat(sep=" ") if lap["lap_start"] else "",
                "lap_end": lap["lap_end"].isoformat(sep=" ") if lap["lap_end"] else "",
                "distance_m": lap["distance_m"],
                "raw_distance_m": lap["raw_distance_m"],
                "time_s": round(lap["time_s"], 2),
                "time_text": lap["time_text"],
                "stroke": lap["stroke"],
                "swim_type": lap["swim_type"],
                "pace_100m_s": round(lap["pace_100m_s"], 2),
                "pace_100m": lap["pace_100m"],
            })
        return fit_path_str, out

    except Exception as e:
        print(f"SKIP {fit_path.name}: {e}", file=sys.stderr)
        return fit_path_str, []


def chunked(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def process_fit_chunk(payload: tuple[list[str], IntervalConfig]):
    paths, interval_config = payload
    return [process_fit_file(p, interval_config=interval_config) for p in paths]


def process_batches(paths_to_process: list[str], batch_size: int, max_workers: int, interval_config: IntervalConfig):
    batches = list(chunked(paths_to_process, batch_size))
    if not batches:
        return

    process_payloads = [(batch, interval_config) for batch in batches]
    try:
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            for batch_results in executor.map(process_fit_chunk, process_payloads, chunksize=1):
                yield batch_results
    except Exception:
        for batch in batches:
            yield [process_fit_file(path, interval_config=interval_config) for path in batch]


def write_detail_csv(detail_rows: list[dict], target_path: Path):
    with target_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "file_name",
                "activity_key",
                "activity_date",
                "user_id",
                "user_name",
                "lap_start",
                "lap_end",
                "swim_type",
                "distance_m",
                "raw_distance_m",
                "time_s",
                "time_text",
                "stroke",
                "pace_100m_s",
                "pace_100m",
                "avg_pace_for_distance_s",
                "avg_pace_for_distance",
                "middle_pace_for_distance_s",
                "middle_pace_for_distance",
                "best_pace_for_distance_s",
                "best_pace_for_distance",
                "best_pace_date_for_distance",
            ],
        )
        writer.writeheader()
        writer.writerows(detail_rows)


def write_summary_csv(summary_rows: list[dict], target_path: Path):
    with target_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "distance_m",
                "count",
                "total_distance_m",
                "avg_time_s",
                "avg_time",
                "best_time_s",
                "best_time",
                "avg_pace_100m_s",
                "avg_pace_100m",
                "best_pace_100m_s",
                "best_pace_100m",
                "best_pace_date",
                "middle_count",
                "middle_pace_100m_s",
                "middle_pace_100m",
                "swim_types",
                "strokes",
            ],
        )
        writer.writeheader()
        writer.writerows(summary_rows)


def generate_dataset(
    runtime_config: RuntimeConfig,
    interval_config: IntervalConfig,
) -> dict:
    total_started = perf_counter()

    if not runtime_config.fit_dir.exists() or not runtime_config.fit_dir.is_dir():
        raise FileNotFoundError(f"Папка FIT не найдена: {runtime_config.fit_dir}")

    t0 = perf_counter()
    fit_files = find_fit_files(runtime_config.fit_dir)
    t1 = perf_counter()

    if not fit_files:
        raise FileNotFoundError(
            f"В папке {runtime_config.fit_dir} не найдено FIT-файлов. Проверьте расширения и расположение файлов."
        )

    t2 = perf_counter()
    old_cache = load_cache(runtime_config.cache_file, expected_root=runtime_config.fit_dir)
    t3 = perf_counter()

    state_by_path = {}
    for p in fit_files:
        try:
            stat = p.stat()
            path_key = str(p.resolve())
            state_by_path[path_key] = {
                "size": stat.st_size,
                "mtime_ns": stat.st_mtime_ns,
                "name": p.name,
            }
        except FileNotFoundError:
            continue

    rows = []
    next_cache = {}
    paths_to_process = []
    cached_files = 0
    cached_rows = 0

    t4 = perf_counter()
    for path_key, meta in state_by_path.items():
        cached = old_cache.get(path_key)
        if (
            cached
            and cached.get("size") == meta["size"]
            and cached.get("mtime_ns") == meta["mtime_ns"]
            and cached.get("interval_config_key") == repr(interval_config)
        ):
            file_rows_all = cached.get("rows", [])
            rows.extend(file_rows_all)
            next_cache[path_key] = cached
            cached_files += 1
            cached_rows += len(file_rows_all)
        else:
            paths_to_process.append(path_key)
    t5 = perf_counter()

    t6 = perf_counter()
    if paths_to_process:
        for batch_results in process_batches(
            paths_to_process,
            batch_size=runtime_config.batch_size,
            max_workers=runtime_config.max_workers,
            interval_config=interval_config,
        ):
            for path_key, file_rows in batch_results:
                meta = state_by_path[path_key]
                next_cache[path_key] = {
                    "size": meta["size"],
                    "mtime_ns": meta["mtime_ns"],
                    "rows": file_rows,
                    "interval_config_key": repr(interval_config),
                }
                rows.extend(file_rows)
    t7 = perf_counter()

    t8 = perf_counter()
    save_cache(runtime_config.cache_file, next_cache)
    t9 = perf_counter()

    return {
        "rows": rows,
        "meta": {
            "fit_dir": str(runtime_config.fit_dir),
            "total_files": len(fit_files),
            "cached_files": cached_files,
            "cached_rows": cached_rows,
            "processed_files": len(paths_to_process),
            "max_workers": runtime_config.max_workers,
            "batch_size": runtime_config.batch_size,
            "timings": {
                "find_files": format_elapsed(t1 - t0),
                "load_cache": format_elapsed(t3 - t2),
                "cache_compare": format_elapsed(t5 - t4),
                "decode_files": format_elapsed(t7 - t6),
                "save_cache": format_elapsed(t9 - t8),
                "total": format_elapsed(t9 - total_started),
            },
        },
    }
