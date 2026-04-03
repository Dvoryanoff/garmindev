import os

from .app.reports import build_report
from .core.config import DEFAULT_BATCH_SIZE, DEFAULT_MAX_WORKERS, IntervalConfig, ReportRequest, RuntimeConfig, parse_distances


def run_cli():
    swim_mode = os.environ.get("SWIM_MODE", "all").strip().lower() or "all"
    period = os.environ.get("SWIM_PERIOD", "all").strip().lower() or "all"
    days_env = os.environ.get("SWIM_DAYS")
    days = int(days_env) if days_env else None

    request = ReportRequest(
        swim_mode=swim_mode,
        period=period,
        days=days,
        persist_csv=True,
        interval_config=IntervalConfig(
            target_distances=parse_distances(os.environ.get("SWIM_TARGET_DISTANCES")),
            distance_tolerance_m=float(os.environ.get("SWIM_DISTANCE_TOLERANCE", "0.5")),
            long_freestyle_min_distance_m=float(os.environ.get("SWIM_LONG_MIN_DISTANCE", "1000")),
            allow_open_water_long=os.environ.get("SWIM_ALLOW_OPEN_WATER_LONG", "1") != "0",
            allow_pool_long_freestyle=os.environ.get("SWIM_ALLOW_POOL_LONG", "1") != "0",
        ),
        runtime_config=RuntimeConfig(
            max_workers=int(os.environ.get("SWIM_MAX_WORKERS", DEFAULT_MAX_WORKERS)),
            batch_size=int(os.environ.get("SWIM_BATCH_SIZE", DEFAULT_BATCH_SIZE)),
        ),
    )

    report = build_report(request)
    runtime = request.runtime_config

    print(f"Смотрю папку: {runtime.fit_dir}")
    print(f"Режим плавания: {report['filters']['swim_mode']}")
    print(f"Период: {report['filters']['period_label']}")
    print(f"Дистанции: {', '.join(map(str, report['filters']['target_distances']))}")
    print(f"Процессов: {report['dataset_meta']['max_workers']}, размер пачки: {report['dataset_meta']['batch_size']}")
    print(f"Найдено FIT-файлов: {report['dataset_meta']['total_files']}")
    print(f"Из кэша: {report['dataset_meta']['cached_files']}/{report['dataset_meta']['total_files']} файлов")
    print(f"Найдено отрезков: {report['overview']['intervals']}")
    print(f"Детальная таблица: {runtime.detail_csv}")
    print(f"Сводная таблица:   {runtime.summary_csv}")
    print(f"Файл кэша:         {runtime.cache_file}")

    print("\nТайминг:")
    for key, value in report["dataset_meta"]["timings"].items():
        print(f"{key:>14}: {value}")

    if not report["summary"]:
        print("\nНичего не найдено.")
        return

    print("\nКраткая сводка:")
    for row in report["summary"]:
        print(
            f'{row["distance_m"]:>4} м | '
            f'{row["count"]:>3} шт | '
            f'ср. время {row["avg_time"]:>7} | '
            f'лучшее {row["best_time"]:>7} | '
            f'ср. темп {row["avg_pace_100m"]:>9} | '
            f'лучший темп {row["best_pace_100m"]:>9} | '
            f'дата лучшего темпа {row["best_pace_date"]}'
        )

