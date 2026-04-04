import json
from collections import defaultdict
from datetime import date, datetime, timedelta

from garmin_dashboard.core.config import RESOURCES_DIR, ReportRequest
from garmin_dashboard.core.dataset import write_detail_csv, write_summary_csv
from garmin_dashboard.core.db_ingest import load_report_rows
from garmin_dashboard.core.fit_parser import summary_distance
from garmin_dashboard.core.utils import format_duration, norm, pace_str, pace_str_precise, to_datetime


def resource_dir_label(path) -> str:
    try:
        return str(path.relative_to(RESOURCES_DIR))
    except Exception:
        return str(path)


def row_activity_date(row: dict):
    for key in ("activity_date", "lap_start", "lap_end"):
        dt = to_datetime(row.get(key))
        if dt:
            return dt.date()
    return None


def row_matches_interval_config(row: dict, request: ReportRequest) -> bool:
    distance = summary_distance(
        float(row.get("raw_distance_m") or row.get("distance_m") or 0),
        str(row.get("stroke") or ""),
        str(row.get("swim_type") or ""),
        request.interval_config,
    )
    if distance is None:
        return False
    if distance not in request.interval_config.target_distances:
        long_min_distance = int(round(request.interval_config.long_freestyle_min_distance_m))
        if not (
            distance > request.interval_config.long_freestyle_min_distance_m
            and long_min_distance in request.interval_config.target_distances
        ):
            return False
    row["distance_m"] = distance
    return True


def is_long_distance_selected(distance: int, request: ReportRequest) -> bool:
    long_min_distance = int(round(request.interval_config.long_freestyle_min_distance_m))
    return (
        request.swim_mode == "open_water"
        and
        distance > request.interval_config.long_freestyle_min_distance_m
        and long_min_distance in request.interval_config.target_distances
    )


def row_matches_requested_distance_group(row: dict, request: ReportRequest) -> bool:
    distance = int(row.get("distance_m") or 0)
    if distance in request.interval_config.target_distances:
        return True
    if is_long_distance_selected(distance, request):
        return True
    return False


def resolve_period(period: str = "current_year", days: int | None = None, today: date | None = None):
    today = today or datetime.now().date()
    period = norm(period or "current_year")

    if days is not None:
        if days < 0:
            raise ValueError("days не может быть отрицательным")
        return today - timedelta(days=days), today, f"Последние {days} дней"

    if period in {"all", "full"}:
        return None, None, "Все данные"
    if period in {"year", "365"}:
        return today - timedelta(days=365), today, "Последний год"
    if period in {"quarter", "90"}:
        return today - timedelta(days=90), today, "Последние 90 дней"
    if period in {"month", "30"}:
        return today - timedelta(days=30), today, "Последние 30 дней"
    if period in {"current_year", "year_to_date", "ytd"}:
        start = today.replace(month=1, day=1)
        return start, today, "Текущий год"
    if period == "current_month":
        start = today.replace(day=1)
        return start, today, "Текущий месяц"
    if period == "last_month":
        current_month_start = today.replace(day=1)
        end = current_month_start - timedelta(days=1)
        start = end.replace(day=1)
        return start, end, "Прошлый месяц"
    raise ValueError("Неизвестный период")


def row_matches_filters(row: dict, start_date: date | None, end_date: date | None, swim_mode: str):
    if swim_mode not in {"all", "pool", "open_water"}:
        raise ValueError("SWIM_MODE должен быть одним из: all, pool, open_water")

    if swim_mode != "all" and row.get("swim_type") != swim_mode:
        return False

    if start_date is None and end_date is None:
        return True

    row_date = row_activity_date(row)
    if row_date is None:
        return False

    if start_date is not None and row_date < start_date:
        return False
    if end_date is not None and row_date > end_date:
        return False
    return True


def best_pace_date_text(row: dict) -> str:
    for key in ("activity_date", "lap_start", "lap_end"):
        value = str(row.get(key, "") or "").strip()
        if value:
            return value[:10]
    return ""


def middle_half_rows(rows: list[dict]) -> list[dict]:
    if not rows:
        return []
    if len(rows) <= 4:
        return list(rows)

    sorted_rows = sorted(
        rows,
        key=lambda r: (
            -r["time_s"],
            r["time_s"],
            str(r.get("activity_date", "")),
            str(r.get("lap_start", "")),
            str(r.get("file_name", "")),
        ),
    )
    count = len(sorted_rows)
    start = count // 4
    end = count - (count // 4)
    middle_rows = sorted_rows[start:end]
    return middle_rows or sorted_rows


def build_summary(rows: list[dict]) -> list[dict]:
    groups = defaultdict(list)
    for row in rows:
        groups[row["distance_m"]].append(row)

    summary = []
    for distance in sorted(groups):
        group = groups[distance]
        count = len(group)
        total_time = sum(r["time_s"] for r in group)
        avg_time = total_time / count
        best_time = min(r["time_s"] for r in group)
        avg_pace = (avg_time / distance) * 100.0 if distance else 0.0
        middle_rows = middle_half_rows(group)
        middle_count = len(middle_rows)
        middle_total_time = sum(r["time_s"] for r in middle_rows)
        middle_avg_time = middle_total_time / middle_count if middle_count else 0.0
        middle_pace = (middle_avg_time / distance) * 100.0 if distance else 0.0

        best_pace_row = min(
            group,
            key=lambda r: (
                r["pace_100m_s"],
                str(r.get("activity_date", "")),
                str(r.get("lap_start", "")),
                str(r.get("file_name", "")),
            ),
        )

        swim_types = sorted({row["swim_type"] for row in group if row.get("swim_type")})
        strokes = sorted({row["stroke"] for row in group if row.get("stroke")})

        summary.append({
            "distance_m": distance,
            "count": count,
            "total_distance_m": round(distance * count, 2),
            "avg_time_s": round(avg_time, 2),
            "avg_time": format_duration(avg_time),
            "best_time_s": round(best_time, 2),
            "best_time": format_duration(best_time),
            "avg_pace_100m_s": round(avg_pace, 2),
            "avg_pace_100m": pace_str_precise(avg_pace),
            "best_pace_100m_s": round(best_pace_row["pace_100m_s"], 2),
            "best_pace_100m": pace_str(best_pace_row["pace_100m_s"]),
            "best_pace_date": best_pace_date_text(best_pace_row),
            "middle_count": middle_count,
            "middle_pace_100m_s": round(middle_pace, 2),
            "middle_pace_100m": pace_str_precise(middle_pace),
            "swim_types": ", ".join(swim_types),
            "strokes": ", ".join(strokes),
        })

    return summary


def add_summary_columns_to_details(rows: list[dict], summary_rows: list[dict]) -> list[dict]:
    summary_by_distance = {row["distance_m"]: row for row in summary_rows}
    enriched = []
    for row in rows:
        summary = summary_by_distance.get(row["distance_m"], {})
        enriched.append({
            **row,
            "avg_pace_for_distance_s": summary.get("avg_pace_100m_s", ""),
            "avg_pace_for_distance": summary.get("avg_pace_100m", ""),
            "middle_pace_for_distance_s": summary.get("middle_pace_100m_s", ""),
            "middle_pace_for_distance": summary.get("middle_pace_100m", ""),
            "best_pace_for_distance_s": summary.get("best_pace_100m_s", ""),
            "best_pace_for_distance": summary.get("best_pace_100m", ""),
            "best_pace_date_for_distance": summary.get("best_pace_date", ""),
        })
    return enriched


def build_workout_groups(rows: list[dict]) -> list[dict]:
    groups = defaultdict(list)
    for row in rows:
        workout_key = row.get("activity_key") or row.get("activity_date", "")[:10] or row.get("lap_start", "")[:10] or "unknown"
        groups[workout_key].append(row)

    workouts = []
    def workout_sort_key(group: list[dict]) -> str:
        sample = group[0] if group else {}
        return sample.get("activity_date", "") or sample.get("lap_start", "") or ""

    sorted_groups = sorted(groups.values(), key=workout_sort_key, reverse=True)
    for group in sorted_groups:
        sample = group[0]
        workout_date = (sample.get("activity_date", "") or sample.get("lap_start", "") or "unknown")[:10]
        total_distance = max((float(r.get("workout_total_distance_m", 0) or 0) for r in group), default=0.0)
        total_time = max((float(r.get("workout_total_time_s", 0) or 0) for r in group), default=0.0)
        best_pace = min((r["pace_100m_s"] for r in group), default=0.0)
        swim_types = sorted({r["swim_type"] for r in group if r.get("swim_type")})
        workouts.append({
            "date": workout_date,
            "intervals": len(group),
            "total_distance_m": round(total_distance, 2),
            "total_time_s": round(total_time, 2),
            "total_time": format_duration(total_time),
            "best_pace_100m_s": round(best_pace, 2),
            "best_pace_100m": pace_str(best_pace) if best_pace else "",
            "swim_types": ", ".join(swim_types),
        })
    return workouts


def build_report(request: ReportRequest) -> dict:
    if request.owner_account_id is None:
        raise ValueError("owner_account_id is required")
    start_date, end_date, period_label = resolve_period(period=request.period, days=request.days)
    filtered_rows, db_meta = load_report_rows(
        runtime_config=request.runtime_config,
        owner_account_id=request.owner_account_id,
        swim_mode=request.swim_mode,
        start_date=start_date.isoformat() if start_date else "",
        end_date=end_date.isoformat() if end_date else "",
    )
    filtered_rows = [
        row for row in filtered_rows
        if row_matches_filters(row, start_date=start_date, end_date=end_date, swim_mode=request.swim_mode)
    ]
    filtered_rows = [row for row in filtered_rows if row_matches_interval_config(row, request)]
    filtered_rows = [row for row in filtered_rows if row_matches_requested_distance_group(row, request)]
    filtered_rows.sort(key=lambda r: (r["activity_date"], r["lap_start"], r["file_name"]))

    summary_rows = build_summary(filtered_rows) if filtered_rows else []
    detail_rows = add_summary_columns_to_details(filtered_rows, summary_rows) if filtered_rows else []
    workouts = build_workout_groups(filtered_rows) if filtered_rows else []

    total_distance = sum(row["distance_m"] for row in filtered_rows)
    total_time = sum(row["time_s"] for row in filtered_rows)
    best_pace = min((row["pace_100m_s"] for row in filtered_rows), default=0.0)
    avg_pace = (total_time / total_distance) * 100.0 if total_distance else 0.0
    swim_types = sorted({row["swim_type"] for row in filtered_rows if row.get("swim_type")})
    strokes = sorted({row["stroke"] for row in filtered_rows if row.get("stroke")})

    if request.persist_csv:
        write_detail_csv(detail_rows, request.runtime_config.detail_csv)
        write_summary_csv(summary_rows, request.runtime_config.summary_csv)

    dataset_meta = {
        "fit_dir": resource_dir_label(request.runtime_config.fit_dir),
        "cached_files": 0,
        "processed_files": 0,
        "processed_rows": 0,
        "deleted_files": 0,
        "ignored_files": db_meta.get("ignored_files", 0),
        "error_files": db_meta.get("error_files", 0),
        "duplicate_files": db_meta.get("duplicate_files", 0),
        "ready_files": db_meta.get("ready_files", 0),
        "max_workers": request.runtime_config.max_workers,
        "batch_size": request.runtime_config.batch_size,
        "timings": {},
        "db_total_files": db_meta.get("total_files", 0),
        "db_ready_files": db_meta.get("ready_files", 0),
        "db_ignored_files": db_meta.get("ignored_files", 0),
        "db_error_files": db_meta.get("error_files", 0),
        "db_duplicate_files": db_meta.get("duplicate_files", 0),
        "db_total_rows": db_meta.get("total_rows", 0),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    }

    return {
        "filters": {
            "swim_mode": request.swim_mode,
            "period": request.period,
            "days": request.days,
            "resource_dir": "user_uploads",
            "period_label": period_label,
            "date_start": start_date.isoformat() if start_date else "",
            "date_end": end_date.isoformat() if end_date else "",
            "target_distances": list(request.interval_config.target_distances),
            "long_freestyle_min_distance_m": request.interval_config.long_freestyle_min_distance_m,
        },
        "dataset_meta": dataset_meta,
        "overview": {
            "intervals": len(filtered_rows),
            "workouts": len(workouts),
            "total_distance_m": round(total_distance, 2),
            "total_time_s": round(total_time, 2),
            "total_time": format_duration(total_time) if total_time else "0:00",
            "avg_pace_100m_s": round(avg_pace, 2),
            "avg_pace_100m": pace_str(avg_pace) if total_distance else "",
            "best_pace_100m_s": round(best_pace, 2),
            "best_pace_100m": pace_str(best_pace) if best_pace else "",
            "swim_types": swim_types,
            "strokes": strokes,
        },
        "summary": summary_rows,
        "workouts": workouts,
        "details": detail_rows,
    }


def serialize_report(report: dict) -> str:
    return json.dumps(report, ensure_ascii=False)
