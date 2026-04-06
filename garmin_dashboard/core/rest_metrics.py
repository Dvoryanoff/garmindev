from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable

from .config import IntervalConfig
from .fit_parser import get_swim_type, map_stroke_label, summary_distance
from .utils import format_duration, to_datetime


COMPETITIVE_STROKES = {"freestyle", "backstroke", "breaststroke", "butterfly"}
REST_CUTOFF_SECONDS = 120.0


def sorted_rows(rows: list[dict]) -> list[dict]:
    return sorted(
        rows,
        key=lambda row: (
            str(row.get("activity_date", "")),
            str(row.get("lap_start", "")),
            str(row.get("lap_end", "")),
            str(row.get("file_name", "")),
        ),
    )


def rest_seconds_between(previous_row: dict, next_row: dict) -> float | None:
    if str(previous_row.get("activity_key") or "") != str(next_row.get("activity_key") or ""):
        return None
    previous_end = to_datetime(previous_row.get("lap_end"))
    next_start = to_datetime(next_row.get("lap_start"))
    if not previous_end or not next_start:
        return None
    rest_seconds = (next_start - previous_end).total_seconds()
    if rest_seconds < 0:
        return None
    return round(rest_seconds, 2)


def mean_seconds(values: list[float]) -> float | None:
    if not values:
        return None
    return round(sum(values) / len(values), 2)


def format_rest(seconds: float | None) -> str:
    if seconds is None:
        return ""
    return format_duration(seconds)


def collect_adjacent_rest_values(
    rows: list[dict],
    pair_filter: Callable[[dict, dict], bool],
) -> list[float]:
    ordered_rows = sorted_rows(rows)
    values: list[float] = []
    for index in range(len(ordered_rows) - 1):
        previous_row = ordered_rows[index]
        next_row = ordered_rows[index + 1]
        if not pair_filter(previous_row, next_row):
            continue
        rest_seconds = rest_seconds_between(previous_row, next_row)
        if rest_seconds is None:
            continue
        values.append(rest_seconds)
    return values


def is_named_pool_interval(row: dict) -> bool:
    return str(row.get("swim_type") or "") == "pool" and str(row.get("stroke") or "") in COMPETITIVE_STROKES


def is_named_style_interval(row: dict) -> bool:
    return str(row.get("stroke") or "") in COMPETITIVE_STROKES


def is_pool_swim_interval_without_drills(row: dict) -> bool:
    stroke = str(row.get("stroke") or "")
    swim_type = str(row.get("swim_type") or "")
    return swim_type == "pool" and stroke not in {"", "drill", "unknown"}


def _positive_laps_from_messages(messages: dict, interval_config: IntervalConfig | None = None) -> list[dict]:
    swim_type = get_swim_type(messages)
    if swim_type != "pool":
        return []
    laps = []
    for lap in messages.get("lap_mesgs", []) or []:
        distance_raw = lap.get("total_distance")
        if distance_raw in (None, "", 0, 0.0):
            continue
        lap_start = to_datetime(lap.get("start_time"))
        lap_end = to_datetime(lap.get("timestamp"))
        if not lap_start or not lap_end:
            continue
        try:
            distance_m = float(distance_raw)
        except Exception:
            continue
        if distance_m <= 0:
            continue
        stroke = map_stroke_label(lap.get("swim_stroke"))
        nominal_distance = None
        if interval_config is not None:
            nominal_distance = summary_distance(distance_m, stroke, swim_type="pool", interval_config=interval_config)
        laps.append({
            "lap_start": lap_start.isoformat(sep=" "),
            "lap_end": lap_end.isoformat(sep=" "),
            "distance_m": distance_m,
            "stroke": stroke,
            "swim_type": "pool",
            "nominal_distance": nominal_distance,
        })
    return sorted_rows(laps)


def compute_summary_rest_by_distance_from_payloads(payloads: list[dict], interval_config: IntervalConfig) -> dict[int, float | None]:
    values_by_distance: dict[int, list[float]] = defaultdict(list)
    for payload in payloads:
        messages = ((payload or {}).get("messages") or {})
        positive_laps = _positive_laps_from_messages(messages, interval_config=interval_config)
        for index in range(len(positive_laps) - 1):
            previous_lap = positive_laps[index]
            next_lap = positive_laps[index + 1]
            if previous_lap.get("stroke") != "freestyle" or next_lap.get("stroke") != "freestyle":
                continue
            if previous_lap.get("nominal_distance") is None or previous_lap.get("nominal_distance") != next_lap.get("nominal_distance"):
                continue
            rest_seconds = rest_seconds_between(previous_lap, next_lap)
            if rest_seconds is None:
                continue
            if rest_seconds > REST_CUTOFF_SECONDS:
                continue
            values_by_distance[int(previous_lap["nominal_distance"])].append(rest_seconds)
    return {distance: mean_seconds(values) for distance, values in values_by_distance.items()}


def compute_workout_rest_stats_from_payloads(payloads_by_activity: dict[str, dict]) -> dict[str, dict]:
    result: dict[str, dict] = {}
    for activity_key, payload in payloads_by_activity.items():
        messages = ((payload or {}).get("messages") or {})
        positive_laps = _positive_laps_from_messages(messages)
        values = [
            rest_seconds
            for index in range(len(positive_laps) - 1)
            if (rest_seconds := rest_seconds_between(positive_laps[index], positive_laps[index + 1])) is not None
        ]
        result[activity_key] = {
            "avg_rest_s": mean_seconds(values),
            "long_rest_count": sum(1 for value in values if value > REST_CUTOFF_SECONDS),
        }
    return result


def compute_monthly_avg_rest_from_payloads(payloads: list[dict]) -> dict[tuple[int, int], float | None]:
    values_by_month: dict[tuple[int, int], list[float]] = defaultdict(list)
    for payload in payloads:
        messages = ((payload or {}).get("messages") or {})
        positive_laps = _positive_laps_from_messages(messages)
        for index in range(len(positive_laps) - 1):
            previous_lap = positive_laps[index]
            next_lap = positive_laps[index + 1]
            if not (is_named_style_interval(previous_lap) and is_named_style_interval(next_lap)):
                continue
            rest_seconds = rest_seconds_between(previous_lap, next_lap)
            if rest_seconds is None:
                continue
            if rest_seconds > REST_CUTOFF_SECONDS:
                continue
            dt = to_datetime(next_lap.get("lap_start"))
            if not dt:
                continue
            values_by_month[(dt.year, dt.month)].append(rest_seconds)
    return {key: mean_seconds(values) for key, values in values_by_month.items()}
