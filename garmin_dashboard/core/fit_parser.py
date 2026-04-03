import sys
from pathlib import Path

from .config import ALL_SUPPORTED_SUBSPORTS, IntervalConfig, OPEN_WATER_SUBSPORTS, POOL_SUBSPORTS
from .utils import ensure_local_venv_packages, format_duration, norm, pace_str, to_datetime

try:
    from garmin_fit_sdk import Decoder, Stream
except ModuleNotFoundError:
    ensure_local_venv_packages()
    from garmin_fit_sdk import Decoder, Stream


def decode_fit_file(fit_path: Path) -> dict:
    fit_bytes = fit_path.read_bytes()
    stream = Stream.from_byte_array(bytearray(fit_bytes))
    decoder = Decoder(stream)

    messages, errors = decoder.read(
        apply_scale_and_offset=True,
        convert_datetimes_to_dates=True,
        convert_types_to_strings=True,
        enable_crc_check=False,
        expand_sub_fields=False,
        expand_components=False,
        merge_heart_rates=False,
    )

    if errors:
        print(f"WARNING: ошибки декодирования {fit_path.name}: {errors}", file=sys.stderr)

    return messages


def get_swim_subsport(messages: dict) -> str:
    session_mesgs = messages.get("session_mesgs", [])
    if not session_mesgs:
        return ""
    return norm(session_mesgs[0].get("sub_sport"))


def get_swim_type(messages: dict) -> str:
    subsport = get_swim_subsport(messages)
    if subsport in OPEN_WATER_SUBSPORTS:
        return "open_water"
    if subsport in POOL_SUBSPORTS:
        return "pool"
    return ""


def is_supported_swim(messages: dict) -> bool:
    session_mesgs = messages.get("session_mesgs", [])
    if not session_mesgs:
        return False

    session = session_mesgs[0]
    sport = norm(session.get("sport"))
    sub_sport = norm(session.get("sub_sport"))

    return sport == "swimming" and sub_sport in ALL_SUPPORTED_SUBSPORTS


def get_activity_datetime(messages: dict):
    session_mesgs = messages.get("session_mesgs", [])
    if session_mesgs:
        session = session_mesgs[0]
        for key in ("start_time", "timestamp"):
            dt = to_datetime(session.get(key))
            if dt:
                return dt

    activity_mesgs = messages.get("activity_mesgs", [])
    if activity_mesgs:
        activity = activity_mesgs[0]
        dt = to_datetime(activity.get("timestamp"))
        if dt:
            return dt

    return None


def map_stroke_label(raw_stroke) -> str:
    stroke = norm(raw_stroke)

    if any(token in stroke for token in ("freestyle", "free", "crawl")):
        return "freestyle"
    if any(token in stroke for token in ("backstroke", "back")):
        return "backstroke"
    if any(token in stroke for token in ("breaststroke", "breast")):
        return "breaststroke"
    if any(token in stroke for token in ("butterfly", "fly")):
        return "butterfly"
    if any(token in stroke for token in ("medley", "mixed", "im")):
        return "mixed"
    if "drill" in stroke:
        return "drill"
    return stroke or "unknown"


def get_open_water_stroke(item: dict, session: dict | None = None) -> str:
    session = session or {}
    stroke = map_stroke_label(item.get("swim_stroke") or session.get("swim_stroke"))
    if stroke in {"", "drill"}:
        return "unknown"
    return stroke


def nearest_target_distance(distance_m: float, interval_config: IntervalConfig):
    for target in interval_config.target_distances:
        if abs(distance_m - target) <= interval_config.distance_tolerance_m:
            return target
    return None


def summary_distance(distance_m: float, stroke: str, swim_type: str, interval_config: IntervalConfig):
    target = nearest_target_distance(distance_m, interval_config=interval_config)
    if target is not None:
        return target

    if distance_m > interval_config.long_freestyle_min_distance_m:
        if swim_type == "open_water" and interval_config.allow_open_water_long:
            return int(round(distance_m))
        if swim_type == "pool" and stroke == "freestyle" and interval_config.allow_pool_long_freestyle:
            return int(round(distance_m))

    return None


def is_real_pool_swim_interval(lap: dict) -> bool:
    stroke = map_stroke_label(lap.get("swim_stroke"))

    if stroke in {"drill", "unknown", ""}:
        return False

    first_length_index = lap.get("first_length_index")
    num_lengths = lap.get("num_lengths")
    if first_length_index is None or num_lengths in (None, 0, 0.0):
        return False

    total_strokes = lap.get("total_strokes")
    total_cycles = lap.get("total_cycles")
    if total_strokes in (0, 0.0) and total_cycles in (0, 0.0):
        return False

    return True


def iter_target_swim_laps(messages: dict, interval_config: IntervalConfig):
    lap_mesgs = messages.get("lap_mesgs", []) or []
    session_mesgs = messages.get("session_mesgs", []) or []
    session = session_mesgs[0] if session_mesgs else {}

    swim_type = get_swim_type(messages)

    if swim_type == "open_water":
        sources = lap_mesgs if lap_mesgs else [session]

        for item in sources:
            distance_m = item.get("total_distance")
            timer_s = item.get("total_timer_time") or item.get("total_elapsed_time")

            if distance_m is None or timer_s is None:
                continue

            try:
                distance_m = float(distance_m)
                timer_s = float(timer_s)
            except Exception:
                continue

            if distance_m <= 0 or timer_s <= 0:
                continue

            stroke = get_open_water_stroke(item, session)
            distance_for_table = summary_distance(distance_m, stroke, swim_type="open_water", interval_config=interval_config)
            if distance_for_table is None:
                continue

            lap_start = to_datetime(item.get("start_time") or session.get("start_time"))
            lap_end = to_datetime(item.get("timestamp") or session.get("timestamp"))
            pace_100m_s = timer_s / distance_for_table * 100.0

            yield {
                "distance_m": distance_for_table,
                "raw_distance_m": round(distance_m, 2),
                "time_s": timer_s,
                "time_text": format_duration(timer_s),
                "pace_100m_s": pace_100m_s,
                "pace_100m": pace_str(pace_100m_s),
                "stroke": stroke,
                "lap_start": lap_start,
                "lap_end": lap_end,
                "swim_type": "open_water",
            }
        return

    for lap in lap_mesgs:
        distance_m = lap.get("total_distance")
        timer_s = lap.get("total_timer_time") or lap.get("total_elapsed_time")

        if distance_m is None or timer_s is None:
            continue

        try:
            distance_m = float(distance_m)
            timer_s = float(timer_s)
        except Exception:
            continue

        if not is_real_pool_swim_interval(lap):
            continue

        stroke = map_stroke_label(lap.get("swim_stroke"))
        distance_for_table = summary_distance(distance_m, stroke, swim_type="pool", interval_config=interval_config)
        if distance_for_table is None:
            continue

        lap_start = to_datetime(lap.get("start_time"))
        lap_end = to_datetime(lap.get("timestamp"))
        pace_100m_s = timer_s / distance_for_table * 100.0

        yield {
            "distance_m": distance_for_table,
            "raw_distance_m": round(distance_m, 2),
            "time_s": timer_s,
            "time_text": format_duration(timer_s),
            "pace_100m_s": pace_100m_s,
            "pace_100m": pace_str(pace_100m_s),
            "stroke": stroke,
            "lap_start": lap_start,
            "lap_end": lap_end,
            "swim_type": "pool",
        }

