from __future__ import annotations

import hashlib
from datetime import datetime
from pathlib import Path
from time import perf_counter

from .config import IntervalConfig, MONTHLY_FIXED_DISTANCES, PARSER_VERSION, RuntimeConfig
from .db import Database, json_dumps
from .fit_parser import (
    decode_fit_file,
    get_activity_datetime,
    get_activity_key,
    get_swim_subsport,
    get_swim_type,
    get_user_id,
    get_user_name,
    is_supported_swim,
    iter_target_swim_laps,
)
from .utils import format_elapsed, norm, pace_str_precise, to_datetime


INGEST_INTERVAL_CONFIG = IntervalConfig(
    target_distances=tuple(range(1, 10001)),
    distance_tolerance_m=0.5,
    long_freestyle_min_distance_m=1.0,
    allow_open_water_long=True,
    allow_pool_long_freestyle=True,
)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def iso_now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _safe_float(value) -> float:
    try:
        return round(float(value), 2)
    except Exception:
        return 0.0


def _messages_payload(messages: dict) -> dict:
    return {"messages": messages}


def parse_fit_file_to_activity(fit_path: Path) -> dict:
    messages = decode_fit_file(fit_path)
    if not is_supported_swim(messages):
        return {
            "status": "ignored",
            "error_text": "",
            "activity_key": "",
            "payload": _messages_payload(messages),
            "activity": None,
            "intervals": [],
        }

    activity_dt = get_activity_datetime(messages)
    activity_dt_text = activity_dt.isoformat(sep=" ") if activity_dt else ""
    activity_key = get_activity_key(messages, fit_path)
    user_id = get_user_id(messages) or "unknown"
    user_name = get_user_name(messages, fit_path)
    session = (messages.get("session_mesgs") or [{}])[0]
    workout_total_distance = _safe_float(session.get("total_distance"))
    workout_total_time = _safe_float(session.get("total_elapsed_time") or session.get("total_timer_time"))

    intervals = []
    for lap in iter_target_swim_laps(messages, interval_config=INGEST_INTERVAL_CONFIG):
        intervals.append({
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
            "workout_total_distance_m": workout_total_distance,
            "workout_total_time_s": workout_total_time,
            "stroke": lap["stroke"],
            "swim_type": lap["swim_type"],
            "pace_100m_s": round(lap["pace_100m_s"], 2),
            "pace_100m": lap["pace_100m"],
        })

    return {
        "status": "ready",
        "error_text": "",
        "activity_key": activity_key,
        "payload": _messages_payload(messages),
        "activity": {
            "activity_key": activity_key,
            "activity_date": activity_dt_text,
            "garmin_user_id": user_id,
            "garmin_user_name": user_name,
            "sport": norm(session.get("sport")),
            "sub_sport": get_swim_subsport(messages),
            "swim_type": get_swim_type(messages),
            "total_distance_m": workout_total_distance,
            "total_time_s": workout_total_time,
        },
        "intervals": intervals,
    }


def update_monthly_history_for_intervals(db: Database, conn, owner_account_id: int, intervals: list[dict]) -> None:
    best_by_key: dict[tuple[int, int, int], float] = {}
    for row in intervals:
        dt = to_datetime(row.get("activity_date") or row.get("lap_start") or row.get("lap_end"))
        if not dt:
            continue
        distance = int(row.get("distance_m") or 0)
        if distance not in MONTHLY_FIXED_DISTANCES:
            continue
        pace_s = float(row.get("pace_100m_s") or 0)
        if pace_s <= 0:
            continue
        key = (dt.year, dt.month, distance)
        current = best_by_key.get(key)
        if current is None or pace_s < current:
            best_by_key[key] = pace_s
    for (year, month, distance), pace_s in best_by_key.items():
        db.upsert_monthly_best(
            conn,
            owner_account_id=owner_account_id,
            year=year,
            month=month,
            distance_m=distance,
            best_pace_s=round(pace_s, 4),
            best_pace_text=pace_str_precise(pace_s).replace("/100m", ""),
        )


def ingest_uploaded_files(runtime_config: RuntimeConfig, owner_account_id: int, files: list[dict]) -> dict:
    db = Database(runtime_config.database_url)
    db.init_schema()
    upload_root = runtime_config.upload_dir / f"user_{owner_account_id}"
    upload_root.mkdir(parents=True, exist_ok=True)

    started = perf_counter()
    processed = 0
    skipped = 0
    duplicates = 0
    errors = 0
    parsed_rows = 0

    for file_item in files:
        original_name = file_item["name"]
        content = file_item["content"]
        file_hash = hashlib.sha256(content).hexdigest()
        stored_name = f"{file_hash[:16]}_{Path(original_name).name}"
        target_path = upload_root / stored_name

        with db.transaction() as conn:
            existing_hash = db.find_existing_file_by_hash(conn, owner_account_id, file_hash)
            if existing_hash:
                skipped += 1
                continue

        target_path.write_bytes(content)
        stat = target_path.stat()
        now_text = iso_now()

        try:
            parsed = parse_fit_file_to_activity(target_path)
        except Exception as exc:
            with db.transaction() as conn:
                db.upsert_source_file(
                    conn,
                    owner_account_id=owner_account_id,
                    file_path=str(target_path.resolve()),
                    file_name=stored_name,
                    original_file_name=original_name,
                    file_hash=file_hash,
                    file_size=stat.st_size,
                    mtime_ns=stat.st_mtime_ns,
                    parser_version=PARSER_VERSION,
                    parse_status="error",
                    error_text=str(exc),
                    activity_key="",
                    uploaded_at=now_text,
                    ingested_at=now_text,
                )
            errors += 1
            continue

        with db.transaction() as conn:
            source_file_id = db.upsert_source_file(
                conn,
                owner_account_id=owner_account_id,
                file_path=str(target_path.resolve()),
                file_name=stored_name,
                original_file_name=original_name,
                file_hash=file_hash,
                file_size=stat.st_size,
                mtime_ns=stat.st_mtime_ns,
                parser_version=PARSER_VERSION,
                parse_status=parsed["status"],
                error_text=parsed["error_text"],
                activity_key=parsed["activity_key"],
                uploaded_at=now_text,
                ingested_at=now_text,
            )
            if parsed["activity"]:
                existing_activity = db.fetch_activity_by_key(conn, owner_account_id, parsed["activity"]["activity_key"])
                if existing_activity and int(existing_activity["source_file_id"]) != source_file_id:
                    db.upsert_source_file(
                        conn,
                        owner_account_id=owner_account_id,
                        file_path=str(target_path.resolve()),
                        file_name=stored_name,
                        original_file_name=original_name,
                        file_hash=file_hash,
                        file_size=stat.st_size,
                        mtime_ns=stat.st_mtime_ns,
                        parser_version=PARSER_VERSION,
                        parse_status="duplicate",
                        error_text="duplicate activity_key",
                        activity_key=parsed["activity"]["activity_key"],
                        uploaded_at=now_text,
                        ingested_at=now_text,
                    )
                    duplicates += 1
                else:
                    db.replace_activity(
                        conn,
                        owner_account_id=owner_account_id,
                        source_file_id=source_file_id,
                        activity_key=parsed["activity"]["activity_key"],
                        activity_date=parsed["activity"]["activity_date"],
                        garmin_user_id=parsed["activity"]["garmin_user_id"],
                        garmin_user_name=parsed["activity"]["garmin_user_name"],
                        sport=parsed["activity"]["sport"],
                        sub_sport=parsed["activity"]["sub_sport"],
                        swim_type=parsed["activity"]["swim_type"],
                        total_distance_m=parsed["activity"]["total_distance_m"],
                        total_time_s=parsed["activity"]["total_time_s"],
                        raw_payload=json_dumps(parsed["payload"]),
                        intervals=parsed["intervals"],
                    )
                    update_monthly_history_for_intervals(db, conn, owner_account_id, parsed["intervals"])
                    parsed_rows += len(parsed["intervals"])
        processed += 1

    with db.transaction() as conn:
        meta = db.fetch_dataset_meta(conn, owner_account_id)

    return {
        "processed_files": processed,
        "skipped_files": skipped,
        "duplicate_files": duplicates,
        "error_files": errors,
        "parsed_rows": parsed_rows,
        "total_files": meta["total_files"],
        "ready_files": meta["ready_files"],
        "timings": {
            "total": format_elapsed(perf_counter() - started),
        },
    }


def load_report_rows(runtime_config: RuntimeConfig, owner_account_id: int, swim_mode: str, start_date: str, end_date: str) -> tuple[list[dict], dict]:
    db = Database(runtime_config.database_url)
    db.init_schema()
    with db.transaction() as conn:
        rows = db.fetch_interval_rows(
            conn,
            owner_account_id=owner_account_id,
            swim_mode=swim_mode,
            start_date=start_date,
            end_date=end_date,
        )
        meta = db.fetch_dataset_meta(conn, owner_account_id)
    return rows, meta


def load_monthly_history(runtime_config: RuntimeConfig, owner_account_id: int) -> list[dict]:
    db = Database(runtime_config.database_url)
    db.init_schema()
    with db.transaction() as conn:
        return db.fetch_monthly_history(conn, owner_account_id)
