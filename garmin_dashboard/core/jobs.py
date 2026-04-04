from __future__ import annotations

import hashlib
import json
import queue
import shutil
import threading
from datetime import datetime
from pathlib import Path
from time import perf_counter

from .config import MIN_FREE_DISK_MB, PARSER_VERSION, RuntimeConfig
from .db import Database, json_dumps, json_loads
from .db_ingest import parse_fit_file_to_activity, update_monthly_history_for_intervals


_JOB_QUEUE: "queue.Queue[tuple[str, int]]" = queue.Queue()
_WORKERS_STARTED: set[str] = set()
_WORKER_LOCK = threading.Lock()


def iso_now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def ensure_workers(database_url: str) -> None:
    if database_url in _WORKERS_STARTED:
        return
    with _WORKER_LOCK:
        if database_url in _WORKERS_STARTED:
            return
        thread = threading.Thread(target=_worker_loop, args=(database_url,), daemon=True)
        thread.start()
        _WORKERS_STARTED.add(database_url)


def enqueue_job(database_url: str, job_id: int) -> None:
    ensure_workers(database_url)
    _JOB_QUEUE.put((database_url, job_id))


def _check_free_disk(upload_dir: Path) -> None:
    usage = shutil.disk_usage(upload_dir)
    free_mb = usage.free / (1024 * 1024)
    if free_mb < MIN_FREE_DISK_MB:
        raise RuntimeError(f"Недостаточно свободного места для ingest: осталось {free_mb:.0f} MB")


def _worker_loop(database_url: str) -> None:
    while True:
        queued_database_url, job_id = _JOB_QUEUE.get()
        if queued_database_url != database_url:
            _JOB_QUEUE.put((queued_database_url, job_id))
            _JOB_QUEUE.task_done()
            continue
        try:
            process_job(database_url, job_id)
        except Exception:
            # Keep the worker alive even if one job crashes unexpectedly.
            pass
        finally:
            _JOB_QUEUE.task_done()


def _compact_payload(payload: dict) -> str:
    files = list(payload.get("files") or [])
    compact_files = [
        {
            "name": str(file_item.get("name") or ""),
            "size_bytes": len(bytes.fromhex(str(file_item.get("content_hex") or ""))),
        }
        for file_item in files
    ]
    return json.dumps({"files": compact_files}, ensure_ascii=False)


def _finalize_job_failure(db: Database, job_id: int, owner_account_id: int | None, error_text: str) -> None:
    with db.transaction() as conn:
        db.update_background_job(
            conn,
            job_id,
            status="failed",
            stage="failed",
            error_text=error_text,
            finished_at=iso_now(),
            progress_percent=100,
        )
        db.append_audit_log(
            conn,
            actor_account_id=owner_account_id,
            target_account_id=owner_account_id,
            event_type="job_failed",
            payload_json=json_dumps({"job_id": job_id, "error": error_text}),
            created_at=iso_now(),
        )


def _process_monthly_history_job(db: Database, job_id: int, job: dict) -> None:
    owner_account_id = int(job["owner_account_id"])
    with db.transaction() as conn:
        db.update_background_job(
            conn,
            job_id,
            status="running",
            stage="monthly_history",
            started_at=iso_now(),
            progress_percent=10,
        )
    with db.transaction() as conn:
        rows = db.fetch_interval_rows(conn, owner_account_id=owner_account_id, swim_mode="all", start_date="", end_date="")
        update_monthly_history_for_intervals(db, conn, owner_account_id, rows)
        db.update_background_job(
            conn,
            job_id,
            status="done",
            stage="done",
            parsed_rows=len(rows),
            progress_percent=100,
            finished_at=iso_now(),
        )
        db.append_audit_log(
            conn,
            actor_account_id=owner_account_id,
            target_account_id=owner_account_id,
            event_type="monthly_history_refreshed",
            payload_json=json_dumps({"job_id": job_id, "rows": len(rows)}),
            created_at=iso_now(),
        )


def process_job(database_url: str, job_id: int) -> None:
    db = Database(database_url)
    db.init_schema()
    started = perf_counter()
    owner_account_id: int | None = None
    with db.transaction() as conn:
        job = db.get_background_job(conn, job_id)
        if not job or job.get("status") not in {"queued", "running"}:
            return
        owner_account_id = int(job["owner_account_id"]) if job.get("owner_account_id") is not None else None
        if job.get("job_type") == "monthly_history":
            _process_monthly_history_job(db, job_id, job)
            return
        payload = json_loads(job.get("payload_json")) or {}
        runtime = RuntimeConfig()
        db.update_background_job(conn, job_id, status="running", stage="ingest", started_at=iso_now(), progress_percent=1)
        db.append_audit_log(
            conn,
            actor_account_id=owner_account_id,
            target_account_id=owner_account_id,
            event_type="job_started",
            payload_json=json_dumps({"job_id": job_id, "job_type": job.get("job_type")}),
            created_at=iso_now(),
        )

    try:
        upload_dir = RuntimeConfig().upload_dir
        upload_dir.mkdir(parents=True, exist_ok=True)
        _check_free_disk(upload_dir)

        payload = payload if isinstance(payload, dict) else {}
        owner_account_id = int(job["owner_account_id"])
        files = list(payload.get("files") or [])
        upload_root = runtime.upload_dir / f"user_{owner_account_id}"
        upload_root.mkdir(parents=True, exist_ok=True)

        processed = 0
        skipped = 0
        duplicates = 0
        errors = 0
        parsed_rows = 0

        total_files = max(1, len(files))
        for index, file_item in enumerate(files, start=1):
            original_name = str(file_item.get("name") or "")
            content = bytes.fromhex(file_item.get("content_hex") or "")
            file_hash = hashlib.sha256(content).hexdigest()
            stored_name = f"{file_hash[:16]}_{Path(original_name).name}"
            target_path = upload_root / stored_name
            target_path.write_bytes(content)
            stat = target_path.stat()
            now_text = iso_now()

            with db.transaction() as conn:
                existing_hash = db.find_existing_file_by_hash(conn, owner_account_id, file_hash)
                if existing_hash:
                    skipped += 1
                    progress = int(index / total_files * 80)
                    db.update_background_job(
                        conn,
                        job_id,
                        processed_files=processed,
                        skipped_files=skipped,
                        duplicate_files=duplicates,
                        error_files=errors,
                        parsed_rows=parsed_rows,
                        progress_percent=progress,
                    )
                    continue

            with db.transaction() as conn:
                try:
                    parsed = parse_fit_file_to_activity(target_path)
                except Exception as exc:
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
                    db.update_background_job(
                        conn,
                        job_id,
                        processed_files=processed,
                        skipped_files=skipped,
                        duplicate_files=duplicates,
                        error_files=errors,
                        parsed_rows=parsed_rows,
                        progress_percent=int(index / total_files * 80),
                        error_text=str(exc),
                    )
                    continue

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
                        parsed_rows += len(parsed["intervals"])
                processed += 1
                db.update_background_job(
                    conn,
                    job_id,
                    processed_files=processed,
                    skipped_files=skipped,
                    duplicate_files=duplicates,
                    error_files=errors,
                    parsed_rows=parsed_rows,
                    progress_percent=int(index / total_files * 80),
                )

        status = "done" if errors == 0 else ("partial" if processed > 0 or skipped > 0 or duplicates > 0 else "failed")
        compact_payload = json.loads(_compact_payload(payload))
        compact_payload["timings"] = {"total": f"{perf_counter() - started:.2f}s"}

        with db.transaction() as conn:
            db.update_background_job(
                conn,
                job_id,
                status=status,
                stage="done",
                processed_files=processed,
                skipped_files=skipped,
                duplicate_files=duplicates,
                error_files=errors,
                parsed_rows=parsed_rows,
                progress_percent=100,
                finished_at=iso_now(),
                payload_json=json.dumps(compact_payload, ensure_ascii=False),
            )
            if processed > 0:
                monthly_job_id = db.create_background_job(
                    conn,
                    owner_account_id=owner_account_id,
                    job_type="monthly_history",
                    total_files=processed,
                    payload_json=json.dumps({"source_job_id": job_id}, ensure_ascii=False),
                    created_at=iso_now(),
                )
            else:
                monthly_job_id = None
            db.append_audit_log(
                conn,
                actor_account_id=owner_account_id,
                target_account_id=owner_account_id,
                event_type="job_finished",
                payload_json=json_dumps(
                    {
                        "job_id": job_id,
                        "status": status,
                        "processed_files": processed,
                        "skipped_files": skipped,
                        "duplicate_files": duplicates,
                        "error_files": errors,
                        "monthly_job_id": monthly_job_id,
                    }
                ),
                created_at=iso_now(),
            )
        if monthly_job_id:
            enqueue_job(database_url, monthly_job_id)
    except Exception as exc:
        _finalize_job_failure(db, job_id, owner_account_id, str(exc))
