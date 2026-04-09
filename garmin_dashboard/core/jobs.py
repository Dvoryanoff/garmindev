from __future__ import annotations

import hashlib
import json
import traceback
import queue
import shutil
import threading
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from pathlib import Path
from time import perf_counter

from .config import MIN_FREE_DISK_MB, PARSER_VERSION, RuntimeConfig
from .db import Database, json_dumps, json_loads
from .db_ingest import parse_fit_file_to_activity, update_monthly_history_for_intervals


_JOB_QUEUE: "queue.Queue[tuple[str, int]]" = queue.Queue()
_WORKERS_STARTED: set[str] = set()
_WORKER_LOCK = threading.Lock()
_SCHEMA_READY_DATABASES: set[str] = set()
_SCHEMA_READY_LOCK = threading.Lock()
_JOB_PROGRESS_EVERY = 50


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


def ensure_job_schema(database_url: str) -> None:
    if database_url in _SCHEMA_READY_DATABASES:
        return
    with _SCHEMA_READY_LOCK:
        if database_url in _SCHEMA_READY_DATABASES:
            return
        Database(database_url).init_schema()
        _SCHEMA_READY_DATABASES.add(database_url)


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
        except Exception as exc:
            # Keep the worker alive even if one job crashes unexpectedly.
            print(f"[jobs] Worker crashed while processing job #{job_id}: {exc}")
            traceback.print_exc()
            try:
                db = Database(database_url)
                owner_account_id = None
                with db.transaction() as conn:
                    job = db.get_background_job(conn, job_id)
                    if job and job.get("owner_account_id") is not None:
                        owner_account_id = int(job["owner_account_id"])
                _finalize_job_failure(db, job_id, owner_account_id, str(exc))
            except Exception as finalize_exc:
                print(f"[jobs] Failed to finalize job #{job_id} after worker crash: {finalize_exc}")
                traceback.print_exc()
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


def _parse_uploaded_fit_path(path_str: str):
    path = Path(path_str)
    try:
        return path_str, parse_fit_file_to_activity(path), ""
    except Exception as exc:
        return path_str, None, str(exc)


def _chunked(seq: list[dict], size: int):
    for index in range(0, len(seq), size):
        yield seq[index:index + size]


def _should_update_progress(completed: int, total: int, *, force: bool = False) -> bool:
    if force or completed >= total:
        return True
    return completed % _JOB_PROGRESS_EVERY == 0


def _parse_pending_files(pending_files: list[dict], runtime: RuntimeConfig):
    if not pending_files:
        return
    paths = [str(item["target_path"]) for item in pending_files]
    if len(paths) == 1:
        yield _parse_uploaded_fit_path(paths[0])
        return
    max_workers = max(1, int(runtime.max_workers or 1))
    chunksize = max(1, int(runtime.batch_size or 100) // 4)
    try:
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            for path_str, parsed, error_text in executor.map(_parse_uploaded_fit_path, paths, chunksize=chunksize):
                yield path_str, parsed, error_text
    except Exception:
        for path_str in paths:
            yield _parse_uploaded_fit_path(path_str)


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
    ensure_job_schema(database_url)
    db = Database(database_url)
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
        pending_files: list[dict] = []

        total_files = max(1, len(files))
        for index, file_item in enumerate(files, start=1):
            original_name = str(file_item.get("name") or "")
            content = bytes.fromhex(file_item.get("content_hex") or "")
            file_hash = hashlib.sha256(content).hexdigest()
            stored_name = f"{file_hash[:16]}_{Path(original_name).name}"
            target_path = upload_root / stored_name
            now_text = iso_now()

            with db.transaction() as conn:
                existing_hash = db.find_existing_file_by_hash(conn, owner_account_id, file_hash)
                if existing_hash:
                    skipped += 1
                    if _should_update_progress(index, total_files):
                        progress = int(index / total_files * 15)
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

            target_path.write_bytes(content)
            stat = target_path.stat()
            pending_files.append(
                {
                    "index": index,
                    "original_name": original_name,
                    "file_hash": file_hash,
                    "stored_name": stored_name,
                    "target_path": target_path,
                    "file_size": stat.st_size,
                    "mtime_ns": stat.st_mtime_ns,
                    "now_text": now_text,
                }
            )

        parsed_by_path = {}
        for path_str, parsed, error_text in _parse_pending_files(pending_files, runtime):
            parsed_by_path[path_str] = (parsed, error_text)

        completed_files = skipped
        ingest_batches = list(_chunked(pending_files, max(1, int(runtime.batch_size or 100))))
        for batch in ingest_batches:
            with db.transaction() as conn:
                for item in batch:
                    parsed, parse_error = parsed_by_path.get(str(item["target_path"]), (None, "parse result missing"))
                    if parsed is None:
                        db.upsert_source_file(
                            conn,
                            owner_account_id=owner_account_id,
                            file_path=str(item["target_path"].resolve()),
                            file_name=item["stored_name"],
                            original_file_name=item["original_name"],
                            file_hash=item["file_hash"],
                            file_size=item["file_size"],
                            mtime_ns=item["mtime_ns"],
                            parser_version=PARSER_VERSION,
                            parse_status="error",
                            error_text=parse_error,
                            activity_key="",
                            uploaded_at=item["now_text"],
                            ingested_at=item["now_text"],
                        )
                        errors += 1
                        completed_files += 1
                        continue

                    source_file_id = db.upsert_source_file(
                        conn,
                        owner_account_id=owner_account_id,
                        file_path=str(item["target_path"].resolve()),
                        file_name=item["stored_name"],
                        original_file_name=item["original_name"],
                        file_hash=item["file_hash"],
                        file_size=item["file_size"],
                        mtime_ns=item["mtime_ns"],
                        parser_version=PARSER_VERSION,
                        parse_status=parsed["status"],
                        error_text=parsed["error_text"],
                        activity_key=parsed["activity_key"],
                        uploaded_at=item["now_text"],
                        ingested_at=item["now_text"],
                    )
                    if parsed["activity"]:
                        existing_activity = db.fetch_activity_by_key(conn, owner_account_id, parsed["activity"]["activity_key"])
                        if existing_activity and int(existing_activity["source_file_id"]) != source_file_id:
                            db.upsert_source_file(
                                conn,
                                owner_account_id=owner_account_id,
                                file_path=str(item["target_path"].resolve()),
                                file_name=item["stored_name"],
                                original_file_name=item["original_name"],
                                file_hash=item["file_hash"],
                                file_size=item["file_size"],
                                mtime_ns=item["mtime_ns"],
                                parser_version=PARSER_VERSION,
                                parse_status="duplicate",
                                error_text="duplicate activity_key",
                                activity_key=parsed["activity"]["activity_key"],
                                uploaded_at=item["now_text"],
                                ingested_at=item["now_text"],
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
                    completed_files += 1

                if _should_update_progress(completed_files, total_files, force=True):
                    db.update_background_job(
                        conn,
                        job_id,
                        processed_files=processed,
                        skipped_files=skipped,
                        duplicate_files=duplicates,
                        error_files=errors,
                        parsed_rows=parsed_rows,
                        progress_percent=max(15, int(completed_files / total_files * 80)),
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
