import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from garmin_dashboard.app.server import resolve_registration_role, validate_upload_request
from garmin_dashboard.core.auth import hash_password, session_expiry
from garmin_dashboard.core.config import UPLOAD_MAX_FILES
from garmin_dashboard.core.config import RuntimeConfig
from garmin_dashboard.core.db import Database, json_loads
from garmin_dashboard.core.db_ingest import load_monthly_history
from garmin_dashboard.core.jobs import process_job


class JobsAndAuthTestCase(unittest.TestCase):
    def test_json_loads_accepts_already_decoded_json(self):
        payload = {"files": [{"name": "sample.fit"}]}
        self.assertEqual(json_loads(payload), payload)

    def test_background_ingest_compacts_payload_and_queues_monthly_history(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runtime = RuntimeConfig(
                database_url=f"sqlite:///{root / 'garmin_dashboard.sqlite'}",
                upload_dir=root / "uploads",
                db_auto_ingest=False,
            )
            db = Database(runtime.database_url)
            db.init_schema()
            with db.transaction() as conn:
                account_id = db.create_account(
                    conn,
                    email="jobs@example.com",
                    password_hash=hash_password("password123"),
                    first_name="Jobs",
                    last_name="User",
                    role="user",
                    created_at="2026-04-04 18:00:00",
                )
                job_id = db.create_background_job(
                    conn,
                    owner_account_id=account_id,
                    job_type="ingest",
                    total_files=1,
                    payload_json=json.dumps({"files": [{"name": "sample.fit", "content_hex": b"fit".hex()}]}),
                    created_at="2026-04-04 18:00:01",
                )

            parsed = {
                "status": "ready",
                "error_text": "",
                "activity_key": "user-1|2026-04-04T10:00:00",
                "payload": {"messages": {"session_mesgs": [{"sport": "swimming"}]}},
                "activity": {
                    "activity_key": "user-1|2026-04-04T10:00:00",
                    "activity_date": "2026-04-04 10:00:00",
                    "garmin_user_id": "user-1",
                    "garmin_user_name": "Jobs",
                    "sport": "swimming",
                    "sub_sport": "lap_swimming",
                    "swim_type": "pool",
                    "total_distance_m": 100.0,
                    "total_time_s": 90.0,
                },
                "intervals": [
                    {
                        "file_name": "sample.fit",
                        "activity_key": "user-1|2026-04-04T10:00:00",
                        "activity_date": "2026-04-04 10:00:00",
                        "user_id": "user-1",
                        "user_name": "Jobs",
                        "lap_start": "2026-04-04 10:00:00",
                        "lap_end": "2026-04-04 10:01:30",
                        "distance_m": 100,
                        "raw_distance_m": 100.0,
                        "time_s": 90.0,
                        "time_text": "1:30",
                        "workout_total_distance_m": 100.0,
                        "workout_total_time_s": 90.0,
                        "stroke": "freestyle",
                        "swim_type": "pool",
                        "pace_100m_s": 90.0,
                        "pace_100m": "1:30/100m",
                    }
                ],
            }

            with patch("garmin_dashboard.core.jobs.RuntimeConfig", return_value=runtime), patch(
                "garmin_dashboard.core.jobs.parse_fit_file_to_activity",
                return_value=parsed,
            ):
                process_job(runtime.database_url, job_id)

            with db.transaction() as conn:
                ingest_job = db.get_background_job(conn, job_id)
                jobs = db.list_background_jobs(conn, account_id, limit=10)

            self.assertEqual(ingest_job["status"], "done")
            self.assertNotIn("content_hex", ingest_job["payload_json"])

            monthly_jobs = [job for job in jobs if job["job_type"] == "monthly_history"]
            self.assertEqual(len(monthly_jobs), 1)
            monthly_job_id = int(monthly_jobs[0]["id"])

            with patch("garmin_dashboard.core.jobs.RuntimeConfig", return_value=runtime):
                process_job(runtime.database_url, monthly_job_id)

            rows = load_monthly_history(runtime, account_id)
            self.assertTrue(rows)

    def test_login_rate_limit_counter_tracks_failures(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runtime = RuntimeConfig(
                database_url=f"sqlite:///{root / 'garmin_dashboard.sqlite'}",
                upload_dir=root / "uploads",
                db_auto_ingest=False,
            )
            db = Database(runtime.database_url)
            db.init_schema()
            with db.transaction() as conn:
                db.record_login_attempt(conn, email="user@example.com", attempted_at="2026-04-04T18:00:00", was_success=False)
                db.record_login_attempt(conn, email="user@example.com", attempted_at="2026-04-04T18:01:00", was_success=False)
                db.record_login_attempt(conn, email="user@example.com", attempted_at="2026-04-04T18:02:00", was_success=True)
                failures = db.count_recent_failed_logins(
                    conn,
                    email="user@example.com",
                    attempted_after="2026-04-04T17:59:00",
                )
            self.assertEqual(failures, 2)

    def test_session_stores_login_day_and_expires_after_idle_timeout(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runtime = RuntimeConfig(
                database_url=f"sqlite:///{root / 'garmin_dashboard.sqlite'}",
                upload_dir=root / "uploads",
                db_auto_ingest=False,
            )
            db = Database(runtime.database_url)
            db.init_schema()
            with db.transaction() as conn:
                account_id = db.create_account(
                    conn,
                    email="idle@example.com",
                    password_hash=hash_password("password123"),
                    first_name="Idle",
                    last_name="User",
                    role="user",
                    created_at="2026-04-04 10:00:00",
                )
                token = "session-token"
                db.create_session(
                    conn,
                    account_id,
                    token,
                    "2026-04-04 10:00:00",
                    session_expiry(),
                    last_seen_at="2026-04-04 10:00:00",
                    login_day="2026-04-04",
                )
                session = db.find_account_by_session(conn, token, "2026-04-04 10:05:00")
                self.assertIsNotNone(session)
                self.assertEqual(session["session_login_day"], "2026-04-04")
                self.assertEqual(session["session_last_seen_at"], "2026-04-04 10:00:00")

                db.delete_idle_sessions(conn, "2026-04-04 12:01:00")
                expired = db.find_account_by_session(conn, token, "2026-04-04 12:01:01")
                self.assertIsNone(expired)

    def test_validate_upload_request_rejects_more_than_100000_files(self):
        files = [{"name": f"f{index}.fit", "content": b"x"} for index in range(UPLOAD_MAX_FILES + 1)]
        with self.assertRaises(ValueError) as exc:
            validate_upload_request(files, total_bytes=len(files))
        self.assertEqual(str(exc.exception), f"Максимум можно обработать {UPLOAD_MAX_FILES} файлов")

    def test_first_registered_account_becomes_admin_only_in_empty_database(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runtime = RuntimeConfig(
                database_url=f"sqlite:///{root / 'garmin_dashboard.sqlite'}",
                upload_dir=root / "uploads",
                db_auto_ingest=False,
            )
            db = Database(runtime.database_url)
            db.init_schema()
            with db.transaction() as conn:
                self.assertEqual(resolve_registration_role(db, conn), "admin")
                first_account_id = db.create_account(
                    conn,
                    email="first@example.com",
                    password_hash=hash_password("password123"),
                    first_name="First",
                    last_name="Admin",
                    role=resolve_registration_role(db, conn),
                    created_at="2026-04-07 09:00:00",
                )
                first_account = db.find_account_by_email(conn, "first@example.com")
                self.assertEqual(first_account["role"], "admin")
                self.assertEqual(first_account_id, int(first_account["id"]))
                self.assertEqual(resolve_registration_role(db, conn), "user")


if __name__ == "__main__":
    unittest.main()
