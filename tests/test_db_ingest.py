import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from garmin_dashboard.app.reports import build_report
from garmin_dashboard.core.auth import hash_password
from garmin_dashboard.core.config import IntervalConfig, ReportRequest, RuntimeConfig
from garmin_dashboard.core.db import Database
from garmin_dashboard.core.db_ingest import ingest_uploaded_files


class DatabaseIngestTestCase(unittest.TestCase):
    def test_build_report_reads_from_database_after_ingest(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            db_path = root / "garmin_dashboard.sqlite"
            upload_dir = root / "uploads"
            runtime = RuntimeConfig(
                database_url=f"sqlite:///{db_path}",
                upload_dir=upload_dir,
                db_auto_ingest=False,
            )
            db = Database(runtime.database_url)
            db.init_schema()
            with db.transaction() as conn:
                account_id = db.create_account(
                    conn,
                    email="alex@example.com",
                    password_hash=hash_password("password123"),
                    first_name="Alex",
                    last_name="Swimmer",
                    role="admin",
                    created_at="2026-04-04 10:00:00",
                )

            request = ReportRequest(
                swim_mode="all",
                period="all",
                owner_account_id=account_id,
                interval_config=IntervalConfig(),
                runtime_config=runtime,
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
                    "garmin_user_name": "Alex",
                    "sport": "swimming",
                    "sub_sport": "lap_swimming",
                    "swim_type": "pool",
                    "total_distance_m": 1500.0,
                    "total_time_s": 1800.0,
                },
                "intervals": [
                    {
                        "file_name": "sample.fit",
                        "activity_key": "user-1|2026-04-04T10:00:00",
                        "activity_date": "2026-04-04 10:00:00",
                        "user_id": "user-1",
                        "user_name": "Alex",
                        "lap_start": "2026-04-04 10:00:00",
                        "lap_end": "2026-04-04 10:01:30",
                        "distance_m": 100,
                        "raw_distance_m": 100.0,
                        "time_s": 90.0,
                        "time_text": "1:30",
                        "workout_total_distance_m": 1500.0,
                        "workout_total_time_s": 1800.0,
                        "stroke": "freestyle",
                        "swim_type": "pool",
                        "pace_100m_s": 90.0,
                        "pace_100m": "1:30/100m",
                    }
                ],
            }

            with patch("garmin_dashboard.core.db_ingest.parse_fit_file_to_activity", return_value=parsed):
                meta = ingest_uploaded_files(runtime, account_id, [{"name": "sample.fit", "content": b"fit"}])
                report = build_report(request)

            self.assertEqual(report["overview"]["intervals"], 1)
            self.assertEqual(report["overview"]["workouts"], 1)
            self.assertEqual(report["summary"][0]["distance_m"], 100)
            self.assertEqual(meta["processed_files"], 1)
            self.assertTrue(db_path.exists())

            with patch("garmin_dashboard.core.db_ingest.parse_fit_file_to_activity", side_effect=AssertionError("should not reparse")):
                meta_second = ingest_uploaded_files(runtime, account_id, [{"name": "sample.fit", "content": b"fit"}])
                report_second = build_report(request)

            self.assertEqual(report_second["overview"]["intervals"], 1)
            self.assertEqual(meta_second["skipped_files"], 1)

    def test_build_report_excludes_long_distances_when_threshold_group_not_selected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            db_path = root / "garmin_dashboard.sqlite"
            upload_dir = root / "uploads"
            runtime = RuntimeConfig(
                database_url=f"sqlite:///{db_path}",
                upload_dir=upload_dir,
                db_auto_ingest=False,
            )
            db = Database(runtime.database_url)
            db.init_schema()
            with db.transaction() as conn:
                account_id = db.create_account(
                    conn,
                    email="maria@example.com",
                    password_hash=hash_password("password123"),
                    first_name="Maria",
                    last_name="Swimmer",
                    role="user",
                    created_at="2026-04-04 10:00:00",
                )

            parsed = {
                "status": "ready",
                "error_text": "",
                "activity_key": "user-2|2026-04-04T10:00:00",
                "payload": {"messages": {"session_mesgs": [{"sport": "swimming"}]}},
                "activity": {
                    "activity_key": "user-2|2026-04-04T10:00:00",
                    "activity_date": "2026-04-04 10:00:00",
                    "garmin_user_id": "user-2",
                    "garmin_user_name": "Maria",
                    "sport": "swimming",
                    "sub_sport": "open_water",
                    "swim_type": "open_water",
                    "total_distance_m": 1003.0,
                    "total_time_s": 1752.0,
                },
                "intervals": [
                    {
                        "file_name": "sample.fit",
                        "activity_key": "user-2|2026-04-04T10:00:00",
                        "activity_date": "2026-04-04 10:00:00",
                        "user_id": "user-2",
                        "user_name": "Maria",
                        "lap_start": "2026-04-04 10:00:00",
                        "lap_end": "2026-04-04 10:29:12",
                        "distance_m": 1003,
                        "raw_distance_m": 1003.0,
                        "time_s": 1752.0,
                        "time_text": "29:12",
                        "workout_total_distance_m": 1003.0,
                        "workout_total_time_s": 1752.0,
                        "stroke": "freestyle",
                        "swim_type": "open_water",
                        "pace_100m_s": 174.68,
                        "pace_100m": "2:55/100m",
                    }
                ],
            }

            with patch("garmin_dashboard.core.db_ingest.parse_fit_file_to_activity", return_value=parsed):
                ingest_uploaded_files(runtime, account_id, [{"name": "long.fit", "content": b"fit"}])

            request = ReportRequest(
                swim_mode="all",
                period="all",
                owner_account_id=account_id,
                interval_config=IntervalConfig(target_distances=(1200,), long_freestyle_min_distance_m=1200.0),
                runtime_config=runtime,
            )
            report = build_report(request)

            self.assertEqual(report["overview"]["intervals"], 0)
            self.assertEqual(report["summary"], [])

    def test_build_report_includes_open_water_distances_above_threshold_when_1000_selected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            db_path = root / "garmin_dashboard.sqlite"
            upload_dir = root / "uploads"
            runtime = RuntimeConfig(
                database_url=f"sqlite:///{db_path}",
                upload_dir=upload_dir,
                db_auto_ingest=False,
            )
            db = Database(runtime.database_url)
            db.init_schema()
            with db.transaction() as conn:
                account_id = db.create_account(
                    conn,
                    email="openwater@example.com",
                    password_hash=hash_password("password123"),
                    first_name="Open",
                    last_name="Water",
                    role="user",
                    created_at="2026-04-04 10:00:00",
                )

            parsed = {
                "status": "ready",
                "error_text": "",
                "activity_key": "user-3|2026-04-04T10:00:00",
                "payload": {"messages": {"session_mesgs": [{"sport": "swimming"}]}},
                "activity": {
                    "activity_key": "user-3|2026-04-04T10:00:00",
                    "activity_date": "2026-04-04 10:00:00",
                    "garmin_user_id": "user-3",
                    "garmin_user_name": "Open",
                    "sport": "swimming",
                    "sub_sport": "open_water",
                    "swim_type": "open_water",
                    "total_distance_m": 1207.0,
                    "total_time_s": 2100.0,
                },
                "intervals": [
                    {
                        "file_name": "open.fit",
                        "activity_key": "user-3|2026-04-04T10:00:00",
                        "activity_date": "2026-04-04 10:00:00",
                        "user_id": "user-3",
                        "user_name": "Open",
                        "lap_start": "2026-04-04 10:00:00",
                        "lap_end": "2026-04-04 10:35:00",
                        "distance_m": 1207,
                        "raw_distance_m": 1207.0,
                        "time_s": 2100.0,
                        "time_text": "35:00",
                        "workout_total_distance_m": 1207.0,
                        "workout_total_time_s": 2100.0,
                        "stroke": "freestyle",
                        "swim_type": "open_water",
                        "pace_100m_s": 174.0,
                        "pace_100m": "2:54/100m",
                    }
                ],
            }

            with patch("garmin_dashboard.core.db_ingest.parse_fit_file_to_activity", return_value=parsed):
                ingest_uploaded_files(runtime, account_id, [{"name": "open.fit", "content": b"fit"}])

            request = ReportRequest(
                swim_mode="open_water",
                period="all",
                owner_account_id=account_id,
                interval_config=IntervalConfig(target_distances=(1000,), long_freestyle_min_distance_m=1000.0),
                runtime_config=runtime,
            )
            report = build_report(request)

            self.assertEqual(report["overview"]["intervals"], 1)
            self.assertEqual(len(report["workouts"]), 1)
            self.assertEqual(report["summary"][0]["distance_m"], 1207)

    def test_build_report_includes_open_water_distances_above_threshold_in_all_mode(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            db_path = root / "garmin_dashboard.sqlite"
            upload_dir = root / "uploads"
            runtime = RuntimeConfig(
                database_url=f"sqlite:///{db_path}",
                upload_dir=upload_dir,
                db_auto_ingest=False,
            )
            db = Database(runtime.database_url)
            db.init_schema()
            with db.transaction() as conn:
                account_id = db.create_account(
                    conn,
                    email="mixedmode@example.com",
                    password_hash=hash_password("password123"),
                    first_name="Mixed",
                    last_name="Mode",
                    role="user",
                    created_at="2026-04-04 10:00:00",
                )

            parsed = {
                "status": "ready",
                "error_text": "",
                "activity_key": "user-4|2026-04-04T10:00:00",
                "payload": {"messages": {"session_mesgs": [{"sport": "swimming"}]}},
                "activity": {
                    "activity_key": "user-4|2026-04-04T10:00:00",
                    "activity_date": "2026-04-04 10:00:00",
                    "garmin_user_id": "user-4",
                    "garmin_user_name": "Mixed",
                    "sport": "swimming",
                    "sub_sport": "open_water",
                    "swim_type": "open_water",
                    "total_distance_m": 1500.0,
                    "total_time_s": 2700.0,
                },
                "intervals": [
                    {
                        "file_name": "mixed.fit",
                        "activity_key": "user-4|2026-04-04T10:00:00",
                        "activity_date": "2026-04-04 10:00:00",
                        "user_id": "user-4",
                        "user_name": "Mixed",
                        "lap_start": "2026-04-04 10:00:00",
                        "lap_end": "2026-04-04 10:45:00",
                        "distance_m": 1500,
                        "raw_distance_m": 1500.0,
                        "time_s": 2700.0,
                        "time_text": "45:00",
                        "workout_total_distance_m": 1500.0,
                        "workout_total_time_s": 2700.0,
                        "stroke": "freestyle",
                        "swim_type": "open_water",
                        "pace_100m_s": 180.0,
                        "pace_100m": "3:00/100m",
                    }
                ],
            }

            with patch("garmin_dashboard.core.db_ingest.parse_fit_file_to_activity", return_value=parsed):
                ingest_uploaded_files(runtime, account_id, [{"name": "mixed.fit", "content": b"fit"}])

            request = ReportRequest(
                swim_mode="all",
                period="all",
                owner_account_id=account_id,
                interval_config=IntervalConfig(target_distances=(1000,), long_freestyle_min_distance_m=1000.0),
                runtime_config=runtime,
            )
            report = build_report(request)

            self.assertEqual(report["overview"]["intervals"], 1)
            self.assertEqual(len(report["workouts"]), 1)
            self.assertEqual(report["summary"][0]["distance_m"], 1500)


if __name__ == "__main__":
    unittest.main()
