import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from garmin_dashboard.app.reports import build_report
from garmin_dashboard.core.auth import hash_password, new_session_token, session_expiry, verify_password
from garmin_dashboard.core.config import IntervalConfig, ReportRequest, RuntimeConfig
from garmin_dashboard.core.db import Database
from garmin_dashboard.core.db_ingest import ingest_uploaded_files, load_monthly_history
from garmin_dashboard.core.monthly_history import build_monthly_history_payload


def _activity_payload(*, key: str, activity_date: str, swim_type: str, total_distance: float, total_time: float, intervals: list[dict]) -> dict:
    return {
        "status": "ready",
        "error_text": "",
        "activity_key": key,
        "payload": {"messages": {"session_mesgs": [{"sport": "swimming"}]}},
        "activity": {
            "activity_key": key,
            "activity_date": activity_date,
            "garmin_user_id": key.split("|", 1)[0],
            "garmin_user_name": key.split("|", 1)[0],
            "sport": "swimming",
            "sub_sport": "lap_swimming" if swim_type == "pool" else "open_water",
            "swim_type": swim_type,
            "total_distance_m": total_distance,
            "total_time_s": total_time,
        },
        "intervals": intervals,
    }


class MultiUserFlowTestCase(unittest.TestCase):
    def fake_parse_fit_file_to_activity(self, fit_path: Path) -> dict:
        name = fit_path.name
        fixtures = {
            "u1_50.fit": _activity_payload(
                key="user-1|2026-01-10T07:00:00",
                activity_date="2026-01-10 07:00:00",
                swim_type="pool",
                total_distance=50.0,
                total_time=45.0,
                intervals=[
                    {
                        "file_name": name,
                        "activity_key": "user-1|2026-01-10T07:00:00",
                        "activity_date": "2026-01-10 07:00:00",
                        "user_id": "user-1",
                        "user_name": "User One",
                        "lap_start": "2026-01-10 07:00:00",
                        "lap_end": "2026-01-10 07:00:45",
                        "distance_m": 50,
                        "raw_distance_m": 50.0,
                        "time_s": 45.0,
                        "time_text": "0:45",
                        "workout_total_distance_m": 50.0,
                        "workout_total_time_s": 45.0,
                        "stroke": "freestyle",
                        "swim_type": "pool",
                        "pace_100m_s": 90.0,
                        "pace_100m": "1:30/100m",
                    }
                ],
            ),
            "u1_100.fit": _activity_payload(
                key="user-1|2026-02-11T07:00:00",
                activity_date="2026-02-11 07:00:00",
                swim_type="pool",
                total_distance=100.0,
                total_time=95.0,
                intervals=[
                    {
                        "file_name": name,
                        "activity_key": "user-1|2026-02-11T07:00:00",
                        "activity_date": "2026-02-11 07:00:00",
                        "user_id": "user-1",
                        "user_name": "User One",
                        "lap_start": "2026-02-11 07:00:00",
                        "lap_end": "2026-02-11 07:01:35",
                        "distance_m": 100,
                        "raw_distance_m": 100.0,
                        "time_s": 95.0,
                        "time_text": "1:35",
                        "workout_total_distance_m": 100.0,
                        "workout_total_time_s": 95.0,
                        "stroke": "freestyle",
                        "swim_type": "pool",
                        "pace_100m_s": 95.0,
                        "pace_100m": "1:35/100m",
                    }
                ],
            ),
            "u1_200.fit": _activity_payload(
                key="user-1|2026-03-12T07:00:00",
                activity_date="2026-03-12 07:00:00",
                swim_type="pool",
                total_distance=200.0,
                total_time=210.0,
                intervals=[
                    {
                        "file_name": name,
                        "activity_key": "user-1|2026-03-12T07:00:00",
                        "activity_date": "2026-03-12 07:00:00",
                        "user_id": "user-1",
                        "user_name": "User One",
                        "lap_start": "2026-03-12 07:00:00",
                        "lap_end": "2026-03-12 07:03:30",
                        "distance_m": 200,
                        "raw_distance_m": 200.0,
                        "time_s": 210.0,
                        "time_text": "3:30",
                        "workout_total_distance_m": 200.0,
                        "workout_total_time_s": 210.0,
                        "stroke": "freestyle",
                        "swim_type": "pool",
                        "pace_100m_s": 105.0,
                        "pace_100m": "1:45/100m",
                    }
                ],
            ),
            "u2_200.fit": _activity_payload(
                key="user-2|2026-01-09T08:00:00",
                activity_date="2026-01-09 08:00:00",
                swim_type="open_water",
                total_distance=200.0,
                total_time=230.0,
                intervals=[
                    {
                        "file_name": name,
                        "activity_key": "user-2|2026-01-09T08:00:00",
                        "activity_date": "2026-01-09 08:00:00",
                        "user_id": "user-2",
                        "user_name": "User Two",
                        "lap_start": "2026-01-09 08:00:00",
                        "lap_end": "2026-01-09 08:03:50",
                        "distance_m": 200,
                        "raw_distance_m": 200.0,
                        "time_s": 230.0,
                        "time_text": "3:50",
                        "workout_total_distance_m": 200.0,
                        "workout_total_time_s": 230.0,
                        "stroke": "freestyle",
                        "swim_type": "open_water",
                        "pace_100m_s": 115.0,
                        "pace_100m": "1:55/100m",
                    }
                ],
            ),
            "u2_400.fit": _activity_payload(
                key="user-2|2026-02-13T08:00:00",
                activity_date="2026-02-13 08:00:00",
                swim_type="open_water",
                total_distance=400.0,
                total_time=500.0,
                intervals=[
                    {
                        "file_name": name,
                        "activity_key": "user-2|2026-02-13T08:00:00",
                        "activity_date": "2026-02-13 08:00:00",
                        "user_id": "user-2",
                        "user_name": "User Two",
                        "lap_start": "2026-02-13 08:00:00",
                        "lap_end": "2026-02-13 08:08:20",
                        "distance_m": 400,
                        "raw_distance_m": 400.0,
                        "time_s": 500.0,
                        "time_text": "8:20",
                        "workout_total_distance_m": 400.0,
                        "workout_total_time_s": 500.0,
                        "stroke": "freestyle",
                        "swim_type": "open_water",
                        "pace_100m_s": 125.0,
                        "pace_100m": "2:05/100m",
                    }
                ],
            ),
            "u2_800.fit": _activity_payload(
                key="user-2|2026-03-14T08:00:00",
                activity_date="2026-03-14 08:00:00",
                swim_type="open_water",
                total_distance=800.0,
                total_time=1120.0,
                intervals=[
                    {
                        "file_name": name,
                        "activity_key": "user-2|2026-03-14T08:00:00",
                        "activity_date": "2026-03-14 08:00:00",
                        "user_id": "user-2",
                        "user_name": "User Two",
                        "lap_start": "2026-03-14 08:00:00",
                        "lap_end": "2026-03-14 08:18:40",
                        "distance_m": 800,
                        "raw_distance_m": 800.0,
                        "time_s": 1120.0,
                        "time_text": "18:40",
                        "workout_total_distance_m": 800.0,
                        "workout_total_time_s": 1120.0,
                        "stroke": "freestyle",
                        "swim_type": "open_water",
                        "pace_100m_s": 140.0,
                        "pace_100m": "2:20/100m",
                    }
                ],
            ),
        }
        for suffix, payload in fixtures.items():
            if name.endswith(suffix):
                return payload
        raise AssertionError(f"unexpected FIT fixture requested: {name}")

    def test_multi_user_flow_with_sessions_reports_and_admin_stats(self):
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
                admin_id = db.create_account(
                    conn,
                    email="admin@example.com",
                    password_hash=hash_password("password123"),
                    first_name="Admin",
                    last_name="User",
                    role="admin",
                    created_at="2026-04-04 10:00:00",
                )
                user_id = db.create_account(
                    conn,
                    email="user@example.com",
                    password_hash=hash_password("password123"),
                    first_name="Regular",
                    last_name="User",
                    role="user",
                    created_at="2026-04-04 10:05:00",
                )

                admin = db.find_account_by_email(conn, "admin@example.com")
                user = db.find_account_by_email(conn, "user@example.com")
                self.assertEqual(admin["role"], "admin")
                self.assertEqual(user["role"], "user")
                self.assertTrue(verify_password("password123", admin["password_hash"]))
                self.assertTrue(verify_password("password123", user["password_hash"]))

                admin_token = new_session_token()
                user_token = new_session_token()
                db.create_session(conn, admin_id, admin_token, "2026-04-04 10:10:00", session_expiry())
                db.create_session(conn, user_id, user_token, "2026-04-04 10:10:00", session_expiry())
                db.update_last_login(conn, admin_id, "2026-04-04 10:10:00")
                db.update_last_login(conn, user_id, "2026-04-04 10:11:00")

                self.assertEqual(db.find_account_by_session(conn, admin_token, "2026-04-04 10:10:01")["id"], admin_id)
                self.assertEqual(db.find_account_by_session(conn, user_token, "2026-04-04 10:10:01")["id"], user_id)

            with patch("garmin_dashboard.core.db_ingest.parse_fit_file_to_activity", side_effect=self.fake_parse_fit_file_to_activity):
                admin_meta = ingest_uploaded_files(
                    runtime,
                    admin_id,
                    [
                        {"name": "u1_50.fit", "content": b"user1-fit-50"},
                        {"name": "u1_100.fit", "content": b"user1-fit-100"},
                        {"name": "u1_200.fit", "content": b"user1-fit-200"},
                    ],
                )
                user_meta = ingest_uploaded_files(
                    runtime,
                    user_id,
                    [
                        {"name": "u2_200.fit", "content": b"user2-fit-200"},
                        {"name": "u2_400.fit", "content": b"user2-fit-400"},
                        {"name": "u2_800.fit", "content": b"user2-fit-800"},
                    ],
                )

            self.assertEqual(admin_meta["processed_files"], 3)
            self.assertEqual(user_meta["processed_files"], 3)

            admin_report = build_report(
                ReportRequest(
                    swim_mode="all",
                    period="all",
                    owner_account_id=admin_id,
                    interval_config=IntervalConfig(target_distances=(50, 100, 200)),
                    runtime_config=runtime,
                )
            )
            self.assertEqual([row["distance_m"] for row in admin_report["summary"]], [50, 100, 200])
            self.assertEqual(admin_report["overview"]["intervals"], 3)
            self.assertEqual(admin_report["overview"]["workouts"], 3)

            user_report = build_report(
                ReportRequest(
                    swim_mode="all",
                    period="all",
                    owner_account_id=user_id,
                    interval_config=IntervalConfig(target_distances=(200, 400, 800)),
                    runtime_config=runtime,
                )
            )
            self.assertEqual([row["distance_m"] for row in user_report["summary"]], [200, 400, 800])
            self.assertEqual(user_report["overview"]["intervals"], 3)
            self.assertEqual(user_report["overview"]["workouts"], 3)

            admin_monthly_payload = build_monthly_history_payload(load_monthly_history(runtime, admin_id))
            user_monthly_payload = build_monthly_history_payload(load_monthly_history(runtime, user_id))
            self.assertEqual(admin_monthly_payload["years"], [2026])
            self.assertEqual(user_monthly_payload["years"], [2026])
            self.assertEqual(len(admin_monthly_payload["rows"]), 3)
            self.assertEqual(len(user_monthly_payload["rows"]), 3)

            filtered_admin_report = build_report(
                ReportRequest(
                    swim_mode="all",
                    period="all",
                    owner_account_id=admin_id,
                    interval_config=IntervalConfig(target_distances=(50,)),
                    runtime_config=runtime,
                )
            )
            self.assertEqual([row["distance_m"] for row in filtered_admin_report["summary"]], [50])
            self.assertEqual(filtered_admin_report["overview"]["intervals"], 1)
            self.assertEqual(filtered_admin_report["overview"]["workouts"], 1)

            with db.transaction() as conn:
                users = db.list_accounts_with_stats(conn)
                admin_stats = next(row for row in users if int(row["id"]) == admin_id)
                user_stats = next(row for row in users if int(row["id"]) == user_id)
                overview = db.admin_overview(conn)
                recent_logins = db.list_recent_logins(conn)
                recent_uploads = db.list_recent_uploads(conn)

            self.assertEqual(len(users), 2)
            self.assertEqual(int(admin_stats["files_count"]), 3)
            self.assertEqual(int(user_stats["files_count"]), 3)
            self.assertEqual(int(admin_stats["activities_count"]), 3)
            self.assertEqual(int(user_stats["activities_count"]), 3)
            self.assertEqual(overview["total_users"], 2)
            self.assertEqual(overview["total_files"], 6)
            self.assertEqual(overview["total_activities"], 6)
            self.assertEqual(overview["total_intervals"], 6)
            self.assertGreaterEqual(len(recent_logins), 2)
            self.assertGreaterEqual(len(recent_uploads), 6)


if __name__ == "__main__":
    unittest.main()
