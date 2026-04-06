import unittest

from garmin_dashboard.core.monthly_history import (
    MONTHLY_FIXED_DISTANCES,
    build_entries_by_user,
    build_monthly_entries,
    choose_user_name,
    dedupe_workouts,
    sanitize_user_slug,
)


class MonthlyHistoryTestCase(unittest.TestCase):
    def test_build_monthly_entries_creates_continuous_months(self):
        rows = [
            {
                "activity_date": "2024-01-15 10:00:00",
                "lap_start": "2024-01-15 10:00:00",
                "lap_end": "2024-01-15 10:10:00",
                "distance_m": 100,
                "time_s": 100,
            },
            {
                "activity_date": "2024-03-02 10:00:00",
                "lap_start": "2024-03-02 10:00:00",
                "lap_end": "2024-03-02 10:10:00",
                "distance_m": 100,
                "time_s": 110,
            },
        ]

        monthly_rows = build_monthly_entries(rows, month_rest_by_key={(2024, 4): 20.0})
        self.assertEqual([row["date"] for row in monthly_rows], ["Январь 2024", "Февраль 2024", "Март 2024"])
        self.assertEqual(monthly_rows[1][100], "")

    def test_build_monthly_entries_uses_best_monthly_pace(self):
        rows = [
            {
                "activity_date": "2024-05-10 08:00:00",
                "lap_start": "2024-05-10 08:00:00",
                "lap_end": "2024-05-10 08:12:00",
                "distance_m": 400,
                "time_s": 400,
            },
            {
                "activity_date": "2024-05-11 08:00:00",
                "lap_start": "2024-05-11 08:00:00",
                "lap_end": "2024-05-11 08:12:00",
                "distance_m": 400,
                "time_s": 420,
            },
        ]

        monthly_rows = build_monthly_entries(rows)
        self.assertEqual(len(monthly_rows), 1)
        may = monthly_rows[0]
        self.assertEqual(may["date"], "Май 2024")
        self.assertEqual(may[400], "1:40.0")
        self.assertIn(1500, MONTHLY_FIXED_DISTANCES)
        self.assertIn(1800, MONTHLY_FIXED_DISTANCES)

    def test_build_monthly_entries_uses_fastest_interval_not_average(self):
        rows = [
            {
                "activity_date": "2021-04-02 08:00:00",
                "lap_start": "2021-04-02 08:00:00",
                "lap_end": "2021-04-02 08:02:00",
                "distance_m": 100,
                "time_s": 81,
            },
            {
                "activity_date": "2021-04-10 08:00:00",
                "lap_start": "2021-04-10 08:00:00",
                "lap_end": "2021-04-10 08:02:00",
                "distance_m": 100,
                "time_s": 103,
            },
        ]

        monthly_rows = build_monthly_entries(rows)
        self.assertEqual(monthly_rows[0]["date"], "Апрель 2021")
        self.assertEqual(monthly_rows[0][100], "1:21.0")

    def test_build_monthly_entries_includes_average_rest(self):
        rows = [
            {
                "activity_key": "a1",
                "activity_date": "2024-04-02 08:00:00",
                "lap_start": "2024-04-02 08:00:00",
                "lap_end": "2024-04-02 08:00:40",
                "distance_m": 50,
                "time_s": 40,
                "stroke": "freestyle",
            },
            {
                "activity_key": "a1",
                "activity_date": "2024-04-02 08:00:00",
                "lap_start": "2024-04-02 08:01:00",
                "lap_end": "2024-04-02 08:01:42",
                "distance_m": 50,
                "time_s": 42,
                "stroke": "backstroke",
            },
        ]

        monthly_rows = build_monthly_entries(rows, month_rest_by_key={(2024, 4): 20.0})
        self.assertEqual(monthly_rows[0]["avg_rest"], "0:20")

    def test_dedupe_workouts_keeps_unique_activity_keys(self):
        workouts = {
            "a1": {"activity_key": "a1", "user_id": "1", "user_name": "alex", "rows": [{"distance_m": 100, "time_s": 100}]},
            "a2": {"activity_key": "a2", "user_id": "1", "user_name": "alex", "rows": [{"distance_m": 100, "time_s": 110}]},
            "bad": {"activity_key": "bad", "user_id": "1", "user_name": "alex", "rows": []},
        }
        unique = dedupe_workouts(workouts)
        self.assertEqual(sorted(unique.keys()), ["a1", "a2"])

    def test_build_entries_by_user_groups_rows_per_user(self):
        workouts = {
            "a1": {
                "activity_key": "a1",
                "user_id": "111",
                "user_name": "dvoryanoff@mail.ru",
                "rows": [{
                    "activity_key": "a1",
                    "user_id": "111",
                    "user_name": "dvoryanoff@mail.ru",
                    "activity_date": "2024-01-15 10:00:00",
                    "lap_start": "2024-01-15 10:00:00",
                    "lap_end": "2024-01-15 10:05:00",
                    "distance_m": 100,
                    "time_s": 100,
                }],
            },
            "a2": {
                "activity_key": "a2",
                "user_id": "222",
                "user_name": "other@example.com",
                "rows": [{
                    "activity_key": "a2",
                    "user_id": "222",
                    "user_name": "other@example.com",
                    "activity_date": "2024-02-15 10:00:00",
                    "lap_start": "2024-02-15 10:00:00",
                    "lap_end": "2024-02-15 10:05:00",
                    "distance_m": 200,
                    "time_s": 220,
                }],
            },
        }
        entries = build_entries_by_user(workouts)
        self.assertEqual(len(entries), 2)
        self.assertIn(("111", "dvoryanoff@mail.ru"), entries)
        self.assertIn(("222", "other@example.com"), entries)
        self.assertEqual(entries[("111", "dvoryanoff@mail.ru")][0]["date"], "Январь 2024")
        self.assertEqual(entries[("222", "other@example.com")][0]["date"], "Февраль 2024")

    def test_sanitize_user_slug(self):
        self.assertEqual(sanitize_user_slug("dvoryanoff@mail.ru"), "dvoryanoff_mail.ru")
        self.assertEqual(sanitize_user_slug("User Name"), "user_name")

    def test_choose_user_name_prefers_real_name_over_placeholder(self):
        self.assertEqual(
            choose_user_name("user_3322691506", "dvoryanoff@mail.ru", "3322691506"),
            "dvoryanoff@mail.ru",
        )
        self.assertEqual(
            choose_user_name("dvoryanoff@mail.ru", "other_alias", "3322691506"),
            "other_alias",
        )


if __name__ == "__main__":
    unittest.main()
