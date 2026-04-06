import unittest

from garmin_dashboard.app.reports import build_summary, build_workout_groups, middle_half_rows, resolve_period


class ReportsTestCase(unittest.TestCase):
    def test_middle_half_rows_keeps_all_rows_for_small_groups(self):
        rows = [{"time_s": 10, "activity_date": "", "lap_start": "", "file_name": ""} for _ in range(4)]
        self.assertEqual(len(middle_half_rows(rows)), 4)

    def test_middle_half_rows_drops_outer_quarters_for_larger_groups(self):
        rows = [
            {"time_s": value, "activity_date": "", "lap_start": "", "file_name": f"f{index}.fit"}
            for index, value in enumerate([100, 95, 90, 85, 80, 75, 70, 65], start=1)
        ]
        middle = middle_half_rows(rows)
        self.assertEqual([row["time_s"] for row in middle], [90, 85, 80, 75])

    def test_build_summary_uses_time_average_and_middle_average(self):
        rows = [
            {
                "distance_m": 300,
                "time_s": time_s,
                "pace_100m_s": time_s / 3,
                "activity_date": "2026-03-01 10:00:00",
                "lap_start": "2026-03-01 10:00:00",
                "file_name": f"row-{index}.fit",
                "stroke": "freestyle",
                "swim_type": "pool",
            }
            for index, time_s in enumerate([399, 356, 349, 345, 329, 322, 322, 321, 319, 315, 312, 310, 296, 291], start=1)
        ]

        summary = build_summary(rows)
        self.assertEqual(len(summary), 1)
        row = summary[0]
        self.assertEqual(row["count"], 14)
        self.assertEqual(row["middle_count"], 8)
        self.assertAlmostEqual(row["avg_time_s"], 327.57, places=2)
        self.assertAlmostEqual(row["avg_pace_100m_s"], 109.19, places=2)
        self.assertAlmostEqual(row["middle_pace_100m_s"], 107.71, places=2)
        self.assertNotEqual(row["avg_pace_100m_s"], row["middle_pace_100m_s"])

    def test_resolve_period_for_custom_days(self):
        start, end, label = resolve_period(days=30)
        self.assertEqual(label, "Последние 30 дней")
        self.assertIsNotNone(start)
        self.assertIsNotNone(end)

    def test_build_summary_includes_pool_rest_for_same_distance(self):
        rows = [
            {
                "activity_key": "w1",
                "distance_m": 50,
                "time_s": 40,
                "pace_100m_s": 80,
                "activity_date": "2026-03-01 10:00:00",
                "lap_start": "2026-03-01 10:00:00",
                "lap_end": "2026-03-01 10:00:40",
                "file_name": "row-1.fit",
                "stroke": "freestyle",
                "swim_type": "pool",
            },
            {
                "activity_key": "w1",
                "distance_m": 50,
                "time_s": 41,
                "pace_100m_s": 82,
                "activity_date": "2026-03-01 10:00:00",
                "lap_start": "2026-03-01 10:01:00",
                "lap_end": "2026-03-01 10:01:41",
                "file_name": "row-2.fit",
                "stroke": "backstroke",
                "swim_type": "pool",
            },
            {
                "activity_key": "w1",
                "distance_m": 100,
                "time_s": 90,
                "pace_100m_s": 90,
                "activity_date": "2026-03-01 10:00:00",
                "lap_start": "2026-03-01 10:02:00",
                "lap_end": "2026-03-01 10:03:30",
                "file_name": "row-3.fit",
                "stroke": "freestyle",
                "swim_type": "pool",
            },
        ]

        summary = build_summary(rows, include_pool_rest=True, rest_by_distance={50: 20.0})
        self.assertEqual(summary[0]["distance_m"], 50)
        self.assertEqual(summary[0]["avg_rest"], "0:20")
        self.assertEqual(summary[1]["avg_rest"], "")

    def test_build_workout_groups_includes_average_rest_for_named_strokes(self):
        rows = [
            {
                "activity_key": "w1",
                "distance_m": 50,
                "time_s": 40,
                "pace_100m_s": 80,
                "activity_date": "2026-03-01 10:00:00",
                "lap_start": "2026-03-01 10:00:00",
                "lap_end": "2026-03-01 10:00:40",
                "file_name": "row-1.fit",
                "stroke": "freestyle",
                "swim_type": "pool",
                "workout_total_distance_m": 150,
                "workout_total_time_s": 180,
            },
            {
                "activity_key": "w1",
                "distance_m": 50,
                "time_s": 45,
                "pace_100m_s": 90,
                "activity_date": "2026-03-01 10:00:00",
                "lap_start": "2026-03-01 10:01:00",
                "lap_end": "2026-03-01 10:01:45",
                "file_name": "row-2.fit",
                "stroke": "breaststroke",
                "swim_type": "pool",
                "workout_total_distance_m": 150,
                "workout_total_time_s": 180,
            },
            {
                "activity_key": "w1",
                "distance_m": 50,
                "time_s": 50,
                "pace_100m_s": 100,
                "activity_date": "2026-03-01 10:00:00",
                "lap_start": "2026-03-01 10:02:30",
                "lap_end": "2026-03-01 10:03:20",
                "file_name": "row-3.fit",
                "stroke": "drill",
                "swim_type": "pool",
                "workout_total_distance_m": 150,
                "workout_total_time_s": 180,
            },
        ]

        workouts = build_workout_groups(rows)
        self.assertEqual(len(workouts), 1)
        self.assertEqual(workouts[0]["avg_rest"], "0:32")
        self.assertEqual(workouts[0]["long_rest_count"], 0)


if __name__ == "__main__":
    unittest.main()
