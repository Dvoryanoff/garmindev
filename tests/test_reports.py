import unittest

from garmin_dashboard.app.reports import build_summary, middle_half_rows, resolve_period


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


if __name__ == "__main__":
    unittest.main()
