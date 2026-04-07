import unittest

from garmin_dashboard.core.config import IntervalConfig, REST_LONG_PAUSE_THRESHOLD_SECONDS
from garmin_dashboard.core.rest_metrics import (
    compute_monthly_avg_rest_from_payloads,
    compute_summary_rest_by_distance_from_payloads,
    compute_workout_rest_stats_from_payloads,
    format_rest,
    rest_seconds_between,
)


def _pool_payload(laps: list[dict]) -> dict:
    return {
        "messages": {
            "session_mesgs": [{"sport": "swimming", "sub_sport": "lap_swimming"}],
            "lap_mesgs": laps,
        }
    }


class RestMetricsTestCase(unittest.TestCase):
    def test_rest_seconds_between_returns_none_for_different_activities(self):
        previous_row = {"activity_key": "a1", "lap_end": "2026-01-01 10:00:30"}
        next_row = {"activity_key": "a2", "lap_start": "2026-01-01 10:00:50"}
        self.assertIsNone(rest_seconds_between(previous_row, next_row))

    def test_summary_rest_ignores_drill_between_equal_freestyle_laps(self):
        payload = _pool_payload([
            {"start_time": "2026-01-01 10:00:00", "timestamp": "2026-01-01 10:00:40", "total_distance": 50.0, "swim_stroke": "freestyle"},
            {"start_time": "2026-01-01 10:01:00", "timestamp": "2026-01-01 10:04:20", "total_distance": 200.0, "swim_stroke": "drill"},
            {"start_time": "2026-01-01 10:04:40", "timestamp": "2026-01-01 10:05:20", "total_distance": 50.0, "swim_stroke": "freestyle"},
        ])
        rest_by_distance = compute_summary_rest_by_distance_from_payloads([payload], IntervalConfig(target_distances=(50, 100, 200)))
        self.assertEqual(rest_by_distance, {})

    def test_summary_rest_uses_only_equal_freestyle_laps_below_threshold(self):
        payload = _pool_payload([
            {"start_time": "2026-01-01 10:00:00", "timestamp": "2026-01-01 10:00:40", "total_distance": 50.0, "swim_stroke": "freestyle"},
            {"start_time": "2026-01-01 10:01:00", "timestamp": "2026-01-01 10:01:42", "total_distance": 50.0, "swim_stroke": "freestyle"},
            {"start_time": "2026-01-01 10:02:20", "timestamp": "2026-01-01 10:03:02", "total_distance": 50.0, "swim_stroke": "freestyle"},
        ])
        rest_by_distance = compute_summary_rest_by_distance_from_payloads([payload], IntervalConfig(target_distances=(50, 100, 200)))
        self.assertEqual(rest_by_distance[50], 29.0)
        self.assertEqual(format_rest(rest_by_distance[50]), "0:29")

    def test_summary_rest_excludes_pauses_above_threshold(self):
        long_pause_start = 10 + (REST_LONG_PAUSE_THRESHOLD_SECONDS / 60.0) + 1
        payload = _pool_payload([
            {"start_time": "2026-01-01 10:00:00", "timestamp": "2026-01-01 10:00:40", "total_distance": 50.0, "swim_stroke": "freestyle"},
            {"start_time": f"2026-01-01 10:{int(long_pause_start):02d}:00", "timestamp": f"2026-01-01 10:{int(long_pause_start):02d}:40", "total_distance": 50.0, "swim_stroke": "freestyle"},
        ])
        rest_by_distance = compute_summary_rest_by_distance_from_payloads([payload], IntervalConfig(target_distances=(50, 100)))
        self.assertEqual(rest_by_distance, {})

    def test_workout_rest_counts_long_pauses_but_keeps_average(self):
        payloads_by_activity = {
            "a1": _pool_payload([
                {"start_time": "2026-01-01 10:00:00", "timestamp": "2026-01-01 10:00:40", "total_distance": 50.0, "swim_stroke": "freestyle"},
                {"start_time": "2026-01-01 10:01:00", "timestamp": "2026-01-01 10:01:42", "total_distance": 50.0, "swim_stroke": "drill"},
                {"start_time": "2026-01-01 10:04:30", "timestamp": "2026-01-01 10:05:10", "total_distance": 50.0, "swim_stroke": "freestyle"},
            ])
        }
        stats = compute_workout_rest_stats_from_payloads(payloads_by_activity)
        self.assertEqual(stats["a1"]["long_rest_count"], 1)
        self.assertAlmostEqual(stats["a1"]["avg_rest_s"], 94.0, places=2)

    def test_monthly_rest_counts_only_named_style_laps_and_applies_threshold(self):
        payloads = [
            _pool_payload([
                {"start_time": "2026-02-01 10:00:00", "timestamp": "2026-02-01 10:00:40", "total_distance": 50.0, "swim_stroke": "freestyle"},
                {"start_time": "2026-02-01 10:01:00", "timestamp": "2026-02-01 10:01:42", "total_distance": 50.0, "swim_stroke": "backstroke"},
                {"start_time": "2026-02-01 10:04:30", "timestamp": "2026-02-01 10:05:10", "total_distance": 50.0, "swim_stroke": "freestyle"},
            ]),
            _pool_payload([
                {"start_time": "2026-03-01 10:00:00", "timestamp": "2026-03-01 10:00:40", "total_distance": 50.0, "swim_stroke": "freestyle"},
                {"start_time": "2026-03-01 10:01:20", "timestamp": "2026-03-01 10:02:00", "total_distance": 50.0, "swim_stroke": "breaststroke"},
            ]),
        ]
        month_rest = compute_monthly_avg_rest_from_payloads(payloads)
        self.assertEqual(month_rest[(2026, 2)], 20.0)
        self.assertEqual(month_rest[(2026, 3)], 40.0)


if __name__ == "__main__":
    unittest.main()
