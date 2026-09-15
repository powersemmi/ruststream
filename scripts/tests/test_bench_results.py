"""The results generator's knobs: the count of deliveries a run is measured over, and the
allocation ratio the terminal report prints next to the two halves."""

import sys
import unittest
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[1]

# The scripts are plain files next to each other, not a package, and they are imported the way
# the workflow runs them.
sys.path.insert(0, str(SCRIPTS))

import bench_results  # noqa: E402


class ConfiguredCount(unittest.TestCase):
    def tearDown(self):
        bench_results.configure(bench_results.DEFAULT_MESSAGES)

    def test_the_per_message_figure_follows_the_configured_count(self):
        bench_results.configure(5000)
        self.assertEqual(bench_results.per_message(5_000_000), 1000)
        bench_results.configure(bench_results.DEFAULT_MESSAGES)
        self.assertEqual(bench_results.per_message(5_000_000), 5000)

    def test_the_floor_scales_with_the_count(self):
        bench_results.configure(5000)
        self.assertEqual(bench_results.FLOOR, 5 * bench_results.FLOOR_PER_DEFAULT_RUN)
        bench_results.configure(bench_results.DEFAULT_MESSAGES)
        self.assertEqual(bench_results.FLOOR, bench_results.FLOOR_PER_DEFAULT_RUN)

    def test_a_run_below_the_scaled_floor_is_refused(self):
        bench_results.configure(5000)
        found = {"suite/base": {"instructions": 200_000, "allocations": 0}}
        with self.assertRaises(SystemExit):
            bench_results.totals(found, "suite", "base")

    def test_a_count_that_is_not_positive_is_refused(self):
        with self.assertRaises(SystemExit):
            bench_results.configure(0)


class AllocationRatio(unittest.TestCase):
    def test_equal_counts_print_as_equal(self):
        self.assertEqual(
            bench_results.allocation_ratio({"allocations": 0}, {"allocations": 0}), "="
        )
        self.assertEqual(
            bench_results.allocation_ratio({"allocations": 5.0}, {"allocations": 5.0}), "="
        )

    def test_a_difference_prints_as_a_multiple(self):
        self.assertEqual(
            bench_results.allocation_ratio({"allocations": 6.0}, {"allocations": 5.0}), "1.20x"
        )

    def test_no_hand_written_half_or_a_zero_divisor_prints_not_applicable(self):
        self.assertEqual(bench_results.allocation_ratio({"allocations": 1.0}, None), "n/a")
        self.assertEqual(
            bench_results.allocation_ratio({"allocations": 1.0}, {"allocations": 0}), "n/a"
        )


if __name__ == "__main__":
    unittest.main()
