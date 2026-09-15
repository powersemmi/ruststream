#!/usr/bin/env python3
"""What the benchmark comment says, against runs recorded in CI.

Three fixtures, all cut to the two metrics the table reports: a full run of the suite against a
baseline, with one scenario edited to cost three percent more than its base and to trip both of
its limits; the same run cut short after two benchmarks, which is what a run that died part way
leaves behind; and a run with no baseline at all.
"""

import sys
import tempfile
import unittest
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[1]
FIXTURES = Path(__file__).resolve().parent / "fixtures"

# The formatters are scripts rather than a package, so the directory holding them goes on the path
# and they are imported the way the workflow runs them.
sys.path.insert(0, str(SCRIPTS))

import bench_comment


def measured(fixture="bench-summary.json"):
    return bench_comment.runs(FIXTURES / fixture)


def rendered(fixture="bench-summary.json", head="1111111aaaaaaa", base="2222222bbbbbbb"):
    return bench_comment.comment(measured(fixture), head, base)


def charted(text):
    """The Mermaid source of the chart, by directive."""
    block = text.split("```mermaid")[1].split("```")[0].strip("\n").splitlines()
    drawn = {"kind": block[0].strip()}
    for line in block[1:]:
        keyword, _, rest = line.strip().partition(" ")
        drawn[keyword] = rest.strip()
    return drawn


def series(value):
    """The numbers of a `bar` or `line` directive."""
    return [float(number) for number in value.strip("[]").split(",")]


def row(text, scenario):
    for line in text.splitlines():
        if line.startswith(f"| {scenario} |"):
            return [cell.strip() for cell in line.strip("|").split("|")]
    raise AssertionError(f"no row for {scenario}")


class Table(unittest.TestCase):
    def test_every_scenario_of_a_full_run_carries_its_numbers(self):
        """A tripped limit stops the verdict, never the measurement: the table stays whole."""
        text = rendered()
        for scenario in bench_comment.SCENARIOS:
            self.assertNotEqual(row(text, scenario.name)[1], "-", scenario.name)

    def test_a_scenario_reports_both_halves_and_the_cold_start(self):
        """The pair, the per-message figures and the start-up cost the run measured."""
        cells = row(rendered(), "consume, JSON decode into a small struct")
        self.assertEqual(
            cells,
            [
                "consume, JSON decode into a small struct",
                "2378.1",
                "1653.9",
                "0.0",
                "0.0",
                "-0.50%",
                "18891 / 26",
                "pass",
            ],
        )

    def test_the_summary_totals_the_gated_scenarios_and_carries_the_verdict(self):
        """What a reader gets without opening anything: the move, and whether it failed."""
        self.assertIn(
            "Instructions across the gated scenarios: +0.24% against the base. "
            "Allocations: +3.12%. Gate: fail on publish through an Out slot with one transform.",
            rendered(),
        )

    def test_a_run_missing_a_gated_scenario_reports_no_total(self):
        """A total over eleven of twelve scenarios would read as a measurement it is not."""
        text = rendered("bench-summary-partial.json")
        self.assertIn("there is no total. Gate: pass.", text)

    def test_the_chart_reads_worst_move_first(self):
        """A reader looks at the top of the chart and sees what moved most."""
        drawn = charted(rendered())
        self.assertEqual(
            [key.strip().strip('"') for key in drawn["x-axis"].strip("[]").split(",")],
            [
                "lane",
                "out-slot",
                "json",
                "hdr-write",
                "mw4",
                "reply",
                "req-reply",
                "retry",
                "json-1kb",
                "mw1",
                "hdr-read",
                "batch64",
            ],
        )

    def test_a_bar_per_scenario_in_the_order_the_names_are_drawn(self):
        """The bar and its name are read together, so the two lists move as one."""
        drawn = charted(rendered())
        self.assertEqual(drawn["kind"], "xychart-beta horizontal")
        self.assertEqual(series(drawn["bar"])[:4], [-3.0, 3.0, -0.5, -0.2])
        self.assertEqual(
            len(series(drawn["bar"])), len(drawn["x-axis"].strip("[]").split(","))
        )

    def test_the_axis_carries_zero_and_the_gate(self):
        """A bar is read against zero, and a range stopping short of the limit hides it."""
        self.assertEqual(charted(rendered())["y-axis"], '"change, %" -5 --> 5')

    def test_the_axis_reaches_past_the_largest_move(self):
        """Rounded outwards, so the worst bar is inside the chart rather than on its edge."""
        self.assertEqual(bench_comment.axis_range([-25.8, -2.3], 2.0), (-30, 5))
        self.assertEqual(bench_comment.axis_range([0.4], 2.0), (0, 5))

    def test_the_limit_is_drawn_as_a_line_across_the_chart(self):
        """One value per scenario, all at the limit: a bar past it is over the gate."""
        drawn = charted(rendered())
        self.assertEqual(series(drawn["line"]), [2.0] * len(series(drawn["bar"])))

    def test_the_title_names_the_metric_and_the_gate(self):
        """The limit comes from the benchmarks themselves, so it cannot drift from the gate."""
        self.assertEqual(
            charted(rendered())["title"],
            '"Instructions per message, head against base (gate 2%)"',
        )

    def test_every_allocation_change_is_named_however_small(self):
        """Allocations are counted, not sampled: a difference is the code, never the run."""
        self.assertIn("Allocation changes: out-slot 15.0 -> 17.0.", rendered())

    def test_a_run_that_allocated_the_same_says_so(self):
        """The line is always there, so its absence never has to be read as an omission."""
        self.assertIn("Allocations: no change.", rendered("bench-summary-partial.json"))

    def test_the_table_is_folded_under_the_summary(self):
        """The comment reads short by default and keeps every row one click away."""
        text = rendered()
        self.assertIn("<details>\n<summary>Every scenario</summary>\n\n| Scenario |", text)
        self.assertTrue(text.rstrip().endswith("</details>"))
        self.assertLess(text.index("```mermaid"), text.index("<details>"))

    def test_the_header_names_both_commits_and_what_the_base_run_was(self):
        """The base is the target branch's library under this pull request's own suite."""
        text = rendered()
        self.assertIn("Head `1111111` against base `2222222`.", text)
        self.assertIn(
            "Base: the sources of `2222222` measured with this pull request's suite and "
            "profile, so the two sides differ in library code alone.",
            text,
        )

    def test_a_tripped_limit_is_reported_with_its_metric(self):
        """A failing gate names each limit once, however many of the scenario's runs tripped it."""
        cells = row(rendered(), "publish through an Out slot with one transform")
        self.assertEqual(cells[5], "+3.00%")
        self.assertEqual(
            cells[7],
            "fail: instructions over the 2% limit; allocations 32028 over 30028",
        )

    def test_the_tripped_scenarios_are_listed_for_the_gate(self):
        """The step that fails the job says which scenarios it failed on."""
        self.assertEqual(
            bench_comment.tripped(measured()),
            [
                "publish through an Out slot with one transform: instructions over the 2% "
                "limit; allocations 32028 over 30028"
            ],
        )

    def test_a_scenario_measured_without_a_gate_says_so(self):
        """The cold path is published and never fails the run."""
        cells = row(rendered(), "a delivery that asks to be redelivered, and the copy")
        self.assertEqual(cells[2], "-")
        self.assertEqual(cells[7], "not gated")

    def test_a_scenario_the_run_does_not_carry_is_left_empty(self):
        """A comment is posted after a run that may have died part way through."""
        cells = row(
            rendered("bench-summary-partial.json"), "reply, encoded to a declared destination"
        )
        self.assertEqual(cells[1:], ["-"] * 7)

    def test_without_a_baseline_the_change_column_is_empty(self):
        """A run with nothing to compare against must not read as a run that did not move."""
        text = rendered("bench-summary-no-baseline.json", base="")
        self.assertIn("measured without a baseline", text)
        cells = row(text, "consume, JSON decode into a small struct")
        self.assertEqual(cells[1], "2378.1")
        self.assertEqual(cells[5], "-")

    def test_a_half_written_line_is_skipped(self):
        """A run cut off mid-line still reports everything measured before it."""
        complete = (FIXTURES / "bench-summary.json").read_text()
        with tempfile.TemporaryDirectory() as scratch:
            cut = Path(scratch) / "bench-summary.json"
            cut.write_text(complete + '{"benchmark_file": "benches/re')
            found = bench_comment.runs(cut)
        self.assertEqual(len(found), len(measured()))


if __name__ == "__main__":
    unittest.main()
