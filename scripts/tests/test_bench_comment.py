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
    """The Mermaid source of the chart, line by line, without its fences."""
    return text.split("```mermaid")[1].split("```")[0].strip("\n").splitlines()


def directive(lines, keyword):
    """What the line opening with this keyword carries after it."""
    for line in lines:
        if line.strip().startswith(keyword):
            return line.strip()[len(keyword) :].strip()
    raise AssertionError(f"no {keyword} in the chart")


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
        axis = directive(charted(rendered()), "x-axis")
        self.assertEqual(
            [key.strip().strip('"') for key in axis.strip("[]").split(",")],
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

    def test_a_scenario_shows_one_bar_in_the_series_of_its_direction(self):
        """Magnitudes in two series: the direction is the colour, so the bar grows from zero."""
        lines = charted(rendered())
        self.assertEqual(lines[1], "xychart-beta horizontal")
        fewer = series(directive(lines, 'bar "fewer"'))
        more = series(directive(lines, 'bar "more"'))
        self.assertEqual(fewer[:4], [3.0, 0.0, 0.5, 0.2])
        self.assertEqual(more[:4], [0.0, 3.0, 0.0, 0.0])
        self.assertTrue(all(value >= 0 for value in fewer + more))
        self.assertTrue(all(not (one and other) for one, other in zip(fewer, more)))
        names = directive(lines, "x-axis").strip("[]").split(",")
        self.assertEqual(len(fewer), len(names))
        self.assertEqual(len(more), len(names))

    def test_the_axis_starts_at_zero_and_clears_the_gate(self):
        """Mermaid grows a bar from the floor of the range, so the floor has to be zero."""
        self.assertEqual(
            directive(charted(rendered()), "y-axis"),
            '"change, % (green: fewer instructions, red: more)" 0 --> 5',
        )

    def test_the_axis_top_rounds_outwards_and_never_hides_the_limit(self):
        """The worst bar sits inside the chart, and the limit line is always on it."""
        self.assertEqual(bench_comment.axis_top([-25.8, -2.3], 2.0), 30)
        self.assertEqual(bench_comment.axis_top([-0.4], 2.0), 5)
        self.assertEqual(bench_comment.axis_top([0.0], None), 5)

    def test_the_colours_come_from_an_init_directive(self):
        """Green for cheaper and red for dearer, in the order the series are declared."""
        first = charted(rendered())[0]
        self.assertTrue(first.startswith("%%{init:"), first)
        self.assertIn('"plotColorPalette": "#2da44e, #cf222e, #cf222e"', first)

    def test_the_limit_is_drawn_as_a_line_across_the_chart(self):
        """One value per scenario, all at the limit: a bar past it is over the gate."""
        lines = charted(rendered())
        self.assertEqual(
            series(directive(lines, "line")),
            [2.0] * len(series(directive(lines, 'bar "fewer"'))),
        )

    def test_the_title_names_the_metric_and_the_gate(self):
        """The limit comes from the benchmarks themselves, so it cannot drift from the gate."""
        self.assertEqual(
            directive(charted(rendered()), "title"),
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
