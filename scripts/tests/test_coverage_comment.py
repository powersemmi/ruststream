#!/usr/bin/env python3
"""What the coverage comment says, against a report recorded in CI.

The fixture is what `cargo llvm-cov report --summary-only` printed on a run of the workspace, cut
to fifteen files and its total.
"""

import sys
import unittest
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[1]
FIXTURES = Path(__file__).resolve().parent / "fixtures"

# The formatters are scripts rather than a package, so the directory holding them goes on the path
# and they are imported the way the workflow runs them.
sys.path.insert(0, str(SCRIPTS))

import coverage_comment


def rendered(gate=95.0, head="1111111aaaaaaa"):
    rows = coverage_comment.parse((FIXTURES / "coverage-summary.txt").read_text())
    return coverage_comment.comment(rows, gate, head)


class Report(unittest.TestCase):
    def test_the_total_is_read_against_the_gate(self):
        """The number the gate is decided on, the gate itself, and the outcome."""
        self.assertIn(
            "Line coverage 95.67% against the 95% gate: pass. 683 of 15790 lines are "
            "uncovered, at `1111111`.",
            rendered(),
        )

    def test_a_total_below_the_gate_reads_as_a_failure(self):
        """The comment must not say pass where the job went red."""
        self.assertIn("against the 96% gate: fail.", rendered(gate=96))

    def test_the_files_are_the_ones_holding_the_most_uncovered_lines(self):
        """A comment names where the coverage is lost, not the whole report."""
        files = [
            line.split("|")[1].strip().strip("`")
            for line in rendered().splitlines()
            if line.startswith("| `")
        ]
        self.assertEqual(
            files[:4],
            [
                "conformance/harness.rs",
                "runtime/dispatch.rs",
                "runtime/retry/mod.rs",
                "bin/ruststream/cargo.rs",
            ],
        )
        self.assertLessEqual(len(files), coverage_comment.ROWS)

    def test_a_fully_covered_file_is_left_out(self):
        """Rows a reader can do nothing about are noise."""
        self.assertNotIn("asyncapi/viewer.rs", rendered())

    def test_a_row_carries_its_own_lines_and_coverage(self):
        """Per file: how much code it is, how much of it is uncovered, where that leaves it."""
        row = next(
            line for line in rendered().splitlines() if "conformance/harness.rs" in line
        )
        cells = [cell.strip() for cell in row.strip("|").split("|")]
        self.assertEqual(cells, ["`conformance/harness.rs`", "297", "59", "80.13%"])


if __name__ == "__main__":
    unittest.main()
