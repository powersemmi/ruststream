#!/usr/bin/env python3
"""Turn a coverage run into the report the pull request carries.

Input is what `cargo llvm-cov report --summary-only` prints: one row per file, then a total. Output
is markdown on stdout: the line coverage of the whole workspace against the gate, and the files
holding the most uncovered lines - where the gate is lost, and the only part of a two-hundred-row
report worth reading in a comment. The whole report stays in the run's job summary.
"""

import argparse
import sys
from pathlib import Path

# The columns llvm-cov prints after the file name, in order. Only the line ones are reported; the
# rest are read so that a report whose shape changed fails here instead of publishing the wrong
# column as coverage.
COLUMNS = (
    "regions",
    "missed_regions",
    "region_cover",
    "functions",
    "missed_functions",
    "executed",
    "lines",
    "missed_lines",
    "line_cover",
    "branches",
    "missed_branches",
    "branch_cover",
)

TOTAL = "TOTAL"

# Rows in the comment. Enough to name what a pull request would have to cover to move the total,
# short enough to read at a glance.
ROWS = 10


def percent(value):
    """One percentage of the report, or nothing where llvm-cov had none to print."""
    try:
        return float(value.rstrip("%"))
    except ValueError:
        return None


def parse(text):
    """Every row of the summary, the total among them under the name llvm-cov gives it."""
    rows = []
    for line in text.splitlines():
        fields = line.split()
        if len(fields) != len(COLUMNS) + 1 or fields[0] == "Filename":
            continue
        name, values = fields[0], fields[1:]
        row = {"name": name}
        for column, value in zip(COLUMNS, values):
            row[column] = percent(value) if column.endswith("cover") else value
        try:
            row["lines"] = int(row["lines"])
            row["missed_lines"] = int(row["missed_lines"])
        except ValueError:
            continue
        rows.append(row)
    return rows


def worst(rows, limit):
    """The files holding the most uncovered lines, the ones with none left out."""
    uncovered = [
        row for row in rows if row["missed_lines"] > 0 and row["line_cover"] is not None
    ]
    uncovered.sort(key=lambda row: (-row["missed_lines"], row["name"]))
    return uncovered[:limit]


def line(cells):
    return "| " + " | ".join(cells) + " |"


def comment(rows, gate, head):
    total = next((row for row in rows if row["name"] == TOTAL), None)
    if total is None:
        sys.exit("the summary carries no total row: nothing to report")
    if total["line_cover"] is None:
        sys.exit("the total row carries no line coverage: nothing to report")
    verdict = "pass" if total["line_cover"] >= gate else "fail"
    files = [row for row in rows if row["name"] != TOTAL]
    text = [
        "## Coverage",
        "",
        f"Line coverage {total['line_cover']:.2f}% against the {gate:g}% gate: {verdict}. "
        f"{total['missed_lines']} of {total['lines']} lines are uncovered"
        + (f", at `{head[:7]}`." if head else "."),
        "",
        line(["File", "Lines", "Uncovered", "Coverage"]),
        line(["---", "---:", "---:", "---:"]),
    ]
    for row in worst(files, ROWS):
        text.append(
            line(
                [
                    f"`{row['name']}`",
                    str(row["lines"]),
                    str(row["missed_lines"]),
                    f"{row['line_cover']:.2f}%",
                ]
            )
        )
    text.extend(
        [
            "",
            f"The {ROWS} files holding the most uncovered lines. The run's job summary carries "
            "the whole report, and the lcov artifact the lines themselves.",
        ]
    )
    return "\n".join(text) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("summary", type=Path, help="what the coverage run printed")
    parser.add_argument(
        "--gate", type=float, default=95.0, help="the line coverage CI fails under"
    )
    parser.add_argument("--head", default="", help="the commit that was measured")
    args = parser.parse_args()

    rows = parse(args.summary.read_text())
    if not rows:
        sys.exit("the summary carries no rows: nothing to report")
    sys.stdout.write(comment(rows, args.gate, args.head))


if __name__ == "__main__":
    main()
