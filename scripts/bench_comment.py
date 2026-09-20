#!/usr/bin/env python3
"""Turn a benchmark run into the report the pull request carries.

Input is the machine-readable summary `cargo bench -- --output-format=json` writes, the same file
`bench_results.py` reads, measured against a baseline taken on the target branch. Output is
markdown on stdout: one row per scenario with what a message costs, what that cost did against the
base commit, what starting the service cost once, and whether the run tripped a limit.

The scenarios and the arithmetic are `bench_results.py`'s, so a renamed benchmark moves in one
place. What differs is the tolerance: this report is posted after a run that may have failed, so a
scenario the summary does not carry becomes an empty row rather than an error. Whether every
scenario is present is the published document's gate, not this one's.
"""

import argparse
import json
import math
import re
import sys
from pathlib import Path

from bench_results import COLD, COUNTS, MESSAGES, REPO, SCENARIOS, rounded

# The two metrics of the table, under the tool and the key each is reported by.
INSTRUCTIONS = ("Callgrind", "Ir")
ALLOCATIONS = ("Dhat", "TotalBlocks")

# What the runner's metric keys are called in a sentence a reader reads.
METRIC_WORDS = {"Ir": "instructions", "TotalBlocks": "allocations"}

EMPTY = "-"


def number(entry):
    """One metric value, whichever of the two shapes the runner wrote it in."""
    if entry is None:
        return None
    if "Int" in entry:
        return int(entry["Int"])
    return float(entry["Float"])


def sides(summary, tool, name):
    """The head and the base value of one metric.

    The runner puts the new run on the left of a pair and the older one on the right; a run with
    nothing to compare against carries one side alone.
    """
    for profile in summary["profiles"]:
        metrics = profile["summaries"]["parts"][0]["metrics_summary"].get(tool)
        if not metrics or name not in metrics:
            continue
        values = metrics[name]["metrics"]
        if "Both" in values:
            head, base = values["Both"]
            return number(head), number(base)
        if "Left" in values:
            return number(values["Left"]), None
        return None, number(values["Right"])
    return None, None


def metric_word(kind):
    """The word for the metric a limit was declared on."""
    if isinstance(kind, dict):
        name = next(iter(kind.values()))
        return METRIC_WORDS.get(name, name)
    return str(kind)


def exceeded(summary):
    """Every limit this benchmark tripped: the metric, how far it went over, and the phrase.

    A limit declared on a scenario holds for each of its three runs, so the same metric trips in
    more than one of them; how far over is carried so that the report can keep the worst.
    """
    tripped = []
    for profile in summary["profiles"]:
        for regression in profile["summaries"]["total"]["regressions"]:
            for shape, body in regression.items():
                word = metric_word(body["metric"])
                if shape == "Soft":
                    # The size of the move is the table's own column; the phrase names the limit.
                    tripped.append(
                        (word, float(body["diff_pct"]), f"{word} over the {body['limit']}% limit")
                    )
                else:
                    new, limit = number(body["new"]), number(body["limit"])
                    tripped.append((word, new - limit, f"{word} {new} over {limit}"))
    return tripped


def runs(path):
    """Every benchmark of the run, keyed `file/function/id`."""
    found = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            summary = json.loads(line)
        except json.JSONDecodeError:
            # A run a failing limit cut short leaves its last line half written. Everything
            # measured before it is still worth reporting.
            continue
        key = (
            f"{Path(summary['benchmark_file']).stem}/"
            f"{summary['function_name']}/{summary['id']}"
        )
        found[key] = {
            "instructions": sides(summary, *INSTRUCTIONS),
            "allocations": sides(summary, *ALLOCATIONS),
            "exceeded": exceeded(summary),
        }
    return found


def steady(found, key):
    """What a message costs in this half of a scenario, on the head and on the base.

    The slope between the two message counts is the steady state: everything that happens once is
    in both totals and cancels in the subtraction.
    """
    if key is None:
        return None
    low = found.get(f"{key}/{COUNTS[0]}")
    high = found.get(f"{key}/{COUNTS[1]}")
    if not low or not high:
        return None
    measured = {"head": {}, "base": {}}
    for metric in ("instructions", "allocations"):
        for side, index in (("head", 0), ("base", 1)):
            start, end = low[metric][index], high[metric][index]
            if start is None or end is None:
                measured[side][metric] = None
            else:
                measured[side][metric] = rounded((end - start) / MESSAGES)
    return measured


def limits(found, scenario):
    """The limits the scenario tripped, one phrase per metric: the run that went furthest over."""
    worst = {}
    for key in (scenario.framework,):
        for count in (COLD, *COUNTS):
            for word, over, phrase in found.get(f"{key}/{count}", {}).get("exceeded", []):
                if word not in worst or over > worst[word][0]:
                    worst[word] = (over, phrase)
    return [phrase for _, phrase in worst.values()]


def change(measured):
    """The head instruction count against the base one, in percent."""
    if not measured:
        return EMPTY
    head, base = measured["head"]["instructions"], measured["base"]["instructions"]
    if head is None or base in (None, 0):
        return EMPTY
    return f"{(head - base) / base * 100:+.2f}%"


def figure(measured, side, metric):
    if not measured or measured[side][metric] is None:
        return EMPTY
    return f"{measured[side][metric]}"


def cold(found, scenario):
    """What starting the service and taking the first delivery cost, counted once."""
    first = found.get(f"{scenario.framework}/{COLD}")
    if not first:
        return EMPTY
    instructions, allocations = first["instructions"][0], first["allocations"][0]
    if instructions is None or allocations is None:
        return EMPTY
    return f"{instructions} / {allocations}"


def verdict(found, scenario, measured):
    if not scenario.gated:
        return "not gated"
    tripped = limits(found, scenario)
    if tripped:
        return "fail: " + "; ".join(tripped)
    if measured is None:
        return EMPTY
    return "pass"


def rows(found):
    for scenario in SCENARIOS:
        framework = steady(found, scenario.framework)
        yield [
            scenario.name,
            figure(framework, "head", "instructions"),
            figure(framework, "head", "allocations"),
            change(framework),
            cold(found, scenario),
            verdict(found, scenario, framework),
        ]


HEADER = [
    "Scenario",
    "Instructions",
    "Allocations",
    "Instruction change",
    "Cold start",
    "Gate",
]

ALIGNMENT = ["---", "---:", "---:", "---:", "---:", "---"]

LEGEND = (
    "Instructions and allocations are per message in the steady state, in the framework's own "
    "code over the in-process transport. The change is the instruction column against the base "
    "commit. Cold start is the instructions and the allocations of starting the service and "
    "taking the first delivery, counted once."
)


def compared(found):
    """Whether the run had a baseline to measure against at all."""
    return any(
        measured["instructions"][1] is not None for measured in found.values()
    )


def line(cells):
    return "| " + " | ".join(cells) + " |"


def percentage(head, base):
    """One total against the other, or nothing where the run cannot say."""
    if head is None or base is None:
        return None
    if base == 0:
        return 0.0 if head == 0 else None
    return (head - base) / base * 100


def totals(found, metric):
    """The framework's total over the gated scenarios, head and base."""
    sides = {"head": 0.0, "base": 0.0}
    for scenario in SCENARIOS:
        if not scenario.gated:
            continue
        measured = steady(found, scenario.framework)
        for side in sides:
            if measured is None or measured[side][metric] is None:
                return None, None
            sides[side] += measured[side][metric]
    return sides["head"], sides["base"]


def overall(found, metric):
    """What the gated scenarios together did to one metric, as a percentage."""
    moved = percentage(*totals(found, metric))
    return EMPTY if moved is None else f"{moved:+.2f}%"


def failing(found):
    """The gated scenarios that went over a limit."""
    return [
        scenario.name
        for scenario in SCENARIOS
        if scenario.gated and limits(found, scenario)
    ]


# The chart is a Mermaid diagram, which GitHub renders natively in a comment. A drawing made of
# characters does not survive the renderer: the run of spaces that aligned its columns is
# collapsed, and the block elements it was drawn with are not single-width in the font that gets
# them. A diagram is laid out by the renderer instead, so nothing here depends on a font.
CHART_TITLE = "Instructions per message, head against base"

# The value axis rounds outwards to a multiple of this, so it reads in steady ticks.
TICK = 5

# Every bar is drawn as a magnitude from zero, and the direction is the colour of the series it
# sits in: a chart whose range holds negatives grows its bars from the bottom of the range rather
# than from zero (mermaid-js/mermaid#5618), which reads as the opposite of what happened. The
# palette is handed to the series in order: cheaper green, dearer red, and the limit line red.
FEWER, MORE = "fewer", "more"
PALETTE = (
    '%%{init: {"themeVariables": {"xyChart": '
    '{"plotColorPalette": "#2da44e, #cf222e, #cf222e"}}}}%%'
)
AXIS = "change, % (green: fewer instructions, red: more)"


def gate_percent():
    """The limit a gated scenario is held to, read where the benchmarks declare it.

    A number copied into this file would drift from the one that fails the run; where the
    declaration cannot be read the heading says nothing rather than something stale.
    """
    try:
        source = (REPO / "benches" / "common" / "mod.rs").read_text()
    except OSError:
        return None
    match = re.search(r"soft_limits\(\[\(EventKind::Ir,\s*([\d.]+)f64\)\]\)", source)
    return float(match.group(1)) if match else None


def moves(found, metric):
    """What each scenario did to one metric, by short name, worst move first."""
    moved = []
    for scenario in SCENARIOS:
        measured = steady(found, scenario.framework)
        if measured is None:
            continue
        percent = percentage(measured["head"][metric], measured["base"][metric])
        if percent is not None:
            moved.append((scenario.key, percent))
    moved.sort(key=lambda row: -abs(row[1]))
    return moved


def allocation_line(found):
    """The scenarios whose allocations per message differ from the base at all.

    Allocations on the delivery path are counted, not sampled, so any difference is the code and
    worth naming however small it looks.
    """
    moved = []
    for scenario in SCENARIOS:
        measured = steady(found, scenario.framework)
        if measured is None:
            continue
        head, base = measured["head"]["allocations"], measured["base"]["allocations"]
        if head is None or base is None or head == base:
            continue
        moved.append(f"{scenario.key} {base} -> {head}")
    if not moved:
        return "Allocations: no change."
    return "Allocation changes: " + "; ".join(moved) + "."


def axis_top(values, gate):
    """Where the value axis ends: outwards to a whole tick, and never short of the limit.

    It starts at zero, because that is what a bar is read against; a range that stopped short of
    the limit would hide the one line a reader is looking for.
    """
    span = [abs(value) for value in values] + [gate or 0.0]
    return int(math.ceil(max(span) / TICK) * TICK) or TICK


def chart(found):
    """The drift, drawn: one bar per scenario, worst first, against the limit that gates them."""
    moved = moves(found, "instructions")
    if not moved:
        return []
    gate = gate_percent()
    title = CHART_TITLE + (f" (gate {gate:g}%)" if gate is not None else "")
    # `or 0.0` turns a negative zero, which a rounded tiny drop leaves behind, into a plain one.
    values = [round(percent, 1) or 0.0 for _, percent in moved]
    fewer = [abs(value) if value < 0 else 0.0 for value in values]
    more = [value if value > 0 else 0.0 for value in values]
    lines = [
        "```mermaid",
        PALETTE,
        "xychart-beta horizontal",
        f'    title "{title}"',
        "    x-axis [" + ", ".join(f'"{key}"' for key, _ in moved) + "]",
        f'    y-axis "{AXIS}" 0 --> {axis_top(values, gate)}',
        f'    bar "{FEWER}" [' + ", ".join(f"{value}" for value in fewer) + "]",
        f'    bar "{MORE}" [' + ", ".join(f"{value}" for value in more) + "]",
    ]
    if gate is not None:
        # A flat series at the limit: a bar past it is over the gate, and the eye finds that
        # without reading a number.
        lines.append("    line [" + ", ".join(f"{gate:g}" for _ in values) + "]")
    lines.append("```")
    return lines


def summary(found):
    """The three lines a reader gets before opening the table."""
    tripped_names = failing(found)
    gate = "pass" if not tripped_names else "fail on " + "; ".join(tripped_names)
    instructions, allocations = overall(found, "instructions"), overall(found, "allocations")
    if EMPTY in (instructions, allocations):
        headline = (
            f"The run does not carry every gated scenario, so there is no total. Gate: {gate}."
        )
    else:
        headline = (
            f"Instructions across the gated scenarios: {instructions} against the base. "
            f"Allocations: {allocations}. Gate: {gate}."
        )
    return [headline, "", *chart(found), "", allocation_line(found)]


def comment(found, head, base):
    text = ["## Cost of the code", ""]
    if compared(found):
        text.append(f"Head `{head[:7]}` against base `{base[:7]}`.")
        text.append("")
        text.append(
            f"Base: the sources of `{base[:7]}` measured with this pull request's suite and "
            "profile, so the two sides differ in library code alone."
        )
    else:
        text.append(
            f"Head `{head[:7]}`, measured without a baseline: the base branch produced none, so "
            "the change column is empty and only the allocation limits were checked."
        )
    text.extend(["", *summary(found)])
    text.extend(["", "<details>", "<summary>Every scenario</summary>", ""])
    text.extend([line(HEADER), line(ALIGNMENT)])
    text.extend(line(cells) for cells in rows(found))
    text.extend(["", LEGEND, "", "</details>"])
    return "\n".join(text) + "\n"


def tripped(found):
    """One line per scenario that went over a limit, for the step that fails the job."""
    lines = []
    for scenario in SCENARIOS:
        phrases = limits(found, scenario)
        if phrases:
            lines.append(f"{scenario.name}: {'; '.join(phrases)}")
    return lines


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("summary", type=Path, help="the JSON the benchmark run wrote")
    parser.add_argument("--head", default="", help="the commit that was measured")
    parser.add_argument("--base", default="", help="the commit it was measured against")
    parser.add_argument(
        "--limits",
        action="store_true",
        help="list the scenarios that went over a limit instead of writing the table",
    )
    args = parser.parse_args()

    found = runs(args.summary)
    if not found:
        sys.exit("the run produced no measurement to report")
    if args.limits:
        for report in tripped(found):
            print(report)
        return
    sys.stdout.write(comment(found, args.head, args.base))


if __name__ == "__main__":
    main()
