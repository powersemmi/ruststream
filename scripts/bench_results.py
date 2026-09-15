#!/usr/bin/env python3
"""Turn a benchmark run into the document the benchmarks page reads.

Input is the machine-readable summary `cargo bench -- --output-format=json` writes, one JSON
object per benchmark. Output is `docs/benchmarks/results.json` (schema 2): the `code` section,
one entry per scenario, with instructions and allocations per message for the framework and for
the hand-written loop it is compared against, plus what starting the service cost once.

Every scenario is measured three times: over one delivery, over MESSAGES of them, and over twice
MESSAGES. The slope between the last two is the steady-state cost of a message - everything that
happens once is in both totals and cancels in the subtraction. The one-delivery run is the cold
start itself: starting the service and handling the first delivery, measured rather than derived,
because the intercept of a line through two totals of several million carries the noise of both
and comes out negative as easily as not.

Two things are checked while converting, because both failures are silent in the benchmark
output itself:

- every scenario named below is present, so a renamed benchmark is an error rather than a row
  that quietly disappears from the published table;
- no scenario reports an implausibly small instruction count. Collection is switched on for one
  frame, and a frame that stops matching (inlining, a rename) leaves the run reporting the cost
  of the process exit and nothing else. That reads as a spectacular improvement rather than as
  the broken measurement it is.
"""

import argparse
import json
import platform
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent

# Deliveries per measured run, the `MESSAGES` constant of the suite. Every published number is
# per message, so the totals are divided by it.
MESSAGES = 1000

# An instruction count below this on a 1000-message run means the measurement broke, not that the
# code got faster: the cheapest scenario here costs a thousand times more. The cold run handles
# one delivery, so it is held to a much lower floor - but not to none, because a collapsed
# measurement reports nearly nothing at all.
FLOOR = 100_000
COLD_FLOOR = 1_000


class Scenario:
    """One published row: what it is called, which benchmark measured each half, and whether a
    regression in it fails CI."""

    def __init__(self, name, framework, hand, gated=True, note=None):
        self.name = name
        self.framework = framework
        self.hand = hand
        self.gated = gated
        self.note = note


# The three runs of every scenario, by the benchmark id that carries each: the cold start on its
# own, and the two counts whose difference is the steady state.
COLD = "first"
COUNTS = ("base", "twice")

# The table, in reading order. Each half names a benchmark as `file/function`: one scenario per
# benchmark file, the framework half in `service` and the hand-written one in `by_hand`, each
# measured at both counts.
SCENARIOS = [
    Scenario(
        "consume, JSON decode into a small struct",
        "consume_json/service",
        "consume_json/by_hand",
    ),
    Scenario(
        "consume, JSON decode of a 1 KB body",
        "consume_json_kilobyte/service",
        "consume_json_kilobyte/by_hand",
    ),
    Scenario(
        "consume on the byte lane, no codec",
        "consume_lane/service",
        "consume_lane/by_hand",
    ),
    Scenario(
        "consume through a middleware stack of one",
        "middleware/service_one",
        "middleware/by_hand",
    ),
    Scenario(
        "consume through a middleware stack of four",
        "middleware/service_four",
        "middleware/by_hand",
    ),
    Scenario(
        "consume in batches of 64",
        "batch/service",
        "batch/by_hand",
    ),
    Scenario(
        "reply, encoded to a declared destination",
        "reply/service",
        "reply/by_hand",
    ),
    Scenario(
        "publish through an Out slot with one transform",
        "out_slot/service",
        "out_slot/by_hand",
    ),
    Scenario(
        "publish with a typed header contract",
        "typed_headers_write/service",
        "typed_headers_write/by_hand",
    ),
    Scenario(
        "read a typed header contract, then publish",
        "typed_headers_read/service",
        "typed_headers_read/by_hand",
    ),
    Scenario(
        "request and reply, one round trip",
        "request_reply/service",
        "request_reply/by_hand",
    ),
    Scenario(
        "a delivery that asks to be redelivered, and the copy",
        "retry_copy/service",
        None,
        gated=False,
        note="cold path: measured and reported, never gated",
    ),
]


def metric(summary, tool, name):
    """The new value of one metric, out of the nested summary the runner emits."""
    for profile in summary["profiles"]:
        metrics = profile["summaries"]["parts"][0]["metrics_summary"].get(tool)
        if not metrics or name not in metrics:
            continue
        values = metrics[name]["metrics"]
        # A benchmark with nothing to compare against - a first run, or one whose baseline was
        # taken before it existed - reports the new value alone under a different key.
        entry = values["Both"][0] if "Both" in values else next(iter(values.values()))
        return int(entry["Int"])
    return None


def measurements(path):
    """Every benchmark in the run, keyed by `file/function/id`.

    The file is part of the key because one scenario per file means the same two function names
    in every one of them.
    """
    found = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        summary = json.loads(line)
        scenario = Path(summary["benchmark_file"]).stem
        key = f"{scenario}/{summary['function_name']}/{summary['id']}"
        found[key] = {
            "instructions": metric(summary, "Callgrind", "Ir"),
            "allocations": metric(summary, "Dhat", "TotalBlocks"),
        }
    return found


def per_message(value):
    """A run total as a per-message figure, rounded the way a reader reads it."""
    if value is None:
        return None
    figure = value / MESSAGES
    if figure < 1:
        # A fixed cost spread over the run rather than a per-message one: shown to three places
        # so that "nothing per message, one allocation for the run" does not read as a flat zero.
        return round(figure, 3)
    return round(figure, 2 if figure < 10 else 1)


def command(*args):
    return subprocess.run(args, capture_output=True, text=True, check=True).stdout.strip()


def cpu_model():
    for line in Path("/proc/cpuinfo").read_text().splitlines():
        if line.startswith("model name"):
            return line.split(":", 1)[1].strip()
    return platform.processor() or "unknown"


def environment():
    valgrind = command("valgrind", "--version")
    cores = command("nproc")
    return {
        "cpu": f"{cpu_model()}, {cores} cores",
        "os": f"{platform.system()} {platform.release()}",
        "rustc": command("rustc", "--version").split()[1],
        "valgrind": valgrind.removeprefix("valgrind-"),
        "profile": "bench (release), default codegen flags",
        "rustflags": "",
    }


def crate_version():
    manifest = (REPO / "Cargo.toml").read_text()
    match = re.search(r'^version = "([^"]+)"', manifest, re.MULTILINE)
    if not match:
        sys.exit("cannot read the crate version from Cargo.toml")
    return match.group(1)


def totals(found, key, count, floor=FLOOR):
    """One benchmark's totals, checked for the two ways this measurement fails silently."""
    full = f"{key}/{count}"
    if full not in found:
        sys.exit(f"benchmark {full} is not in the run: rename it here or in benches/")
    measured = found[full]
    if measured["instructions"] is None or measured["instructions"] < floor:
        sys.exit(
            f"benchmark {full} reports {measured['instructions']} instructions, which is below "
            f"the floor of {floor}: collection did not cover the measured region"
        )
    return measured


def half(found, key):
    """The steady state and the cold start of one half of a pair.

    The slope between the two counts is what a message costs once the service is running; the
    one-delivery run is what starting it and taking that delivery cost.
    """
    if key is None:
        return None, None
    base = totals(found, key, COUNTS[0])
    twice = totals(found, key, COUNTS[1])
    if twice["instructions"] <= base["instructions"]:
        sys.exit(
            f"benchmark {key} does not grow with the message count "
            f"({base['instructions']} -> {twice['instructions']}): the two runs measured the "
            f"same work, so the slope means nothing"
        )
    first = totals(found, key, COLD, COLD_FLOOR)
    steady = {}
    for metric_name in ("instructions", "allocations"):
        per = (twice[metric_name] - base[metric_name]) / MESSAGES
        steady[metric_name] = round(per, 3) if abs(per) < 1 else round(per, 1)
    cold = {name: first[name] for name in ("instructions", "allocations")}
    return steady, cold


def build(found):
    rows = []
    for scenario in SCENARIOS:
        framework, cold = half(found, scenario.framework)
        hand, _ = half(found, scenario.hand)
        row = {
            "name": scenario.name,
            "messages": MESSAGES,
            "framework": framework,
            "hand_written": hand,
            "cold": cold,
            "gated": scenario.gated,
        }
        if hand:
            row["overhead"] = {
                "instructions": round(
                    framework["instructions"] - hand["instructions"], 1
                ),
                "allocations": round(
                    framework["allocations"] - hand["allocations"], 3
                ),
            }
        if scenario.note:
            row["note"] = scenario.note
        rows.append(row)
    return rows


def report(rows):
    """The same table the page publishes, for a terminal and for a CI job summary."""
    header = (
        f"{'scenario':<52}{'framework':>11}{'by hand':>10}{'overhead':>10}"
        f"{'alloc':>8}{'cold instr':>12}{'cold alloc':>12}"
    )
    print(header)
    print("-" * len(header))
    for row in rows:
        hand = row["hand_written"]
        overhead = row.get("overhead", {})
        cold = row["cold"]
        print(
            f"{row['name']:<52}"
            f"{row['framework']['instructions']:>11}"
            f"{hand['instructions'] if hand else '-':>10}"
            f"{overhead.get('instructions', '-'):>10}"
            f"{row['framework']['allocations']:>8}"
            f"{cold['instructions']:>12}"
            f"{cold['allocations']:>12}"
        )
    print()
    print(
        "instructions and allocations per message in the steady state; cold is what starting the"
    )
    print("service and handling the first delivery cost once")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("summary", type=Path, help="the JSON the benchmark run wrote")
    parser.add_argument("output", type=Path, help="where to write the results document")
    args = parser.parse_args()

    version = crate_version()
    document = {
        "schema": 2,
        "crate": "ruststream",
        "crate_version": version,
        "core_version": version,
        "measured_at": datetime.now(timezone.utc).date().isoformat(),
        "environment": environment(),
        "scenarios": [],
        "code": build(measurements(args.summary)),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(document, indent=2) + "\n")
    report(document["code"])


if __name__ == "__main__":
    main()
