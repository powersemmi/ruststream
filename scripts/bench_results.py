#!/usr/bin/env python3
"""Turn a benchmark run into the document the benchmarks page reads.

Input is the machine-readable summary `cargo bench -- --output-format=json` writes, one JSON
object per benchmark. Output is `docs/benchmarks/results.json` (schema 2): the `code` section,
one entry per scenario, with instructions and allocations per message for the framework and for
the hand-written loop it is compared against.

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
# code got faster: the cheapest scenario here costs a thousand times more.
FLOOR = 100_000


class Scenario:
    """One published row: what it is called, which benchmark measured each half, and whether a
    regression in it fails CI."""

    def __init__(self, name, framework, hand, gated=True, note=None):
        self.name = name
        self.framework = framework
        self.hand = hand
        self.gated = gated
        self.note = note


# The table, in reading order. Each half names a benchmark as `function/id`.
SCENARIOS = [
    Scenario(
        "consume, JSON decode into a small struct",
        "consume_json/small",
        "consume_json_hand/small",
    ),
    Scenario(
        "consume, JSON decode of a 1 KB body",
        "consume_json/kilobyte",
        "consume_json_hand/kilobyte",
    ),
    Scenario(
        "consume on the byte lane, no codec",
        "consume_lane/bytes",
        "consume_lane_hand/bytes",
    ),
    Scenario(
        "consume through a middleware stack of one",
        "middleware/one",
        "consume_json_hand/small",
    ),
    Scenario(
        "consume through a middleware stack of four",
        "middleware/four",
        "consume_json_hand/small",
    ),
    Scenario(
        "consume in batches of 64",
        "consume_batch_64/of_64",
        "consume_batch_64_hand/of_64",
    ),
    Scenario(
        "reply, encoded to a declared destination",
        "reply/json",
        "reply_hand/json",
    ),
    Scenario(
        "publish through an Out slot with one transform",
        "out_slot/one_transform",
        "out_slot_hand/one_transform",
    ),
    Scenario(
        "publish with a typed header contract",
        "typed_headers_write/write",
        "typed_headers_write_hand/write",
    ),
    Scenario(
        "read a typed header contract, then publish",
        "typed_headers_read/read",
        "typed_headers_read_hand/read",
    ),
    Scenario(
        "request and reply, one round trip",
        "request_reply/round_trip",
        "request_reply_hand/round_trip",
    ),
    Scenario(
        "a delivery that asks to be redelivered, and the copy",
        "retry_copy/once",
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
        entry = values["Both"][0] if "Both" in values else values["New"]
        return int(entry["Int"])
    return None


def measurements(path):
    """Every benchmark in the run, keyed by `function/id`."""
    found = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        summary = json.loads(line)
        key = f"{summary['function_name']}/{summary['id']}"
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


def half(found, key):
    if key is None:
        return None
    if key not in found:
        sys.exit(f"benchmark {key} is not in the run: rename it here or in benches/")
    measured = found[key]
    if measured["instructions"] is None or measured["instructions"] < FLOOR:
        sys.exit(
            f"benchmark {key} reports {measured['instructions']} instructions, which is below "
            f"the floor of {FLOOR}: collection did not cover the measured region"
        )
    return {
        "instructions": per_message(measured["instructions"]),
        "allocations": per_message(measured["allocations"]),
    }


def build(found):
    rows = []
    for scenario in SCENARIOS:
        framework = half(found, scenario.framework)
        hand = half(found, scenario.hand)
        row = {
            "name": scenario.name,
            "messages": MESSAGES,
            "framework": framework,
            "hand_written": hand,
            "gated": scenario.gated,
        }
        if hand:
            row["overhead"] = {
                "instructions": round(
                    framework["instructions"] - hand["instructions"], 1
                ),
                "allocations": round(
                    framework["allocations"] - hand["allocations"], 2
                ),
            }
        if scenario.note:
            row["note"] = scenario.note
        rows.append(row)
    return rows


def report(rows):
    """The same table the page publishes, for a terminal and for a CI job summary."""
    header = f"{'scenario':<52}{'framework':>12}{'by hand':>12}{'overhead':>12}{'allocations':>13}"
    print(header)
    print("-" * len(header))
    for row in rows:
        hand = row["hand_written"]
        overhead = row.get("overhead", {})
        print(
            f"{row['name']:<52}"
            f"{row['framework']['instructions']:>12}"
            f"{hand['instructions'] if hand else '-':>12}"
            f"{overhead.get('instructions', '-'):>12}"
            f"{row['framework']['allocations']:>13}"
        )
    print()
    print("instructions and allocations per message; overhead is the difference")


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
