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
import os
import subprocess
import sys
import tomllib
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent

# Deliveries per measured run, the default of the suite's `MESSAGES` constant. Every published
# number is per message, so the totals are divided by it. `just bench N` builds the
# benches with another count and passes the same one here through `--messages`.
DEFAULT_MESSAGES = 1000
MESSAGES = DEFAULT_MESSAGES

# An instruction count below this on a default-count run means the measurement broke, not that
# the code got faster: the cheapest scenario here costs a thousand times more. The floor scales
# with the count. The cold run handles one delivery, so it is held to a much lower floor - but
# not to none, because a collapsed measurement reports nearly nothing at all.
FLOOR_PER_DEFAULT_RUN = 100_000
FLOOR = FLOOR_PER_DEFAULT_RUN
COLD_FLOOR = 1_000


def configure(messages):
    """Measure against another count of deliveries per run: the per-message division and the
    floor a run is held to follow it."""
    global MESSAGES, FLOOR
    if messages <= 0:
        sys.exit("--messages must be a positive number of deliveries")
    MESSAGES = messages
    FLOOR = FLOOR_PER_DEFAULT_RUN * messages // DEFAULT_MESSAGES


class Scenario:
    """One published row: what it is called, which benchmark measured each half, and whether a
    regression in it fails CI.

    `key` is the short name a narrow report writes instead of the sentence: the chart in the
    pull request comment is read in one column, and a scenario has to be recognisable in ten
    characters. The sentence stays everywhere there is room for it.
    """

    def __init__(self, name, key, framework, hand, gated=True, note=None):
        self.name = name
        self.key = key
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
        "json",
        "consume_json/service",
        "consume_json/by_hand",
    ),
    Scenario(
        "consume, JSON decode of a 1 KB body",
        "json-1kb",
        "consume_json_kilobyte/service",
        "consume_json_kilobyte/by_hand",
    ),
    Scenario(
        "consume on the byte lane, no codec",
        "lane",
        "consume_lane/service",
        "consume_lane/by_hand",
    ),
    Scenario(
        "consume through a middleware stack of one",
        "mw1",
        "middleware/service_one",
        "middleware/by_hand",
    ),
    Scenario(
        "consume through a middleware stack of four",
        "mw4",
        "middleware/service_four",
        "middleware/by_hand",
    ),
    Scenario(
        "consume in batches of 64",
        "batch64",
        "batch/service",
        "batch/by_hand",
    ),
    Scenario(
        "reply, encoded to a declared destination",
        "reply",
        "reply/service",
        "reply/by_hand",
    ),
    Scenario(
        "publish through an Out slot with one transform",
        "out-slot",
        "out_slot/service",
        "out_slot/by_hand",
    ),
    Scenario(
        "publish with a typed header contract",
        "hdr-write",
        "typed_headers_write/service",
        "typed_headers_write/by_hand",
    ),
    Scenario(
        "read a typed header contract, then publish",
        "hdr-read",
        "typed_headers_read/service",
        "typed_headers_read/by_hand",
    ),
    Scenario(
        "request and reply, one round trip",
        "req-reply",
        "request_reply/service",
        "request_reply/by_hand",
    ),
    Scenario(
        "a delivery that asks to be redelivered, and the copy",
        "retry",
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


def rounded(figure):
    """A per-message figure at the precision the published tables read it in.

    A cost that falls below one per message is shown to three places, so that "nothing per
    message, one allocation for the whole run" does not read as a flat zero.
    """
    return round(figure, 3) if abs(figure) < 1 else round(figure, 1)


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


def read(path, default=None):
    """One line of a sysfs or proc file, or `default` where the file is not there."""
    try:
        return Path(path).read_text().strip()
    except OSError:
        return default


def proc_cpuinfo():
    return read("/proc/cpuinfo", "") or ""


def cpu_model():
    for line in proc_cpuinfo().splitlines():
        if line.startswith("model name"):
            return line.split(":", 1)[1].strip()
    return platform.processor() or "unknown"


# The feature sets the x86-64 levels are defined by, coarsest first. A level is claimed only when
# every flag of it and of the levels below is present, so this names what the binary could have
# been built for rather than guessing a marketing name for the chip.
X86_LEVELS = [
    ("x86-64-v2", ["sse4_2", "popcnt", "ssse3"]),
    ("x86-64-v3", ["avx2", "bmi1", "bmi2", "fma", "movbe"]),
    ("x86-64-v4", ["avx512f", "avx512bw", "avx512cd", "avx512dq", "avx512vl"]),
]


def architecture():
    machine = platform.machine()
    flags = set()
    for line in proc_cpuinfo().splitlines():
        if line.startswith("flags"):
            flags = set(line.split(":", 1)[1].split())
            break
    level = None
    for name, needed in X86_LEVELS:
        if all(flag in flags for flag in needed):
            level = name
        else:
            break
    return f"{machine} ({level})" if level else machine


def megahertz(path):
    value = read(path)
    try:
        return round(int(value) / 1000)
    except (TypeError, ValueError):
        return None


def cpu_frequency():
    """Base and maximum clock, as the kernel reports them.

    A CPU whose firmware does not publish a base clock (every AMD one here) says so rather than
    having a number invented for it.
    """
    cpufreq = "/sys/devices/system/cpu/cpu0/cpufreq"
    base = megahertz(f"{cpufreq}/base_frequency")
    top = megahertz(f"{cpufreq}/cpuinfo_max_freq")
    bottom = megahertz(f"{cpufreq}/cpuinfo_min_freq")
    parts = [f"base {base} MHz" if base else "base unknown"]
    if bottom:
        parts.append(f"min {bottom} MHz")
    parts.append(f"max {top} MHz" if top else "max unknown")
    return ", ".join(parts)


def cores():
    """Physical and logical count, counted off the topology the kernel publishes."""
    physical = set()
    package = core = None
    for line in proc_cpuinfo().splitlines():
        if line.startswith("physical id"):
            package = line.split(":", 1)[1].strip()
        elif line.startswith("core id"):
            core = line.split(":", 1)[1].strip()
            physical.add((package, core))
    logical = os.cpu_count() or 0
    count = len(physical) or logical
    return f"{count} physical, {logical} logical"


def memory():
    for line in (read("/proc/meminfo", "") or "").splitlines():
        if line.startswith("MemTotal"):
            kib = int(line.split()[1])
            return f"{kib / 1024 / 1024:.1f} GiB"
    return "unknown"


def memory_speed(previous=None):
    """Module speed and type, out of the DMI tables where they can be read at all.

    They are root-only on most Linux systems, and a benchmark run is not worth a root shell. A
    value a run cannot read is not lost, though: where the document already published one and the
    machine still looks like the one it was published from, it is carried over, so an ordinary run
    does not overwrite what a privileged one found. Otherwise the answer is `unknown` rather than
    a guess.
    """
    try:
        dmi = subprocess.run(
            ["dmidecode", "--type", "memory"], capture_output=True, text=True, timeout=10
        )
    except (OSError, subprocess.SubprocessError):
        return previous or "unknown"
    if dmi.returncode != 0:
        return previous or "unknown"
    speeds = set()
    kinds = set()
    for line in dmi.stdout.splitlines():
        line = line.strip()
        if line.startswith("Configured Memory Speed:") or line.startswith("Speed:"):
            value = line.split(":", 1)[1].strip()
            if value and "Unknown" not in value:
                speeds.add(value)
        elif line.startswith("Type:") and "Unknown" not in line:
            kinds.add(line.split(":", 1)[1].strip())
    if not speeds:
        return previous or "unknown"
    return ", ".join(sorted(kinds | speeds))


# What cargo compiles a release with when the manifest says nothing. The benchmark profile
# inherits release, so this is where its description starts.
RELEASE_DEFAULTS = {"opt-level": 3, "lto": False, "codegen-units": 16, "debug": False}


def profile():
    """The profile the benchmarks are built with, as it actually resolves."""
    manifest = tomllib.loads((REPO / "Cargo.toml").read_text())
    profiles = manifest.get("profile", {})
    resolved = dict(RELEASE_DEFAULTS)
    resolved.update(profiles.get("release", {}))
    resolved.update(profiles.get("bench", {}))
    resolved.pop("inherits", None)
    settings = ", ".join(
        f"{key} = {str(value).lower() if isinstance(value, bool) else value}"
        for key, value in resolved.items()
    )
    return f"bench, inheriting release ({settings})"


def features():
    """The feature list the benchmarks are built with, read where it is defined."""
    justfile = (REPO / "justfile").read_text()
    match = re.search(r'^bench_features := "([^"]+)"', justfile, re.MULTILINE)
    if not match:
        sys.exit("cannot read bench_features from the justfile")
    return f"--no-default-features --features {match.group(1)}"


def published_memory_speed(output):
    """The memory speed the document already carries, when it describes this machine.

    Matched on the processor and the memory size: a document written on another machine says
    nothing about this one's modules.
    """
    try:
        previous = json.loads(output.read_text()).get("environment", {})
    except (OSError, ValueError):
        return None
    if previous.get("cpu") != cpu_model() or previous.get("memory") != memory():
        return None
    speed = previous.get("memory_speed")
    return None if speed in (None, "unknown") else speed


def environment(output):
    """The machine and the build, so a number can be read against what produced it."""
    valgrind = command("valgrind", "--version")
    return {
        "cpu": cpu_model(),
        "architecture": architecture(),
        "cpu_frequency": cpu_frequency(),
        "cores": cores(),
        "memory": memory(),
        "memory_speed": memory_speed(published_memory_speed(output)),
        "os": f"{platform.system()} {platform.release()}",
        "rustc": command("rustc", "--version").split()[1],
        "valgrind": valgrind.removeprefix("valgrind-"),
        "profile": profile(),
        "features": features(),
        "rustflags": "",
    }


def crate_version():
    manifest = (REPO / "Cargo.toml").read_text()
    match = re.search(r'^version = "([^"]+)"', manifest, re.MULTILINE)
    if not match:
        sys.exit("cannot read the crate version from Cargo.toml")
    return match.group(1)


def totals(found, key, count, floor=None):
    """One benchmark's totals, checked for the two ways this measurement fails silently."""
    if floor is None:
        floor = FLOOR
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
        steady[metric_name] = rounded((twice[metric_name] - base[metric_name]) / MESSAGES)
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


def allocation_ratio(framework, hand):
    """The framework's allocations per message as a multiple of the hand-written half's.

    `=` says both halves sit at the same count (the usual case is zero against zero), a number
    is the multiple, and `n/a` is a scenario with no hand-written half or a hand-written half
    that allocates nothing while the framework does, where a multiple would be infinite.
    """
    if hand is None:
        return "n/a"
    ours = framework["allocations"]
    theirs = hand["allocations"]
    if ours == theirs:
        return "="
    if theirs == 0:
        return "n/a"
    return f"{ours / theirs:.2f}x"


def report(rows):
    """The same table the page publishes, for a terminal and for a CI job summary."""
    header = (
        f"{'scenario':<52}{'framework':>11}{'by hand':>10}{'overhead':>10}"
        f"{'alloc':>8}{'by hand':>9}{'ratio':>7}{'cold instr':>12}{'cold alloc':>12}"
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
            f"{hand['allocations'] if hand else '-':>9}"
            f"{allocation_ratio(row['framework'], hand):>7}"
            f"{cold['instructions']:>12}"
            f"{cold['allocations']:>12}"
        )
    print()
    print(
        f"instructions and allocations per message in the steady state over {MESSAGES} "
        "deliveries; the ratio is the framework's"
    )
    print(
        "allocations as a multiple of the hand-written half's; cold is what starting the service"
        " and handling the first delivery cost once"
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("summary", type=Path, help="the JSON the benchmark run wrote")
    parser.add_argument("output", type=Path, help="where to write the results document")
    parser.add_argument(
        "--messages",
        type=int,
        default=DEFAULT_MESSAGES,
        help="deliveries per measured run, the count the benches were built with",
    )
    args = parser.parse_args()
    configure(args.messages)

    version = crate_version()
    document = {
        "schema": 2,
        "crate": "ruststream",
        "crate_version": version,
        "core_version": version,
        "measured_at": datetime.now(timezone.utc).date().isoformat(),
        "environment": environment(args.output),
        "scenarios": [],
        "code": build(measurements(args.summary)),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(document, indent=2) + "\n")
    report(document["code"])


if __name__ == "__main__":
    main()
