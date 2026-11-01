#!/usr/bin/env python3
"""Collect three-run public medians for docs/build_report.py.

Run after T1-T4 and a T5 profile have been saved as JSON by the supplied
run_demo.py or experiments/run_impairment_tests.sh. This reads the existing
measurements; it does not change the supplied harness or recalculate events.
"""

from __future__ import annotations

import argparse
import json
import statistics
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
METRICS = {
    "p50": "p50_ms",
    "p95": "p95_ms",
    "p99": "p99_ms",
    "fresh": "fresh_pct",
    "fresh_finals": "fresh_final_pct",
    "complete": "final_completeness_pct",
    "coverage": "normal_update_coverage_pct",
}
CONDITIONS = {
    "T1": "20 normal clients",
    "T2": "100 normal clients",
    "T3": "100; 10% slow",
    "T4": "100; 20% reconnect",
}


def nested(summary: dict, path: str):
    value = summary
    for part in path.split("."):
        if not isinstance(value, dict) or part not in value:
            return None
        value = value[part]
    return value


def summarize(paths: list[Path], label: str, condition: str) -> dict:
    if len(paths) != 3:
        raise SystemExit(f"{label}: expected exactly three JSON runs; found {len(paths)}: {paths}")
    runs = [json.loads(path.read_text(encoding="utf-8")) for path in paths]
    result = {"condition": condition}
    for display, key in METRICS.items():
        values = [nested(run, key) for run in runs]
        if any(value is None for value in values):
            raise SystemExit(f"{label}: missing {key} in one or more runs")
        median = statistics.median(float(value) for value in values)
        result[display] = f"{median:.0f}" if display in ("p50", "p95", "p99") else f"{median:.2f}%"
    result["min_complete"] = min(float(run["final_completeness_pct"]) for run in runs)
    result["errors_by_run"] = [int(run.get("connection_errors", 0)) for run in runs]
    result["duplicates_by_run"] = [int(run.get("duplicates", 0)) for run in runs]
    result["stale_by_run"] = [int(run.get("stale", 0)) for run in runs]
    for name, key in (("normal_p95", "normal_clients.p95_ms"),
                      ("slow_p95", "slow_clients.p95_ms"),
                      ("recovery_p95", "recovery_p95_ms")):
        values = [nested(run, key) for run in runs]
        if all(value is not None for value in values):
            result[name] = statistics.median(float(value) for value in values)
    result["files"] = [str(path.relative_to(ROOT)) if path.is_relative_to(ROOT) else str(path) for path in paths]
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--t5-folder", type=Path, required=True,
                        help="results/impairment-... folder produced by run_impairment_tests.sh")
    parser.add_argument("--t5-scenario", default="t5_delay_loss",
                        help="T5 profile to put in the main report table")
    args = parser.parse_args()

    data = {}
    for test, condition in CONDITIONS.items():
        stem = test.lower()
        paths = sorted((ROOT / "results").glob(f"{stem}_run[0-9][0-9].json"))
        data[test] = summarize(paths, test, condition)
    folder = args.t5_folder.resolve()
    t5_paths = sorted(folder.glob(f"{args.t5_scenario}_run[0-9][0-9].json"))
    data["T5"] = summarize(t5_paths, "T5", args.t5_scenario.replace("_", " "))

    output = ROOT / "docs" / "report_data.json"
    output.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
    for test, result in data.items():
        print(f"{test}: P95={result['p95']} ms, final completeness={result['complete']}, "
              f"minimum={result['min_complete']:.2f}%, errors={result['errors_by_run']}")
    print(f"Wrote {output}")


if __name__ == "__main__":
    main()
