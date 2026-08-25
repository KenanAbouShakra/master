from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

from rq2_run_stage_b import (
    EXPECTED_OUTPUTS,
    SUMMARY_KEYS,
    SUMMARY_METRICS,
    summary_frame,
    update_summary,
    verify_protocol_freeze,
)


EXPECTED_SCENARIOS = 42003
EXPECTED_DETECTOR_ROWS = 252018
EXPECTED_WEEK_ROWS = 11592828
EXPECTED_SUMMARY_ROWS = 2538
EXPECTED_GRID_SHA256 = "b17550141accf12d8b01729baa7dc48e8fb7a63d226d747639bbbce6e2f17937"
PRIMARY_DETECTORS = {
    "mad:latency_log", "mad:failure_rate", "mad:rerun_rate",
    "mad:union", "mad:two_of_three", "mewma:mewma",
}


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_yaml(path: Path) -> dict:
    value = yaml.safe_load(path.read_text(encoding="utf-8-sig"))
    if not isinstance(value, dict):
        raise RuntimeError(f"YAML root is not a mapping: {path}")
    return value


def count_csv_rows(path: Path) -> int:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return max(sum(1 for _ in handle) - 1, 0)


def canonical(value):
    if pd.isna(value):
        return None
    if isinstance(value, (np.integer, int)):
        return int(value)
    if isinstance(value, (np.floating, float)):
        return float(value)
    return str(value)


def compare_summaries(observed: pd.DataFrame, rebuilt: pd.DataFrame) -> dict:
    keys = list(SUMMARY_KEYS)
    observed = observed.sort_values(keys, na_position="first").reset_index(drop=True)
    rebuilt = rebuilt.sort_values(keys, na_position="first").reset_index(drop=True)
    if len(observed) != len(rebuilt):
        return {"rows_match": False, "maximum_absolute_difference": None}
    key_match = all(
        [canonical(value) for value in observed[column]]
        == [canonical(value) for value in rebuilt[column]]
        for column in keys
    )
    numeric_columns = [
        column for column in observed.columns
        if column not in keys and column in rebuilt.columns
    ]
    maximum = 0.0
    mismatch = False
    for column in numeric_columns:
        left = pd.to_numeric(observed[column], errors="coerce").to_numpy(float)
        right = pd.to_numeric(rebuilt[column], errors="coerce").to_numpy(float)
        same_missing = np.array_equal(np.isnan(left), np.isnan(right))
        finite = np.isfinite(left) & np.isfinite(right)
        difference = float(np.max(np.abs(left[finite] - right[finite]))) if finite.any() else 0.0
        maximum = max(maximum, difference)
        mismatch |= (not same_missing) or difference > 1e-12
    return {
        "rows_match": True,
        "keys_match": key_match,
        "numeric_values_match_tolerance_1e-12": not mismatch,
        "maximum_absolute_difference": maximum,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="stage_b_confirmatory_config.yaml")
    parser.add_argument("--chunk-size", type=int, default=250000)
    args = parser.parse_args()
    config_path = Path(args.config).resolve()
    root = config_path.parent
    cfg = load_yaml(config_path)
    output = root / cfg["outputs"]["directory"]
    failures: dict[str, str] = {}

    try:
        protocol = verify_protocol_freeze(root, cfg)
    except Exception as exc:
        protocol = {"error": f"{type(exc).__name__}: {exc}"}
        failures["protocol_freeze"] = protocol["error"]

    missing = [name for name in EXPECTED_OUTPUTS if not (output / name).is_file()]
    if missing:
        failures["missing_outputs"] = ", ".join(missing)
        print(json.dumps({"status": "FAIL", "failures": failures}, indent=2))
        return 1

    metadata = json.loads((output / "rq2_stage_b_metadata.json").read_text(encoding="utf-8"))
    status = json.loads((output / "rq2_stage_b_validation_status.json").read_text(encoding="utf-8"))
    scenarios = pd.read_csv(output / "rq2_stage_b_scenarios.csv", low_memory=False)
    detector_path = output / "rq2_stage_b_detector_results.csv.gz"
    summary_observed = pd.read_csv(output / "rq2_stage_b_primary_summary.csv", low_memory=False)

    scenario_checks = {
        "rows": int(len(scenarios)),
        "unique_scenario_ids": int(scenarios["scenario_id"].nunique()),
        "unique_seeds": int(scenarios["seed"].nunique()),
        "grid_sha256": sha256(output / "rq2_stage_b_scenarios.csv"),
    }
    if scenario_checks["rows"] != EXPECTED_SCENARIOS:
        failures["scenario_rows"] = str(scenario_checks["rows"])
    if scenario_checks["unique_scenario_ids"] != EXPECTED_SCENARIOS:
        failures["scenario_ids"] = str(scenario_checks["unique_scenario_ids"])
    if scenario_checks["unique_seeds"] != EXPECTED_SCENARIOS:
        failures["scenario_seeds"] = str(scenario_checks["unique_seeds"])
    if scenario_checks["grid_sha256"] != EXPECTED_GRID_SHA256:
        failures["scenario_grid_sha256"] = scenario_checks["grid_sha256"]

    aggregation: dict = {}
    detector_rows = 0
    pair_counts: Counter = Counter()
    observed_detectors: set[str] = set()
    unknown_scenarios = 0
    scenario_ids = set(scenarios["scenario_id"].astype(str))
    for chunk in pd.read_csv(detector_path, chunksize=args.chunk_size, low_memory=False):
        detector_rows += len(chunk)
        observed_detectors.update(chunk["detector_id"].dropna().astype(str))
        unknown_scenarios += int((~chunk["scenario_id"].astype(str).isin(scenario_ids)).sum())
        pair_counts.update(zip(chunk["scenario_id"].astype(str), chunk["detector_id"].astype(str)))
        for row in chunk.to_dict("records"):
            update_summary(aggregation, row)
    duplicate_pairs = sum(count != 1 for count in pair_counts.values())
    if detector_rows != EXPECTED_DETECTOR_ROWS:
        failures["detector_rows"] = str(detector_rows)
    if observed_detectors != PRIMARY_DETECTORS:
        failures["detector_set"] = json.dumps(sorted(observed_detectors))
    if len(pair_counts) != EXPECTED_DETECTOR_ROWS or duplicate_pairs:
        failures["scenario_detector_pairs"] = f"unique={len(pair_counts)}, nonunit={duplicate_pairs}"
    if unknown_scenarios:
        failures["unknown_scenarios"] = str(unknown_scenarios)

    rebuilt = summary_frame(aggregation)
    summary_check = compare_summaries(summary_observed, rebuilt)
    if not all([
        summary_check.get("rows_match", False),
        summary_check.get("keys_match", False),
        summary_check.get("numeric_values_match_tolerance_1e-12", False),
    ]):
        failures["summary_reconstruction"] = json.dumps(summary_check)
    if len(summary_observed) != EXPECTED_SUMMARY_ROWS:
        failures["summary_rows"] = str(len(summary_observed))

    week_rows = 0
    week_logic_failures = 0
    week_path = output / "rq2_stage_b_paired_week_results.csv.gz"
    for chunk in pd.read_csv(week_path, chunksize=args.chunk_size, low_memory=False):
        week_rows += len(chunk)
        evaluable = chunk["pair_evaluable"].eq(True) | chunk["pair_evaluable"].astype(str).str.lower().eq("true")
        reference = chunk["reference_alarm"].eq(True) | chunk["reference_alarm"].astype(str).str.lower().eq("true")
        injected = chunk["injected_alarm"].eq(True) | chunk["injected_alarm"].astype(str).str.lower().eq("true")
        incremental = chunk["incremental_alarm"].eq(True) | chunk["incremental_alarm"].astype(str).str.lower().eq("true")
        expected_incremental = evaluable & injected & ~reference
        week_logic_failures += int((incremental != expected_incremental).sum())
    if week_rows != EXPECTED_WEEK_ROWS:
        failures["paired_week_rows"] = str(week_rows)
    if week_logic_failures:
        failures["incremental_alarm_logic"] = str(week_logic_failures)

    metadata_checks = {
        "status_pass": metadata.get("status") == "PASS",
        "scenario_rows": metadata.get("scenario_rows") == EXPECTED_SCENARIOS,
        "detector_rows": metadata.get("detector_result_rows") == EXPECTED_DETECTOR_ROWS,
        "paired_week_rows": metadata.get("paired_week_rows") == EXPECTED_WEEK_ROWS,
        "summary_rows": metadata.get("summary_rows") == EXPECTED_SUMMARY_ROWS,
        "detector_not_refitted": metadata.get("detector_refitted") is False,
        "runner_status_pass": status.get("status") == "PASS",
        "runner_count_match": status.get("detector_row_count_matches") is True,
    }
    for name, passed in metadata_checks.items():
        if not passed:
            failures[f"metadata_{name}"] = "false"

    hashes = {name: sha256(output / name) for name in EXPECTED_OUTPUTS}
    result = {
        "status": "PASS" if not failures else "FAIL",
        "failures": failures,
        "protocol_freeze": protocol,
        "scenario_checks": scenario_checks,
        "detector_checks": {
            "rows": detector_rows,
            "unique_scenario_detector_pairs": len(pair_counts),
            "detectors": sorted(observed_detectors),
            "unknown_scenario_rows": unknown_scenarios,
        },
        "paired_week_checks": {
            "rows": week_rows,
            "incremental_alarm_logic_failures": week_logic_failures,
        },
        "summary_checks": {"rows": len(summary_observed), **summary_check},
        "metadata_checks": metadata_checks,
        "output_sha256": hashes,
    }
    print("RQ2 STAGE-B INDEPENDENT VALIDATION")
    print(json.dumps(result, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())