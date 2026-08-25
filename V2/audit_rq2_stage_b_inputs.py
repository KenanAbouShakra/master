from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd


CONDITION_COLUMNS = (
    "repo_full",
    "detector_family",
    "detector_variant",
    "metric",
    "affected_metrics",
    "magnitude_level",
    "magnitude",
    "duration_weeks",
    "volume_condition",
    "missingness_condition",
)

IDENTIFIER_COLUMNS = (
    "scenario_id",
    "scenario",
    "replicate",
    "repetition",
    "seed",
    "random_seed",
    "injection_id",
)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def verify_manifest(root: Path, manifest_path: Path) -> tuple[dict, dict]:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    failures: dict[str, str] = {}
    for relative, record in manifest.get("files", {}).items():
        path = root / relative
        if not path.is_file():
            failures[relative] = "missing"
        elif path.stat().st_size != int(record["bytes"]):
            failures[relative] = "size mismatch"
        elif sha256(path) != record["sha256"]:
            failures[relative] = "hash mismatch"
    return manifest, failures


def compact_values(series: pd.Series, limit: int = 30) -> dict:
    values = series.dropna().astype(str).drop_duplicates().sort_values().tolist()
    return {
        "unique_count": len(values),
        "values": values[:limit],
        "truncated": len(values) > limit,
    }


def inspect_csv(path: Path) -> dict:
    frame = pd.read_csv(path, low_memory=False)
    identifiers = [column for column in IDENTIFIER_COLUMNS if column in frame.columns]
    mean_columns = [column for column in frame.columns if column.startswith("mean_")]
    valid_count_columns = [
        column for column in frame.columns if column.endswith("_valid_observations")
    ]
    conditions = {
        column: compact_values(frame[column])
        for column in CONDITION_COLUMNS
        if column in frame.columns
    }
    return {
        "file": str(path),
        "rows": int(len(frame)),
        "columns": list(frame.columns),
        "identifier_columns": identifiers,
        "mean_metric_columns": mean_columns,
        "valid_observation_columns": valid_count_columns,
        "appears_aggregated": bool(mean_columns or valid_count_columns),
        "condition_levels": conditions,
    }


def main() -> int:
    root = Path.cwd()
    output = root / "analysis_outputs"
    stage_manifest_path = output / "STAGE_A_FROZEN.json"
    rq1_manifest_path = output / "RQ1_BASELINES_FROZEN.json"
    failures: dict[str, object] = {}

    if not stage_manifest_path.is_file():
        failures["STAGE_A_FROZEN.json"] = "missing"
    if not rq1_manifest_path.is_file():
        failures["RQ1_BASELINES_FROZEN.json"] = "missing"
    if failures:
        print("RQ2 STAGE-B READ-ONLY INPUT INVENTORY")
        print(json.dumps({"status": "FAIL", "failures": failures}, indent=2))
        return 2

    stage_manifest, stage_failures = verify_manifest(root, stage_manifest_path)
    rq1_manifest, rq1_failures = verify_manifest(root, rq1_manifest_path)
    if stage_failures:
        failures["stage_a_freeze"] = stage_failures
    if rq1_failures:
        failures["rq1_freeze"] = rq1_failures

    frozen_stage_csvs = []
    for relative in stage_manifest.get("files", {}):
        path = root / relative
        if path.suffix.lower() == ".csv" and path.is_file():
            frozen_stage_csvs.append(path)

    discovered = sorted(output.glob("stage_a*.csv"))
    csvs = []
    seen: set[Path] = set()
    for path in [*frozen_stage_csvs, *discovered]:
        resolved = path.resolve()
        if resolved not in seen:
            seen.add(resolved)
            csvs.append(path)

    tables = []
    for path in csvs:
        try:
            tables.append(inspect_csv(path))
        except Exception as exc:
            failures[str(path)] = f"{type(exc).__name__}: {exc}"

    scenario_level_candidates = [
        item["file"] for item in tables
        if item["identifier_columns"] and not item["appears_aggregated"]
    ]
    aggregate_only = bool(tables) and not scenario_level_candidates

    result = {
        "status": "PASS" if not failures else "FAIL",
        "failures": failures,
        "stage_a_manifest_status": stage_manifest.get("status"),
        "stage_a_manifest_sha256": sha256(stage_manifest_path),
        "stage_a_files_verified": len(stage_manifest.get("files", {})),
        "rq1_manifest_status": rq1_manifest.get("status"),
        "rq1_manifest_sha256": sha256(rq1_manifest_path),
        "rq1_files_verified": len(rq1_manifest.get("files", {})),
        "stage_a_csv_tables_found": len(tables),
        "scenario_level_candidates": scenario_level_candidates,
        "aggregate_only_indication": aggregate_only,
        "interpretation": (
            "Scenario-level candidates found; inspect identifiers and replication contract."
            if scenario_level_candidates else
            "No unaggregated scenario-level table was identified by schema; Stage B may require new locked scenario outputs."
        ),
        "tables": tables,
    }
    print("RQ2 STAGE-B READ-ONLY INPUT INVENTORY")
    print(json.dumps(result, indent=2))
    return 0 if result["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())