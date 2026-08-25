from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd


EXPECTED_STAGE_A_MANIFEST_HASH = (
    "aaa51548f571e71ff7e44fe6150943ed71ea5ed6849cb8314706a0e0b55534e0"
)
EXPECTED_RQ1_MANIFEST_HASH = (
    "d871b9c08870c57486985e601d448466c0baecbbd505679dce767ddf385bf55d"
)

OUTCOME_NAMES = {
    "precision",
    "recall",
    "false_alarm_rate",
    "episode_detection_rate",
    "detection_delay_weeks",
    "boundary_overlap",
    "alarm_duration_weeks",
    "unevaluable_fraction",
}

CELL_COLUMNS = [
    "repo_full",
    "scenario_type",
    "affected_metrics",
    "magnitude_level",
    "magnitude",
    "duration_weeks",
    "volume_condition",
    "missingness_condition",
]


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def manifest_inventory(root: Path, manifest: dict) -> list[dict]:
    rows = []
    for relative, record in manifest.get("files", {}).items():
        path = root / relative
        rows.append({
            "file": relative,
            "suffix": path.suffix.lower(),
            "exists": path.is_file(),
            "bytes": path.stat().st_size if path.is_file() else None,
            "expected_bytes": record.get("bytes"),
            "hash_matches": sha256(path) == record.get("sha256") if path.is_file() else False,
        })
    return rows


def inspect_tabular(path: Path) -> dict | None:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        frame = pd.read_csv(path, low_memory=False)
    elif suffix in {".parquet", ".pq"}:
        frame = pd.read_parquet(path)
    else:
        return None
    columns = list(frame.columns)
    raw_outcomes = sorted(OUTCOME_NAMES.intersection(columns))
    mean_outcomes = sorted(
        column for column in columns
        if column.startswith("mean_") and column[5:] in OUTCOME_NAMES
    )
    return {
        "file": str(path),
        "rows": int(len(frame)),
        "columns": columns,
        "has_scenario_id": "scenario_id" in columns,
        "has_detector_identity": bool(
            {"detector_family", "detector_variant"}.intersection(columns)
        ),
        "raw_outcome_columns": raw_outcomes,
        "mean_outcome_columns": mean_outcomes,
        "raw_scenario_detector_candidate": bool(
            "scenario_id" in columns
            and raw_outcomes
            and {"detector_family", "detector_variant"}.intersection(columns)
        ),
    }


def counts(series: pd.Series) -> dict:
    return {
        str(key): int(value)
        for key, value in series.fillna("<NA>").astype(str).value_counts(dropna=False).items()
    }


def main() -> int:
    root = Path.cwd()
    out = root / "analysis_outputs"
    stage_path = out / "STAGE_A_FROZEN.json"
    rq1_path = out / "RQ1_BASELINES_FROZEN.json"
    failures: dict[str, object] = {}

    for path in (stage_path, rq1_path):
        if not path.is_file():
            failures[path.name] = "missing"
    if failures:
        print("RQ2 STAGE-A REPLICATION-CONTRACT AUDIT")
        print(json.dumps({"status": "FAIL", "failures": failures}, indent=2))
        return 2

    if sha256(stage_path) != EXPECTED_STAGE_A_MANIFEST_HASH:
        failures[stage_path.name] = "unexpected manifest hash"
    if sha256(rq1_path) != EXPECTED_RQ1_MANIFEST_HASH:
        failures[rq1_path.name] = "unexpected manifest hash"

    stage_manifest = json.loads(stage_path.read_text(encoding="utf-8"))
    manifest_files = manifest_inventory(root, stage_manifest)
    invalid_frozen = [
        item["file"] for item in manifest_files
        if not item["exists"] or not item["hash_matches"]
    ]
    if invalid_frozen:
        failures["invalid_frozen_stage_a_files"] = invalid_frozen

    scenario_path = out / "stage_a_synthetic_scenarios.csv"
    if not scenario_path.is_file():
        failures[scenario_path.name] = "missing"
        scenarios = pd.DataFrame()
    else:
        scenarios = pd.read_csv(scenario_path, low_memory=False)

    required = set(CELL_COLUMNS + ["scenario_id", "repetition", "seed", "status"])
    missing_columns = sorted(required - set(scenarios.columns))
    if missing_columns:
        failures["scenario_missing_columns"] = missing_columns

    scenario_report: dict[str, object] = {}
    if not scenarios.empty and not missing_columns:
        duplicate_ids = int(scenarios["scenario_id"].duplicated().sum())
        supported = scenarios["status"].astype(str).str.lower().isin(
            {"supported", "pass", "valid", "ok"}
        )
        cells = scenarios.groupby(CELL_COLUMNS, dropna=False).agg(
            scenario_rows=("scenario_id", "size"),
            repetitions=("repetition", "nunique"),
            seeds=("seed", "nunique"),
        ).reset_index()
        scenario_report = {
            "rows": int(len(scenarios)),
            "unique_scenario_ids": int(scenarios["scenario_id"].nunique()),
            "duplicate_scenario_ids": duplicate_ids,
            "unique_seeds": int(scenarios["seed"].nunique()),
            "status_counts": counts(scenarios["status"]),
            "scenario_type_counts": counts(scenarios["scenario_type"]),
            "unsupported_reason_counts": (
                counts(scenarios.loc[~supported, "unsupported_reason"])
                if "unsupported_reason" in scenarios.columns else {}
            ),
            "design_cells": int(len(cells)),
            "scenario_rows_per_cell": {
                "minimum": int(cells["scenario_rows"].min()),
                "median": float(cells["scenario_rows"].median()),
                "maximum": int(cells["scenario_rows"].max()),
            },
            "repetitions_per_cell": {
                "minimum": int(cells["repetitions"].min()),
                "median": float(cells["repetitions"].median()),
                "maximum": int(cells["repetitions"].max()),
            },
            "seeds_per_cell": {
                "minimum": int(cells["seeds"].min()),
                "median": float(cells["seeds"].median()),
                "maximum": int(cells["seeds"].max()),
            },
        }
        if duplicate_ids:
            failures["duplicate_scenario_ids"] = duplicate_ids

    candidate_paths: list[Path] = []
    for item in manifest_files:
        path = root / item["file"]
        if path.suffix.lower() in {".csv", ".parquet", ".pq"} and path.is_file():
            candidate_paths.append(path)
    for pattern in ("*.csv", "*.parquet", "*.pq"):
        candidate_paths.extend(out.glob(pattern))

    tabular = []
    seen: set[Path] = set()
    for path in candidate_paths:
        resolved = path.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        try:
            item = inspect_tabular(path)
            if item is not None:
                tabular.append(item)
        except Exception as exc:
            failures[str(path)] = f"{type(exc).__name__}: {exc}"

    raw_candidates = [
        item["file"] for item in tabular if item["raw_scenario_detector_candidate"]
    ]

    result = {
        "status": "PASS" if not failures else "FAIL",
        "failures": failures,
        "stage_a_manifest_sha256": sha256(stage_path),
        "rq1_manifest_sha256": sha256(rq1_path),
        "frozen_stage_a_files": manifest_files,
        "scenario_registry": scenario_report,
        "raw_scenario_detector_result_candidates": raw_candidates,
        "stage_b_implication": (
            "Raw scenario-detector outcomes exist and must be audited before designing Stage B."
            if raw_candidates else
            "No raw scenario-detector outcome table was found; Stage B must create new locked raw outcomes without modifying Stage A."
        ),
        "tabular_files": tabular,
    }
    print("RQ2 STAGE-A REPLICATION-CONTRACT AUDIT")
    print(json.dumps(result, indent=2))
    return 0 if result["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())