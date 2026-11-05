"""Reconstruct Chapter 5.1 coverage tables from stored V2 artifacts only.

Run from the repository root with pandas installed:
    python V2/analysis_outputs/section_5_1_coverage.py

Does not refit detectors or alter source outputs. RQ1 result CSVs are permission
restricted in this workspace; the corresponding table is emitted with blank
measurements and an explicit status rather than inferred detector outputs.
"""
from pathlib import Path
import csv
import json
import hashlib
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
BASE = ROOT / "V2" / "analysis_outputs"
OUT = BASE / "section_5_1"
OUT.mkdir(exist_ok=True)

panel_path = BASE / "repository_week_panel_audited.csv"
panel = pd.read_csv(panel_path)
period_masks = {
    "development_holdout": panel["split"].eq("holdout"),
    "external_calibration": panel["external_calibration"].eq(True),
    "external_evaluation": panel["external_evaluation_eligible"].eq(True),
}

# Availability means a stored non-null weekly value. Support counts are sums of
# the corresponding observation denominators, and remain distinct from weeks.
metrics = {
    "latency": ("latency_log", "latency_n"),
    "failure_rate": ("failure_rate", "outcome_n"),
    "rerun_rate": ("rerun_rate", "logical_run_n"),
}
support_rows = []
for period, mask in period_masks.items():
    for repo, g in panel.loc[mask].groupby("repo_full", sort=True):
        row = {
            "repository": repo, "role": "development" if period == "development_holdout" else "external",
            "period": period, "planned_weeks": len(g),
            "attempt_records": int(g["attempts_total"].sum()),
            "eligible_outcomes": int(g["outcome_n"].sum()),
            "failure_outcomes": int(g["failure_count"].sum()),
            "logical_runs": int(g["logical_run_n"].sum()),
            "rerun_logical_runs": int(g["rerun_count"].sum()),
            "additional_attempts": int(g["additional_attempt_count"].sum()),
            "low_outcome_support_weeks": int(g["low_outcome_support"].fillna(False).sum()),
            "low_latency_support_weeks": int(g["low_latency_support"].fillna(False).sum()),
            "missing_core_metric_weeks": int(g["missing_core_metric_count"].gt(0).sum()),
        }
        for name, (value, count) in metrics.items():
            row[f"{name}_valid_weeks"] = int(g[value].notna().sum())
            row[f"{name}_support_count"] = int(g[count].sum())
        support_rows.append(row)
pd.DataFrame(support_rows).to_csv(OUT / "measurement_support.csv", index=False)

# The exact source is frozen and listed in RQ1_BASELINES_FROZEN.json, but the
# environment denies reads of both the weekly and summary CSV. Leave values NA.
detectors = ["latency_MAD_persistent", "failure_rate_MAD_persistent",
             "rerun_rate_MAD_persistent", "persistent_union",
             "persistent_two_of_three", "MEWMA"]
eval_rows = []
for period, mask in period_masks.items():
    for repo, g in panel.loc[mask].groupby("repo_full", sort=True):
        population = "development_holdout" if period == "development_holdout" else "external_evaluation"
        for detector in detectors:
            eval_rows.append({
                "repository": repo, "period": period, "detector": detector,
                "planned_weeks": len(g), "evaluable_weeks": pd.NA,
                "unavailable_weeks": pd.NA, "alarm_weeks": pd.NA,
                "evaluability_proportion": pd.NA, "alarm_prevalence_evaluable_only": pd.NA,
                "source_population": population,
                "status": "not reconstructed: permission denied reading rq1_baseline_week_results.csv and rq1_baseline_summary.csv",
            })
pd.DataFrame(eval_rows).to_csv(OUT / "rq1_detector_evaluability.csv", index=False)

# Scenario registry counts: all planned injected rows stay in the denominator,
# including scenarios whose stored status marks an evaluation limitation.
scenario_path = BASE / "rq2_stage_b" / "rq2_stage_b_scenarios.csv"
sc = pd.read_csv(scenario_path)
sc["injected"] = sc["scenario_type"].ne("no_injection")
group_cols = ["scenario_type", "volume_condition", "missingness_condition", "magnitude_level", "duration_weeks"]
rrows = []
for keys, g in sc.groupby(group_cols, dropna=False, sort=True):
    d = dict(zip(group_cols, keys))
    d.update({"scenario_rows_all_repositories": len(g),
              "evaluable_status_rows": int(g["status"].eq("evaluable").sum()),
              "other_status_rows": int(g["status"].ne("evaluable").sum()),
              "in_primary_injected_denominator": bool(g["scenario_type"].ne("no_injection").all()),
              "scenario_file": "analysis_outputs/rq2_stage_b/rq2_stage_b_scenarios.csv"})
    rrows.append(d)
pd.DataFrame(rrows).to_csv(OUT / "rq2_scenario_coverage.csv", index=False)

# Pair availability is reported separately from scenario status/detection.
paired_path = BASE / "rq2_stage_b" / "rq2_stage_b_paired_week_results.csv.gz"
pair_rows = []
for chunk in pd.read_csv(paired_path, usecols=["scenario_id", "repo_full", "detector_id", "pair_evaluable"], chunksize=500_000):
    agg = chunk.groupby(["repo_full", "detector_id"], dropna=False).agg(
        paired_week_rows=("pair_evaluable", "size"), paired_week_evaluable=("pair_evaluable", "sum"))
    for (repo, detector), row in agg.iterrows():
        pair_rows.append({"repository": repo, "detector_id": detector,
                          "paired_week_rows": int(row.paired_week_rows),
                          "paired_week_evaluable": int(row.paired_week_evaluable)})
pair = pd.DataFrame(pair_rows).groupby(["repository", "detector_id"], as_index=False).sum()
pair["paired_week_unavailable"] = pair["paired_week_rows"] - pair["paired_week_evaluable"]
pair["paired_week_unavailability_proportion"] = pair["paired_week_unavailable"] / pair["paired_week_rows"]
pair["interpretation"] = "paired-week availability; not an excluded-scenario proportion"
pair.to_csv(OUT / "rq2_paired_week_availability.csv", index=False)

def sha(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()

source_files = [
    "analysis_outputs/MEASUREMENT_FROZEN.json", "analysis_outputs/measurement_metadata.json",
    "analysis_outputs/repository_week_panel_audited.csv", "analysis_outputs/attempt_measurements.csv.gz",
    "analysis_outputs/RQ1_BASELINES_FROZEN.json", "analysis_outputs/rq1_baseline_week_results.csv",
    "analysis_outputs/rq1_baseline_summary.csv", "analysis_outputs/RQ2_STAGE_B_PROTOCOL_FROZEN.json",
    "analysis_outputs/RQ2_STAGE_B_RESULTS_FROZEN.json", "analysis_outputs/rq2_stage_b/rq2_stage_b_scenarios.csv",
    "analysis_outputs/rq2_stage_b/rq2_stage_b_paired_week_results.csv.gz",
]
inventory = {"scope": "stored-output reconstruction; no detector fit or complete analysis rerun",
             "notes": {"Master_CI_Monitoring_Study_Note": "not found in repository",
                       "MEWMA_Checked_Methods_Note_EN": "not found in repository"},
             "sources": []}
for rel in source_files:
    p = BASE / rel.removeprefix("analysis_outputs/") if rel.startswith("analysis_outputs/") else ROOT / "V2" / rel
    entry = {"path": "V2/" + rel, "exists": p.exists()}
    if p.exists():
        entry["bytes"] = p.stat().st_size
        try:
            entry["sha256"] = sha(p)
            entry["readable"] = True
        except PermissionError:
            entry["sha256"] = None
            entry["readable"] = False
    inventory["sources"].append(entry)
(OUT / "source_inventory.json").write_text(json.dumps(inventory, indent=2), encoding="utf-8")

print(f"Wrote Chapter 5.1 coverage tables to {OUT}")
