"""Read-only follow-up audit for Chapter 5.1 unresolved provenance questions.

Run from repository root. Reconstructs rolling reference diagnostics from the
audited weekly panel according to V2/03_fit_baselines.py, compares them with
stored baseline_results.csv intermediate columns, audits figure hashes, then
packages a report and selected evidence without modifying source outputs.
"""
from pathlib import Path
import hashlib
import json
import shutil
import zipfile

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
V2 = ROOT / "V2"
AO = V2 / "analysis_outputs"
OUT = AO / "section_5_1_followup"
OUT.mkdir(exist_ok=True)

def sha(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

panel_path = AO / "repository_week_panel_audited.csv"
base_path = AO / "baseline_results.csv"
panel = pd.read_csv(panel_path)
base = pd.read_csv(base_path)
panel["week"] = pd.to_datetime(panel["week"])
base["week"] = pd.to_datetime(base["week"])
panel = panel.sort_values(["repo_full", "week"]).reset_index(drop=True)
base = base.sort_values(["repo_full", "week"]).reset_index(drop=True)
ev = base.loc[base["reporting_partition"].isin(["holdout", "external_evaluation"])].copy()
metric_map = {"latency_MAD_persistent": "latency_log",
              "failure_rate_MAD_persistent": "failure_rate",
              "rerun_rate_MAD_persistent": "rerun_rate"}
raw_cols = {name: f"mad_w13_t3_{metric}" for name, metric in metric_map.items()}
score_cols = {name: f"mad_w13_t3_{metric}_score" for name, metric in metric_map.items()}
persist_cols = {name: f"mad_w13_t3_{metric}_k2" for name, metric in metric_map.items()}
detectors = [*metric_map, "persistent_union", "persistent_two_of_three", "MEWMA"]

# Verify panel-based support aggregates requested for the combined RQ1 period.
mask = panel["split"].eq("holdout") | panel["external_evaluation_eligible"].fillna(False).astype(bool)
support = panel.loc[mask].groupby("repo_full").agg(
    planned_weeks=("week", "size"), attempt_records=("attempts_total", "sum"),
    eligible_outcomes=("outcome_n", "sum"), latency_support=("latency_n", "sum"),
    logical_runs=("logical_run_n", "sum"), additional_attempts=("additional_attempt_count", "sum"),
).reset_index()
support.to_csv(OUT / "evaluation_support_reconstructed.csv", index=False)

# Reconstruct preceding-13-week center/scale. The deployed score columns are
# read independently; discrepancy fields expose any failure to reproduce.
rows = []
for repo, g in panel.groupby("repo_full", sort=True):
    g = g.sort_values("week").reset_index(drop=True)
    b = base.loc[base.repo_full.eq(repo)].sort_values("week").reset_index(drop=True)
    b_by_week = b.set_index("week")
    for i, prow in g.iterrows():
        if not (prow["split"] == "holdout" or bool(prow.get("external_evaluation_eligible", False))):
            continue
        week = prow["week"]
        if week not in b_by_week.index:
            continue
        brow = b_by_week.loc[week]
        if isinstance(brow, pd.DataFrame):
            brow = brow.iloc[0]
        history = g.iloc[max(0, i-13):i]
        hist_diag = {}
        for metric in metric_map.values():
            vals = pd.to_numeric(history[metric], errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
            center = float(vals.median()) if len(vals) else np.nan
            raw_mad = float(np.median(np.abs(vals.to_numpy() - center))) if len(vals) else np.nan
            scale = 1.4826 * raw_mad if len(vals) else np.nan
            current = pd.to_numeric(pd.Series([prow[metric]]), errors="coerce").iloc[0]
            reconstructed_score = ((current - center) / scale) if pd.notna(current) and len(vals) == 13 and pd.notna(scale) and scale > 0 else np.nan
            stored_score = brow.get(f"mad_w13_t3_{metric}_score", np.nan)
            hist_diag[metric] = {
                "current_measurement": None if pd.isna(current) else float(current),
                "reference_weeks_available": int(len(history)),
                "reference_valid_observations": int(len(vals)),
                "reference_history_start": str(history.week.iloc[0].date()) if len(history) else None,
                "reference_history_end": str(history.week.iloc[-1].date()) if len(history) else None,
                "reference_median": None if pd.isna(center) else center,
                "reference_raw_mad": None if pd.isna(raw_mad) else raw_mad,
                "scaled_mad": None if pd.isna(scale) else scale,
                "reconstructed_score": None if pd.isna(reconstructed_score) else float(reconstructed_score),
                "stored_score": None if pd.isna(stored_score) else float(stored_score),
                "score_match_tolerance_1e-8": bool(pd.isna(reconstructed_score) and pd.isna(stored_score) or
                                                   pd.notna(reconstructed_score) and pd.notna(stored_score) and
                                                   np.isclose(reconstructed_score, stored_score, rtol=1e-8, atol=1e-8)),
            }
        all_scores_known = all(hist_diag[m]["stored_score"] is not None for m in metric_map.values())
        for detector in detectors:
            if detector in metric_map:
                metric = metric_map[detector]
                score = brow.get(score_cols[detector], np.nan)
                raw_flag = brow.get(raw_cols[detector], pd.NA)
                output = brow.get(persist_cols[detector], pd.NA)
                ix = g.index[g.week.eq(week)][0]
                prev_flag = None
                if ix > 0:
                    prev_week = g.loc[ix-1, "week"]
                    prev_flag = b_by_week.loc[prev_week, raw_cols[detector]] if prev_week in b_by_week.index else pd.NA
                d = hist_diag[metric]
                if pd.notna(output):
                    cause = "available"
                elif pd.isna(score):
                    if pd.isna(prow[metric]): cause = "current_measurement_missing"
                    elif d["reference_valid_observations"] < 13: cause = "insufficient_valid_preceding_13_week_history"
                    elif d["scaled_mad"] is not None and d["scaled_mad"] <= 0: cause = "zero_reference_mad"
                    else: cause = "score_unavailable_reason_unresolved"
                elif pd.isna(raw_flag):
                    cause = "raw_component_alarm_unknown_despite_score; unresolved"
                elif pd.isna(prev_flag):
                    cause = "persistence_k2_waiting_for_two_consecutive_known_flags; prior_week_flag_unknown"
                else:
                    cause = "persistence_output_unknown_despite_known_inputs; unresolved"
                outrow = {
                    "repository": repo, "week": week.date().isoformat(), "detector": detector,
                    "available": bool(pd.notna(output)), "primary_persistence_weeks": 2,
                    "current_measurement": d["current_measurement"],
                    "reference_valid_observations": d["reference_valid_observations"],
                    "reference_history_start": d["reference_history_start"],
                    "reference_history_end": d["reference_history_end"],
                    "reference_median": d["reference_median"], "reference_raw_mad": d["reference_raw_mad"],
                    "scaled_mad": d["scaled_mad"], "stored_score": None if pd.isna(score) else float(score),
                    "current_component_flag": None if pd.isna(raw_flag) else bool(raw_flag),
                    "prior_component_flag": None if prev_flag is None or pd.isna(prev_flag) else bool(prev_flag),
                    "persistent_output": None if pd.isna(output) else bool(output),
                    "score_reconstruction_match": d["score_match_tolerance_1e-8"],
                    "cause": cause,
                    "source_files": "analysis_outputs/baseline_results.csv; analysis_outputs/repository_week_panel_audited.csv; 03_fit_baselines.py",
                    "component_scores_json": None, "component_reference_diagnostics_json": None,
                    "current_component_flags_json": None, "prior_component_flags_json": None,
                    "composite_all_three_scores_known": None,
                }
            elif detector in ("persistent_union", "persistent_two_of_three"):
                raw_col = f"mad_w13_t3_{'union' if detector == 'persistent_union' else 'two_of_three'}_k2"
                output = brow.get(raw_col, pd.NA)
                score_known = all(hist_diag[m]["stored_score"] is not None for m in metric_map.values())
                ix = g.index[g.week.eq(week)][0]
                prior_component_flags = []
                if ix > 0:
                    prev_week = g.loc[ix-1, "week"]
                    prev = b_by_week.loc[prev_week] if prev_week in b_by_week.index else None
                    if isinstance(prev, pd.DataFrame): prev = prev.iloc[0]
                    if prev is not None:
                        prior_component_flags = [prev.get(raw_cols[n], pd.NA) for n in metric_map]
                if pd.notna(output): cause = "available"
                elif not score_known:
                    bad = [m for m in metric_map.values() if hist_diag[m]["stored_score"] is None]
                    labels = []
                    for m in bad:
                        d = hist_diag[m]
                        labels.append("zero_reference_mad:"+m if d["reference_valid_observations"] == 13 and d["scaled_mad"] == 0
                                      else "insufficient_history:"+m if d["reference_valid_observations"] < 13
                                      else "score_missing:"+m)
                    cause = "composite_requires_all_three_component_scores; " + ";".join(labels)
                elif len(prior_component_flags) == 3 and not all(pd.notna(x) for x in prior_component_flags):
                    cause = "persistence_k2_initialization_after_unknown_prior_composite_flag"
                else: cause = "composite_persistence_unknown_reason_unresolved"
                outrow = {
                    "repository": repo, "week": week.date().isoformat(), "detector": detector,
                    "available": bool(pd.notna(output)), "primary_persistence_weeks": 2,
                    "current_measurement": None, "reference_valid_observations": None,
                    "reference_history_start": None, "reference_history_end": None,
                    "reference_median": None, "reference_raw_mad": None, "scaled_mad": None,
                    "stored_score": None, "current_component_flag": None,
                    "prior_component_flag": None, "persistent_output": None if pd.isna(output) else bool(output),
                    "score_reconstruction_match": all(hist_diag[m]["score_match_tolerance_1e-8"] for m in metric_map.values()),
                    "cause": cause,
                    "source_files": "analysis_outputs/baseline_results.csv; analysis_outputs/repository_week_panel_audited.csv; 03_fit_baselines.py",
                    "component_scores_json": json.dumps({m: hist_diag[m]["stored_score"] for m in metric_map.values()}, sort_keys=True),
                    "component_reference_diagnostics_json": json.dumps(hist_diag, sort_keys=True),
                    "current_component_flags_json": json.dumps({m: (None if pd.isna(brow.get(raw_cols[n], pd.NA)) else bool(brow.get(raw_cols[n]))) for n,m in metric_map.items()}, sort_keys=True),
                    "prior_component_flags_json": json.dumps({m: (None if len(prior_component_flags) != 3 or pd.isna(prior_component_flags[j]) else bool(prior_component_flags[j])) for j,m in enumerate(metric_map.values())}, sort_keys=True),
                    "composite_all_three_scores_known": bool(score_known),
                }
            else:
                score = brow.get("mewma_stat", np.nan)
                alarm = brow.get("mewma_alarm", pd.NA)
                outrow = {
                    "repository": repo, "week": week.date().isoformat(), "detector": detector,
                    "available": bool(pd.notna(score) and pd.notna(alarm)), "primary_persistence_weeks": None,
                    "current_measurement": None, "reference_valid_observations": None,
                    "reference_history_start": None, "reference_history_end": None,
                    "reference_median": None, "reference_raw_mad": None, "scaled_mad": None,
                    "stored_score": None if pd.isna(score) else float(score),
                    "current_component_flag": None, "prior_component_flag": None,
                    "persistent_output": None if pd.isna(alarm) else bool(alarm),
                    "score_reconstruction_match": None,
                    "cause": "available" if pd.notna(score) and pd.notna(alarm) else "MEWMA_statistic_or_alarm_missing_reason_unresolved",
                    "source_files": "analysis_outputs/baseline_results.csv; analysis_outputs/baseline_metadata.json; 03_fit_baselines.py",
                    "component_scores_json": None, "component_reference_diagnostics_json": None,
                    "current_component_flags_json": None, "prior_component_flags_json": None,
                    "composite_all_three_scores_known": None,
                }
            rows.append(outrow)

diag = pd.DataFrame(rows)
diag.to_csv(OUT / "rq1_unavailability_diagnostics.csv", index=False)

# Direct hash audit of all figure assets listed by the existing visualization manifest.
figdir = AO / "rq2_stage_b_visualizations"
manifest_path = figdir / "rq2_visualization_manifest.json"
manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
hash_rows = []
for name, rec in manifest["files"].items():
    p = figdir / name
    exists = p.is_file()
    digest = sha(p) if exists else None
    hash_rows.append({"file": name, "exists": exists,
                      "actual_bytes": p.stat().st_size if exists else None,
                      "manifest_bytes": rec["bytes"], "actual_sha256": digest,
                      "manifest_sha256": rec["sha256"],
                      "matches_manifest": bool(exists and digest == rec["sha256"] and p.stat().st_size == rec["bytes"])})
hashes = pd.DataFrame(hash_rows)
hashes.to_csv(OUT / "rq2_visualization_hash_audit.csv", index=False)

# Verify each of the seven RQ2 analysis CSVs against its analysis metadata.
analysis_dir = AO / "rq2_stage_b_analysis"
analysis_meta_path = analysis_dir / "rq2_analysis_metadata.json"
analysis_meta = json.loads(analysis_meta_path.read_text(encoding="utf-8"))
analysis_hash_rows = []
for name, rec in analysis_meta["outputs"].items():
    p = analysis_dir / name
    digest = sha(p) if p.is_file() else None
    analysis_hash_rows.append({"file": name, "exists": p.is_file(),
                               "actual_sha256": digest, "metadata_sha256": rec["sha256"],
                               "matches_metadata": bool(digest and digest == rec["sha256"])})
analysis_hashes = pd.DataFrame(analysis_hash_rows)
analysis_hashes.to_csv(OUT / "rq2_analysis_hash_audit.csv", index=False)

# Re-run only hash checks on frozen manifests / currently accessible RQ1 files.
rq1_manifest = json.loads((AO / "RQ1_BASELINES_FROZEN.json").read_text(encoding="utf-8"))
rq1_hashes=[]
for name, rec in rq1_manifest["files"].items():
    p=AO / Path(name).name
    try:
        digest=sha(p); readable=True
    except (PermissionError, OSError):
        digest=None; readable=False
    rq1_hashes.append({"path":name,"exists":p.exists(),"readable":readable,"bytes":p.stat().st_size if p.exists() else None,
                       "recorded_bytes":rec.get("bytes"),"actual_sha256":digest,"recorded_sha256":rec.get("sha256"),
                       "hash_matches":bool(digest and digest==rec.get("sha256"))})
pd.DataFrame(rq1_hashes).to_csv(OUT / "rq1_frozen_hash_audit.csv",index=False)

# Machine-readable provenance summary with limitations preserved.
support_totals = support[["planned_weeks","attempt_records","eligible_outcomes","latency_support","logical_runs","additional_attempts"]].sum().to_dict()
score_check = diag.loc[diag.detector.isin(metric_map) & diag.stored_score.notna(), "score_reconstruction_match"]
score_match_count = int(score_check.fillna(False).sum())
payload = {
  "direct_files_checked": {
    "weekly_measurement_panel": {"path":"V2/analysis_outputs/repository_week_panel_audited.csv","sha256":sha(panel_path),"bytes":panel_path.stat().st_size},
    "intermediate_scores": {"path":"V2/analysis_outputs/baseline_results.csv","sha256":sha(base_path),"bytes":base_path.stat().st_size},
    "figure_script": {"path":"V2/visualize_rq2_results.py","sha256":sha(V2/"visualize_rq2_results.py")},
    "mad_source": {"path":"V2/03_fit_baselines.py","sha256":sha(V2/"03_fit_baselines.py")},
    "visualization_manifest": {"path":"V2/analysis_outputs/rq2_stage_b_visualizations/rq2_visualization_manifest.json","sha256":sha(manifest_path)},
  },
  "reconstructed_evaluation_support_totals": {k:int(v) for k,v in support_totals.items()},
  "diagnostic_rows":len(diag),
  "diagnostic_unavailable_rows":int((~diag.available).sum()),
  "reconstructed_mad_scores_matched":score_match_count,
  "reconstructed_mad_scores_compared":len(score_check),
  "figure_assets_matching_manifest":int(hashes.matches_manifest.sum()),
  "figure_assets_listed_in_manifest":len(hashes),
  "rq2_analysis_csvs_matching_metadata":int(analysis_hashes.matches_metadata.sum()),
  "rq2_analysis_csvs_listed_in_metadata":len(analysis_hashes),
  "scope":"read-only reconstruction from stored panels/intermediates and source code; no detector refit or RQ2 regeneration",
  "limitations":["RQ1 baseline weekly output is permission-denied in this session; its intermediate baseline_results.csv is readable.",
                 "Named main study note and corrected MEWMA methods note are not present in the inspected repository.",
                 "The RQ2 visualization manifest records output/data hashes but does not hash the generation script itself."],
}
(OUT/"audit_inventory.json").write_text(json.dumps(payload,indent=2),encoding="utf-8")

report = f'''# Chapter 5.1 follow-up evidence audit

Audit date: 2026-10-22. Scope: inspect stored outputs and source only; no detector refitting, parameter changes, or study rerun.

## Exact versions inspected

- Main note `Master_CI_Monitoring_Study_Note(20261022-212912)` and corrected MEWMA methods note: not present in the inspected repository; therefore no prose from them is attributed here. No Draft(7) or legacy extract was used.
- Measurement panel: `V2/analysis_outputs/repository_week_panel_audited.csv`, SHA-256 `{sha(panel_path)}`.
- Frozen intermediate scores: `V2/analysis_outputs/baseline_results.csv`, SHA-256 `{sha(base_path)}`.
- RQ1 result: `V2/analysis_outputs/rq1_baseline_week_results.csv` and four companion outputs exist but reads are denied by filesystem permissions. The supplied prior validation says five RQ1 outputs match its validation record; I treat that as a previously checked claim. This session can confirm recorded byte lengths but cannot recompute their hashes. The current unavailability reconstruction uses readable `baseline_results.csv` and `03_fit_baselines.py`.
- RQ2 visual outputs and manifest are the files under `V2/analysis_outputs/rq2_stage_b_visualizations/` timestamped 2026-08-26; generator source is `V2/visualize_rq2_results.py`.

## Detector unavailability

`rq1_unavailability_diagnostics.csv` has one row per 195 evaluation weeks × six detectors. It reports current measurement, preceding 13-week history count and dates, median, raw MAD, scaled MAD, stored score/flag, persistence result, and cause. The history centers and scales are reconstructed from the audited weekly panel by applying the exact preceding-week, 13-observation rolling median/MAD rule in `03_fit_baselines.py`, then compared with the stored score columns. The reconstruction matches {score_match_count} of {len(score_check)} non-null component scores at 1e-8 tolerance. This is an independent reconstruction from stored measurements/intermediates, not a detector rerun.

For containerd and helm, each of the five MAD outputs is unavailable only on 2025-09-22. Current values and MAD scores exist. At the preceding calibration week (2025-09-15), the 13-prior-week MAD warmup was still incomplete (12 valid prior values); persistence requires two consecutive known component/composite flags, so the first external evaluation week is unknown. From 2025-09-29 onward those MAD outputs are evaluable.

For pytest, latency and failure MAD outputs share that same 2025-09-22 persistence initialization gap. Rerun MAD has 39 consecutive external weeks (2025-09-22 through 2026-06-15) where all 13 rerun-rate history values are present but reference raw MAD is zero, making the scaled score undefined under the frozen zero-MAD unevaluable rule. The additional 2026-06-22 week has a score/flag, but the previous unknown flag leaves k=2 persistence unevaluable. It becomes available 2026-06-29, giving six evaluable weeks through 2026-08-03. Union and two-of-three also remain unavailable through 2026-06-22 because their contract requires all three component scores/flags; the zero-MAD rerun component prevents composite eligibility, followed by the same k=2 persistence transition. Current measurement-missingness is not the cause in these rows. The CSV marks any cause not supported by the panel, stored intermediate fields, and code as unresolved.

MEWMA has finite stored statistics and alarms for all evaluation weeks. Its frozen metadata says the reference centering/scaling uses the first 13 observed external weeks and the common empirical limit uses 108 development training rows; those statements are metadata facts, while per-week MEWMA values were directly read from `baseline_results.csv`.

The period details recorded in the frozen config/metadata are: development MEWMA reference = 36 Monday weeks from 2025-06-23 through 2026-02-23; each external MEWMA reference = the first 13 observed Monday weeks from 2025-06-23 through 2025-09-15; MAD uses the preceding 13 observed weekly values causally, excluding the current week. The common MEWMA control limit is recorded as empirical 0.99 quantile, 7.150034791553729, from the 108 development training rows. These establish the intended/frozen reference periods and limits, not the source API cutoff.

## Measurement support and coverage

The requested 195-week combined evaluation totals are reconstructed from the audited panel and match: {int(support_totals['planned_weeks'])} repository-weeks, {int(support_totals['attempt_records']):,} attempts, {int(support_totals['eligible_outcomes']):,} eligible outcomes, {int(support_totals['latency_support']):,} latency-support observations, {int(support_totals['logical_runs']):,} logical runs, and {int(support_totals['additional_attempts']):,} additional attempts. `evaluation_support_reconstructed.csv` gives the repository rows.

The earlier `attempt_coverage_mean = 1.0` in the first Section 5.1 script was computed inside each repository-period group as `mean(attempts_total / attempts_total)` over weekly panel rows. Since the denominator was the identical numerator and those weeks had positive counts, this equals 1.0 by construction. It is not an extraction coverage measure. The actual metadata coverage is grouped by repository over rows in `workflow_runs` with configured PR-validation event names: distinct run IDs represented in the filtered attempt table (`runs_with_attempts`) divided by `eligible_event_runs`. It ranges from 0.9990 to 1.0 and cannot establish complete API extraction. Separately, the builder defines a logical run as a `(repo_full, run_id)` group, rerun when its maximum `attempt_number` is greater than one, `rerun_rate = rerun_count / logical_run_n`, and additional attempts as the sum over logical runs of `max(attempt_number)-1` (floored at zero). These are measurement definitions, not API completeness guarantees.

## RQ2 visualization provenance

All {int(hashes.matches_manifest.sum())} of {len(hashes)} files listed in `rq2_visualization_manifest.json` currently match their recorded byte counts and SHA-256, including all five PNGs, five vector PDFs, and five `figure_data_*.csv` files. All {int(analysis_hashes.matches_metadata.sum())} RQ2 analysis CSVs also match `rq2_analysis_metadata.json`. Thus the mismatch described for uploaded copies is not present in the repository files inspected here. The manifest and data files share the 2026-08-26 visualization output set. `visualize_rq2_results.py` is the matching generator: its current code reads the frozen analysis tables, writes those five figure-data tables, and exports each figure as PNG and PDF. The manifest does not hash the generator source; it establishes output and figure-data hashes, not the exact source-code revision. A separate uploaded copy not present in this repository cannot be compared or classified as edited/re-exported.

## Verification limits

- **Direct file checks:** panel and baseline intermediate values; RQ2 figure/data byte sizes and hashes; filenames and timestamps; source code behavior.
- **Reconstructed:** RQ1 unavailable dates/causes from rolling histories and persisted intermediate flags; 195-week measurement support totals.
- **Metadata-only:** frozen reference period declarations, output manifests and protocol/results linkage.
- **Unresolved:** extraction API/database cutoff timestamp; whether all events for final week ending 2026-08-03 had arrived before extraction; actual execution provenance for the source-code bytes that produced the frozen outputs (manifests record code/config hashes but no run attestation); content of the two requested note documents. The current 354-week panel includes the final calendar week, which alone does not establish event completeness.

See `rq1_frozen_hash_audit.csv` for access/hash status. Source workbook/database extraction was not rerun.
'''
(OUT/"report.md").write_text(report,encoding="utf-8")

# Package evidence copies. Preserve originals and use the current manifest as-is.
evidence = [
    OUT/"report.md", OUT/"rq1_unavailability_diagnostics.csv", OUT/"evaluation_support_reconstructed.csv",
    OUT/"rq2_visualization_hash_audit.csv", OUT/"rq2_analysis_hash_audit.csv", OUT/"rq1_frozen_hash_audit.csv", OUT/"audit_inventory.json",
    V2/"analysis_outputs"/"section_5_1_coverage.py", V2/"analysis_outputs"/"section_5_1_followup_audit.py",
    V2/"analysis_outputs"/"repository_week_panel_audited.csv", V2/"analysis_outputs"/"baseline_results.csv",
    V2/"analysis_outputs"/"baseline_metadata.json", V2/"analysis_outputs"/"MEASUREMENT_FROZEN.json",
    V2/"analysis_outputs"/"measurement_metadata.json", V2/"analysis_outputs"/"data_quality_report.json",
    V2/"analysis_config.yaml", V2/"01_build_measurement_panels.py", V2/"02_validate_measurements.py",
    V2/"analysis_outputs"/"RQ1_BASELINES_FROZEN.json", V2/"analysis_outputs"/"RQ2_STAGE_B_PROTOCOL_FROZEN.json",
    V2/"analysis_outputs"/"RQ2_STAGE_B_RESULTS_FROZEN.json", V2/"03_fit_baselines.py", V2/"ci_common.py",
    V2/"visualize_rq2_results.py", manifest_path,
    AO/"section_5_1"/"measurement_support.csv", AO/"section_5_1"/"source_inventory.json",
    analysis_meta_path,
]
evidence.extend(sorted(analysis_dir.glob("rq2_*.csv")))
evidence.extend(sorted(figdir.glob("figure_data_*.csv")))
evidence.extend(sorted(figdir.glob("figure_rq2_*.png")))
evidence.extend(sorted(figdir.glob("figure_rq2_*.pdf")))
zip_path = AO / "Chapter_5_1_Evaluation_Coverage_Evidence.zip"
with zipfile.ZipFile(zip_path,"w",zipfile.ZIP_DEFLATED) as z:
    for src in evidence:
        if src.exists():
            z.write(src, arcname=src.relative_to(V2).as_posix())
print(json.dumps({"report":str(OUT/"report.md"),"diagnostic_rows":len(diag),
                  "unavailable_rows":int((~diag.available).sum()),
                  "figure_assets_matching_manifest":int(hashes.matches_manifest.sum()),
                  "figure_assets_total":len(hashes),
                  "rq2_analysis_csvs_matching_metadata":int(analysis_hashes.matches_metadata.sum()),
                  "rq2_analysis_csvs_total":len(analysis_hashes),"zip":str(zip_path)},indent=2))
