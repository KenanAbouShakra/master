# Chapter 5.1 follow-up evidence audit

Audit date: 2026-10-22. Scope: inspect stored outputs and source only; no detector refitting, parameter changes, or study rerun.

## Exact versions inspected

- Main note `Master_CI_Monitoring_Study_Note(20261022-212912)` and corrected MEWMA methods note: not present in the inspected repository; therefore no prose from them is attributed here. No Draft(7) or legacy extract was used.
- Measurement panel: `V2/analysis_outputs/repository_week_panel_audited.csv`, SHA-256 `73e06ac4ad67c5c8ff01fbc298544f41b3fe4a2ad5f1c2d07d3a1fd254e78061`.
- Frozen intermediate scores: `V2/analysis_outputs/baseline_results.csv`, SHA-256 `cf79206ee7f3fe7d601c43b4719df0ca9ac7f7715053e1830a1733fcbe4f131f`.
- RQ1 result: `V2/analysis_outputs/rq1_baseline_week_results.csv` and four companion outputs exist but reads are denied by filesystem permissions. The supplied prior validation says five RQ1 outputs match its validation record; I treat that as a previously checked claim. This session can confirm recorded byte lengths but cannot recompute their hashes. The current unavailability reconstruction uses readable `baseline_results.csv` and `03_fit_baselines.py`.
- RQ2 visual outputs and manifest are the files under `V2/analysis_outputs/rq2_stage_b_visualizations/` timestamped 2026-08-26; generator source is `V2/visualize_rq2_results.py`.

## Detector unavailability

`rq1_unavailability_diagnostics.csv` has one row per 195 evaluation weeks × six detectors. It reports current measurement, preceding 13-week history count and dates, median, raw MAD, scaled MAD, stored score/flag, persistence result, and cause. The history centers and scales are reconstructed from the audited weekly panel by applying the exact preceding-week, 13-observation rolling median/MAD rule in `03_fit_baselines.py`, then compared with the stored score columns. The reconstruction matches 546 of 546 non-null component scores at 1e-8 tolerance. This is an independent reconstruction from stored measurements/intermediates, not a detector rerun.

For containerd and helm, each of the five MAD outputs is unavailable only on 2025-09-22. Current values and MAD scores exist. At the preceding calibration week (2025-09-15), the 13-prior-week MAD warmup was still incomplete (12 valid prior values); persistence requires two consecutive known component/composite flags, so the first external evaluation week is unknown. From 2025-09-29 onward those MAD outputs are evaluable.

For pytest, latency and failure MAD outputs share that same 2025-09-22 persistence initialization gap. Rerun MAD has 39 consecutive external weeks (2025-09-22 through 2026-06-15) where all 13 rerun-rate history values are present but reference raw MAD is zero, making the scaled score undefined under the frozen zero-MAD unevaluable rule. The additional 2026-06-22 week has a score/flag, but the previous unknown flag leaves k=2 persistence unevaluable. It becomes available 2026-06-29, giving six evaluable weeks through 2026-08-03. Union and two-of-three also remain unavailable through 2026-06-22 because their contract requires all three component scores/flags; the zero-MAD rerun component prevents composite eligibility, followed by the same k=2 persistence transition. Current measurement-missingness is not the cause in these rows. The CSV marks any cause not supported by the panel, stored intermediate fields, and code as unresolved.

MEWMA has finite stored statistics and alarms for all evaluation weeks. Its frozen metadata says the reference centering/scaling uses the first 13 observed external weeks and the common empirical limit uses 108 development training rows; those statements are metadata facts, while per-week MEWMA values were directly read from `baseline_results.csv`.

The period details recorded in the frozen config/metadata are: development MEWMA reference = 36 Monday weeks from 2025-06-23 through 2026-02-23; each external MEWMA reference = the first 13 observed Monday weeks from 2025-06-23 through 2025-09-15; MAD uses the preceding 13 observed weekly values causally, excluding the current week. The common MEWMA control limit is recorded as empirical 0.99 quantile, 7.150034791553729, from the 108 development training rows. These establish the intended/frozen reference periods and limits, not the source API cutoff.

## Measurement support and coverage

The requested 195-week combined evaluation totals are reconstructed from the audited panel and match: 195 repository-weeks, 65,957 attempts, 58,634 eligible outcomes, 57,905 latency-support observations, 61,769 logical runs, and 4,188 additional attempts. `evaluation_support_reconstructed.csv` gives the repository rows.

The earlier `attempt_coverage_mean = 1.0` in the first Section 5.1 script was computed inside each repository-period group as `mean(attempts_total / attempts_total)` over weekly panel rows. Since the denominator was the identical numerator and those weeks had positive counts, this equals 1.0 by construction. It is not an extraction coverage measure. The actual metadata coverage is grouped by repository over rows in `workflow_runs` with configured PR-validation event names: distinct run IDs represented in the filtered attempt table (`runs_with_attempts`) divided by `eligible_event_runs`. It ranges from 0.9990 to 1.0 and cannot establish complete API extraction. Separately, the builder defines a logical run as a `(repo_full, run_id)` group, rerun when its maximum `attempt_number` is greater than one, `rerun_rate = rerun_count / logical_run_n`, and additional attempts as the sum over logical runs of `max(attempt_number)-1` (floored at zero). These are measurement definitions, not API completeness guarantees.

## RQ2 visualization provenance

All 15 of 15 files listed in `rq2_visualization_manifest.json` currently match their recorded byte counts and SHA-256, including all five PNGs, five vector PDFs, and five `figure_data_*.csv` files. All 7 RQ2 analysis CSVs also match `rq2_analysis_metadata.json`. Thus the mismatch described for uploaded copies is not present in the repository files inspected here. The manifest and data files share the 2026-08-26 visualization output set. `visualize_rq2_results.py` is the matching generator: its current code reads the frozen analysis tables, writes those five figure-data tables, and exports each figure as PNG and PDF. The manifest does not hash the generator source; it establishes output and figure-data hashes, not the exact source-code revision. A separate uploaded copy not present in this repository cannot be compared or classified as edited/re-exported.

## Verification limits

- **Direct file checks:** panel and baseline intermediate values; RQ2 figure/data byte sizes and hashes; filenames and timestamps; source code behavior.
- **Reconstructed:** RQ1 unavailable dates/causes from rolling histories and persisted intermediate flags; 195-week measurement support totals.
- **Metadata-only:** frozen reference period declarations, output manifests and protocol/results linkage.
- **Unresolved:** extraction API/database cutoff timestamp; whether all events for final week ending 2026-08-03 had arrived before extraction; actual execution provenance for the source-code bytes that produced the frozen outputs (manifests record code/config hashes but no run attestation); content of the two requested note documents. The current 354-week panel includes the final calendar week, which alone does not establish event completeness.

See `rq1_frozen_hash_audit.csv` for access/hash status. Source workbook/database extraction was not rerun.
