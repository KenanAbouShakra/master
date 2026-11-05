# RQ2 Chapter 5 figure verification report

Audit/export date: 2026-10-23. No detector was rerun; no scenario, calibration, parameter, or stored result was altered.

## Exact source versions inspected

- Requested `Master_CI_Monitoring_Study_Note(20261022-212912)` and `MEWMA_Checked_Methods_Note_EN`: not present in the repository or supplied attachment folder (the only supplied attachment was the request text). No legacy PR/release/CD/flakiness files or Draft(7) outputs were used.
- Current source result set: `V2/analysis_outputs/rq2_stage_b/` with `RQ2_STAGE_B_RESULTS_FROZEN.json` and `RQ2_STAGE_B_PROTOCOL_FROZEN.json`; all result-manifest files were verified by byte size and SHA-256. Current summaries are `V2/analysis_outputs/rq2_stage_b_analysis/` and all seven analysis CSVs match `rq2_analysis_metadata.json`.
- The available visualization manifest is `V2/analysis_outputs/rq2_stage_b_visualizations/rq2_visualization_manifest.json` (SHA-256 `de245b522efad8cf1d8633c3eaeae76305a4815dc95fa8040c1d9ec2b7d65171`). The specifically cited `rq2_visualization_manifest(1).json` was not supplied. In this checkout all listed old PNG/PDF/data files match the current repository manifest; the external PNG copy described in the request cannot be compared because it is absent.
- Plot source is this package's `build_chapter5_rq2_figures.py`; the matching current analytical sources are frozen Stage-B scenario/detector/paired-week outputs. The methods notes are unavailable, so the requested positive-state, no-covariance MEWMA wording is retained as specified in the request and is consistent with the implementation code's positive-part squared statistic.

## Numerical checks

The scenario registry has 25,200 primary observed-volume/no-additional-missingness scenarios: 8,400 per repository. It yields 6,300 per duration and 8,400 per magnitude. Medium observed and controlled-missingness profiles each contain 8,400 scenarios per detector. Raw detector outcomes contain 25,200 rows per detector. Every primary strict-detection outcome is present; if one were missing, export would stop rather than treat it as a non-detection. Reconstructed overall, repository, duration, and magnitude numerators/denominators match the frozen analysis tables exactly.

Reference burden was independently rebuilt from the paired-week file. For each scenario it is reference alarm weeks divided by pair-evaluable weeks, then averaged over scenarios where that ratio is defined. Reconstructed scenario burdens match the stored scenario-level result and primary operating-profile aggregation. Pair-week unavailability was rebuilt from `pair_evaluable` over the 46 evaluation weeks for each scenario-detector pair and matches the stored scenario field. The `pooled_reference_alarm_week_ratio` is included as an audit diagnostic; the plotted reference burden follows the frozen mean-of-scenario-ratios aggregation.

Controlled-missingness contrasts are computed within 84 repository × affected-metric-combination × duration cells per detector at medium magnitude, then averaged equally across cells. Both conditions contain 100 repetitions in each cell (8,400 total each); injection locations were independently seeded.

## Presentation and uncertainty

Five revised PDFs are vector exports; PNG companions are exported at 320 dpi. Titles, axes, repository labels, detector colours, numerical duration spacing, medium-only missingness contrast and operating-marker legend follow the request. MAD failure and MAD rerun retain their exact data coordinates; separate marker shapes and offset labels make them distinguishable. Marker area uses `90 + 500 x mean unavailability proportion` pt^2, so zero unavailability remains visible at 90 pt^2. No uncertainty bands or error bars are drawn. The source analysis has nominal scenario-level intervals, but they do not quantify variation across project populations; repeated scenarios are nested in three repositories. The new figures therefore show point estimates only.

## Limits

The user-described uploaded PNG files and the named `(1)` manifest were not present, so their claimed mismatch cannot be specifically reconciled. Current repository exports match the current manifest. The new manifest records hashes for the actual inputs and all new outputs and script; it does not overwrite or relabel the previous manifest. The two requested methodological notes are also unavailable. No inference is made about applicability beyond the three external repositories.
