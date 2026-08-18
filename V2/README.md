# Detecting and Evaluating Persistent Adverse CI Operational States

This repository contains the data-processing and analysis pipeline for an MSc research project investigating persistent adverse operational states in GitHub Actions continuous integration environments.

The study applies a repository-relative, multi-indicator longitudinal design. It combines attempt-level CI measurements, workflow-aware aggregation, statistical process-monitoring baselines, latent-state modelling, temporal validation, external-repository evaluation, and downstream pull-request analysis.

## Research Objectives

The study addresses two research questions:

1. How effectively can repository-relative, multi-indicator temporal models identify and explain persistent adverse CI operational states in GitHub Actions pull-request validation workflows, compared with robust rule-based and statistical process-monitoring baselines?

2. To what extent are validated adverse-state probabilities associated with concurrent or subsequent changes in pull-request cycle time, qualified human-review latency, and merge throughput?

The observational design supports association and detection claims. It does not establish a causal effect of CI conditions on pull-request outcomes.

## Study Population

The research sample contains six open-source GitHub repositories.

### Development repositories

These repositories are used for measurement development, preprocessing, model fitting, and temporal holdout evaluation:

* `docker/cli`
* `prometheus/prometheus`
* `tektoncd/pipeline`

### External evaluation repositories

These repositories are reserved for cross-repository evaluation and are not used for model selection:

* `pytest-dev/pytest`
* `helm/helm`
* `containerd/containerd`

The configured CI observation period is 1 June 2025 through 7 August 2026. Complete Monday-anchored analysis weeks cover 23 June 2025 through 3 August 2026.

## Collected Data

The SQLite database is the authoritative data source. CSV files are treated as derived exports.

The database contains:

* repository metadata;
* workflow definitions;
* pull requests;
* pull-request commits;
* pull-request reviews;
* workflow runs;
* individual run attempts;
* workflow jobs;
* PR–CI linkage records; and
* extraction metadata.

The completed research dataset contains approximately:

| Data source          |      Rows |
| -------------------- | --------: |
| Workflows            |       418 |
| Pull requests        |    14,283 |
| Pull-request reviews |    36,369 |
| Pull-request commits |    35,977 |
| Workflow runs        |   160,338 |
| Run attempts         |   136,196 |
| Workflow jobs        | 1,082,259 |
| PR–CI links          |   137,443 |

The database also contains metadata for prescreened repositories that are not part of the final research sample. All analytical scripts enforce the frozen six-repository population.

## CI Population and Eligibility

The primary CI population consists of GitHub Actions executions associated with pull-request validation events:

* `pull_request`
* `pull_request_target`
* `merge_group`

Push-only, scheduled, deployment, dispatch, and unrelated automation runs are excluded from the primary analytical population. Excluded observations are retained in provenance and quality reports where applicable.

Completed attempts with the following conclusions are treated as eligible outcomes:

### Successful outcome

* `success`

### Adverse outcome

* `failure`
* `timed_out`
* `startup_failure`

The following conclusions are excluded from the primary failure denominator or reported separately:

* `cancelled`
* `skipped`
* `neutral`
* `stale`
* `action_required`

## Measurement Model

The analysis uses three primary CI-health dimensions.

### Feedback latency

Feedback latency is measured for each eligible attempt from the attempt creation timestamp to the latest valid job completion timestamp.

This definition retains queueing and execution time while avoiding `workflow_run.updated_at`, which may change after execution has completed.

The pipeline reports:

* median weekly feedback latency;
* 90th-percentile feedback latency;
* queue latency;
* execution span;
* valid latency observations; and
* latency exclusions.

Because workflow composition can change over time, the analysis retains both raw repository-level latency and workflow-relative latency evidence. Workflow references are estimated without using temporal holdout observations.

### Reliability burden

For repository (r), workflow (w), and week (t):

[
\text{FailureRate}*{rwt} =
\frac{\text{FailureCount}*{rwt}}
{\text{EligibleOutcomeCount}_{rwt}}
]

Every analytical table retains both the numerator and denominator. Weeks with small denominators are flagged rather than interpreted as equally precise observations.

### Re-execution burden

A logical workflow run is classified as rerun when its maximum observed attempt number is greater than one:

[
\text{RerunRate}*{rwt} =
\frac{\text{RerunLogicalRuns}*{rwt}}
{\text{EligibleLogicalRuns}_{rwt}}
]

The pipeline also reports:

* additional-attempt count;
* average attempts per logical run;
* failed-then-passed recovery count; and
* failed-then-passed recovery rate among reruns.

Rerun behaviour is not labelled as test flakiness because repeated execution can have several causes.

## Pull-Request Outcomes

The downstream analysis includes:

* pull-request cycle time;
* time to the first qualified non-bot human review;
* weekly merge throughput;
* pull-request volume;
* additions and deletions;
* changed-file count; and
* commit count.

Bot reviews are excluded from qualified human-review latency. Pull-request outcomes are analysed concurrently and with a one-week lag.

PR–CI linkage is many-to-many. Link counts must therefore not be interpreted as counts of unique pull requests.

## Units of Analysis

The pipeline constructs the following analytical units:

| Unit            | Key                                 | Purpose                                          |
| --------------- | ----------------------------------- | ------------------------------------------------ |
| Attempt         | repository × run × attempt          | Canonical execution measurement                  |
| Logical run     | repository × run                    | Rerun and recovery reconstruction                |
| Workflow-week   | repository × workflow × Monday week | Workflow composition and measurement diagnostics |
| Repository-week | repository × Monday week            | Detector fitting, validation, and RQ2 alignment  |
| Pull request    | repository × PR number              | Downstream outcomes and covariates               |

## Data Splits

The development repositories use a leakage-resistant temporal design:

| Split            | Period                        |
| ---------------- | ----------------------------- |
| Training         | 23 June 2025–23 February 2026 |
| Purge interval   | 2 March 2026–23 March 2026    |
| Temporal holdout | 30 March 2026–3 August 2026   |

The three external repositories form a separate external-evaluation split.

Preprocessing, detector parameters, thresholds, and model structure must be frozen before temporal holdout and external evaluation. Purge observations must not be relabelled as training or evaluation data.

## Analytical Model Ladder

The analysis compares increasingly expressive approaches.

### Causal rolling MAD

The transparent rule-based baseline uses only observations preceding week (t).

The primary reference window is 26 weeks. A 13-week window is retained as sensitivity analysis.

The analysis reports:

* thresholds of 2.5, 3.0, and 3.5 scaled MAD units;
* persistence lengths of one through four weeks;
* individual metric alarms;
* union alarms;
* two-of-three alarms;
* alarm prevalence; and
* episode duration.

MAD alarms are baseline signals, not observed ground truth.

### MEWMA

Multivariate exponentially weighted moving-average monitoring is used as a statistical process-monitoring comparator. Its parameters and empirical control limit are estimated from development training observations only.

### Hidden Markov model

The candidate main model is a parsimonious pooled two-state hidden Markov model.

Each repository is treated as a separate temporal sequence. Repository histories are never concatenated into one artificial sequence.

The implementation reports:

* state-dependent emissions;
* transition probabilities;
* log likelihood;
* convergence;
* state occupancy;
* stability across random initialisations;
* filtered state probabilities;
* retrospective state evidence; and
* Viterbi segmentation.

The states retain neutral labels until their emissions, persistence, measurement support, and validation evidence justify an adverse interpretation.

### Change-point robustness analysis

Change-point analysis evaluates whether the main findings depend on the latent-state formulation. A detected change point indicates a distributional level change and does not independently establish CI degradation.

## Validation Strategy

### Synthetic injection

Known adverse changes are injected into real baseline sequences. Scenarios vary:

* effect magnitude;
* affected indicators;
* episode duration;
* run volume;
* missingness;
* outliers; and
* workflow-composition disturbance.

Evaluation measures include:

* precision;
* recall;
* false-alarm rate;
* detection delay; and
* episode-level detection performance.

### Temporal holdout

The detector is applied once to the frozen temporal holdout after preprocessing and modelling decisions have been fixed.

### External evaluation

The frozen detector is evaluated on the three external repositories without using them to select metrics, thresholds, state count, or detector hyperparameters.

### Real-case triangulation

Candidate episodes should be examined using workflow changes, commits, pull requests, issues, releases where available, missingness, run volume, and workflow composition.

Human inspection provides triangulation rather than perfect ground truth.

## Quality Gate

Model fitting is prohibited until the measurement quality gate reports `PASS`.

The gate evaluates:

* the six-repository population;
* repository-week uniqueness;
* workflow-week uniqueness;
* Monday-anchored weeks;
* attempt coverage;
* duration validity;
* numerator–denominator reconstruction;
* missingness;
* weekly support;
* workflow identities;
* workflow concentration;
* temporal coverage; and
* split consistency.

The gate preserves exclusions and does not silently convert missing measurements into normal observations.

## Installation

Create and activate a Python virtual environment.

### Windows PowerShell

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements_analysis.txt
```

### macOS or Linux

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements_analysis.txt
```

## Configuration

Edit the paths in `analysis_config.yaml`:

```yaml
paths:
  sqlite: data/ci_research.sqlite
  releases_csv: data/exports/releases.csv
  output: analysis_outputs
```

The SQLite path must reference the completed database. The analysis reads the existing database and does not make new GitHub API requests.

Methodological settings should not be changed after holdout access without a dated decision-log entry.

## Execution

### 1. Test the analysis environment

```bash
python smoke_test.py
```

Expected result:

```text
SMOKE TEST PASSED
```

The smoke test uses generated synthetic data and does not modify the research database.

### 2. Construct measurement panels

```bash
python 01_build_measurement_panels.py --config analysis_config.yaml
```

Principal outputs:

* `attempt_measurements.csv.gz`
* `repository_week_panel.csv`
* `workflow_week_panel.csv`
* `measurement_exclusions.csv.gz`
* `measurement_metadata.json`

### 3. Run the measurement quality gate

```bash
python 02_validate_measurements.py --config analysis_config.yaml
```

Principal outputs:

* `repository_week_panel_audited.csv`
* `data_quality_report.json`

Do not continue if the report status is `FAIL`.

### 4. Fit statistical baselines

```bash
python 03_fit_baselines.py --config analysis_config.yaml
```

Principal outputs:

* `baseline_results.csv`
* `mad_prevalence_table.csv`
* `mewma_metadata.json`
* `pelt_robustness_change_points.csv`

### 5. Fit the latent-state model

```bash
python 04_fit_hmm.py --config analysis_config.yaml
```

Principal outputs:

* `hmm_results.csv`
* `hmm_model.json`

### 6. Validate RQ1

```bash
python 05_validate_rq1.py --config analysis_config.yaml
```

Principal outputs:

* `synthetic_injection_results.csv`
* `synthetic_injection_summary.csv`
* `candidate_episodes.csv`
* `holdout_external_summary.csv`
* `rq1_validation_status.json`

### 7. Freeze RQ1

RQ1 must be reviewed and documented before it is frozen:

```bash
python 06_freeze_rq1.py --config analysis_config.yaml --confirm
```

This creates `RQ1_FROZEN.json`, containing hashes of the frozen configuration and RQ1 outputs.

No detector setting may subsequently be changed to maximise an RQ2 association.

### 8. Estimate RQ2 associations

```bash
python 07_fit_rq2.py --config analysis_config.yaml
```

Principal outputs:

* `rq2_effect_estimates.csv`
* `rq2_metadata.json`

RQ2 estimates represent associations, not causal effects.

## Reproducibility

The pipeline supports reproducibility through:

* a frozen repository population;
* explicit temporal splits;
* deterministic random seeds;
* input and output SHA-256 hashes;
* preserved numerators and denominators;
* exclusion records;
* configuration-controlled analytical decisions;
* an explicit RQ1 freeze artifact; and
* machine-readable quality and model metadata.

Numerical sample descriptions should be generated from the frozen analytical artifacts rather than manually copied into thesis text.

## Interpretation Boundaries

An unusual observation is not automatically a degradation episode.

A candidate state supports the study-specific interpretation of CI degradation only when the evidence demonstrates:

* an adverse direction;
* temporal persistence;
* practical magnitude;
* sufficient measurement support;
* robustness across reasonable analytical choices;
* consideration of alternative explanations; and
* temporal or external generalisation.

If these conditions are not met, the result should be described more conservatively as an adverse deviation, instability episode, workflow-composition change, or ambiguous operational state.

## Known Limitations

The study is observational and repository selection is purposive. The number of repositories is small, CI workflows are heterogeneous, workflow identities evolve, and some weeks have unequal measurement support.

Release information is used only when a consistent six-repository release source is available. Missing release data must remain missing and must not be interpreted as evidence that no releases occurred.

Reruns are not equivalent to flaky tests, filtered state probabilities are not causal variables, and detector agreement is not independent ground truth.
