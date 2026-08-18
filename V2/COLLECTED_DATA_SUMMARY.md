# Summary of Collected CI Research Data

## Overview

- Configured study period: 2025-06-01 to 2026-08-07.
- Research sample: 6 repositories, divided into 3 pilot and 3 evaluation repositories.
- Data quality status: **PASS** with 0 reported errors.
- Canonical modelling runs: 115,368; repository-weeks: 354; workflow-weeks: 3,363.
- The database contains metadata for 12 repositories. 6 are prescreening candidates outside the research sample.

## Data Volume

| Data type | Rows |
|---|---|
| workflows | 418 |
| pull_requests | 14,283 |
| pr_reviews | 36,369 |
| pr_commits | 35,977 |
| workflow_runs | 160,338 |
| run_attempts | 136,196 |
| workflow_jobs | 1,082,259 |
| pr_ci_links | 137,443 |

## Per Repository

| Repository | Group | Workflows | PRs | Reviews | Commits | Runs | Attempts | Jobs | PR-CI links |
|---|---|---|---|---|---|---|---|---|---|
| docker/cli | pilot | 16 | 1,938 | 4,584 | 3,394 | 20,048 | 21,616 | 249,266 | 20,467 |
| prometheus/prometheus | pilot | 28 | 4,506 | 11,518 | 16,714 | 44,812 | 45,615 | 290,636 | 50,717 |
| tektoncd/pipeline | pilot | 23 | 2,442 | 4,328 | 3,714 | 43,148 | 30,406 | 94,436 | 34,546 |
| pytest-dev/pytest | evaluation | 14 | 1,899 | 3,851 | 5,258 | 4,235 | 2,898 | 77,435 | 3,033 |
| helm/helm | evaluation | 14 | 1,883 | 5,620 | 4,162 | 15,403 | 8,277 | 7,025 | 10,458 |
| containerd/containerd | evaluation | 27 | 1,615 | 6,468 | 2,735 | 32,692 | 27,384 | 363,461 | 18,222 |

## Pull Requests

| Repository | PRs | Merged | Closed | Draft | Bot | Median additions | Median deletions | Median files | PRs with CI link |
|---|---|---|---|---|---|---|---|---|---|
| docker/cli | 1,938 | 86.7% | 93.2% | 3.8% | 3.1% | 14.0 | 7.0 | 3.0 | 44.4% |
| prometheus/prometheus | 4,506 | 67.0% | 91.7% | 6.7% | 24.1% | 20.0 | 6.0 | 2.0 | 46.1% |
| tektoncd/pipeline | 2,442 | 75.6% | 95.7% | 0.7% | 52.1% | 19.5 | 5.0 | 3.0 | 55.8% |
| pytest-dev/pytest | 1,899 | 76.9% | 93.8% | 3.2% | 39.3% | 17.0 | 3.0 | 2.0 | 46.7% |
| helm/helm | 1,883 | 61.5% | 90.5% | 2.4% | 29.4% | 12.0 | 4.0 | 2.0 | 53.2% |
| containerd/containerd | 1,615 | 67.9% | 85.6% | 5.8% | 16.8% | 23.0 | 3.0 | 2.0 | 95.0% |

## CI Attempts and Jobs

| Repository | Runs with attempts | Runs with retry | Retry rate | Maximum attempts | Valid job durations | Mean job minutes |
|---|---|---|---|---|---|---|
| docker/cli | 20,046 | 1,489 | 7.4% | 6 | 248,217 | 1.7 |
| prometheus/prometheus | 44,812 | 663 | 1.5% | 10 | 274,346 | 5.8 |
| tektoncd/pipeline | 27,322 | 2,541 | 9.3% | 23 | 94,049 | 13.2 |
| pytest-dev/pytest | 2,847 | 46 | 1.6% | 6 | 77,370 | 3.6 |
| helm/helm | 7,424 | 847 | 11.4% | 3 | 7,021 | 3.5 |
| containerd/containerd | 25,901 | 1,198 | 4.6% | 8 | 357,991 | 10.3 |

## Temporal Coverage

| Data type | Earliest timestamp | Latest timestamp |
|---|---|---|
| Pull requests | 2024-01-01T23:10:32Z | 2026-08-07T23:26:55Z |
| Workflow runs | 2025-06-03T00:35:56Z | 2026-08-07T23:26:59Z |
| Reviews | 2024-01-02T19:28:08Z | 2026-08-10T20:18:58Z |
| Commits | 2017-06-01T21:15:13Z | 2026-08-09T17:43:20Z |

## Key Distributions

### Workflow-run conclusions

| Conclusion | Count | Share |
|---|---|---|
| success | 126,367 | 78.8% |
| failure | 19,833 | 12.4% |
| skipped | 8,953 | 5.6% |
| cancelled | 3,239 | 2.0% |
| action_required | 1,001 | 0.6% |
| startup_failure | 943 | 0.6% |
| (missing) | 2 | 0.0% |

### Workflow-run events

| Event | Count | Share |
|---|---|---|
| pull_request | 113,696 | 70.9% |
| push | 19,651 | 12.3% |
| issue_comment | 9,293 | 5.8% |
| dynamic | 8,165 | 5.1% |
| schedule | 5,529 | 3.4% |
| merge_group | 1,405 | 0.9% |
| repository_dispatch | 1,379 | 0.9% |
| create | 366 | 0.2% |
| workflow_run | 309 | 0.2% |
| pull_request_target | 267 | 0.2% |
| pull_request_review_comment | 152 | 0.1% |
| workflow_dispatch | 64 | 0.0% |
| branch_protection_rule | 62 | 0.0% |

### Job conclusions

| Conclusion | Count | Share |
|---|---|---|
| success | 900,507 | 83.2% |
| skipped | 82,783 | 7.6% |
| failure | 61,462 | 5.7% |
| cancelled | 37,502 | 3.5% |
| (missing) | 5 | 0.0% |

### PR-CI link methods

| Method | Count | Share |
|---|---|---|
| api_pull_requests_array | 73,785 | 53.7% |
| commit_sha | 63,658 | 46.3% |

## Modelling Panel and Data Quality

- Analysis weeks: 2025-06-23 to 2026-08-03.
- Split: external=177, train=108, holdout=57, purge=12.
- Excluded or duplicate run rows: 44,970.
- Invalid duration rows: 5,513.
- Repository-weeks without runs: 0.
- Missing training targets: 21 (allowed limit: 36).
- Attempt coverage is 100% in four repositories, 99.97% for tektoncd/pipeline, and 99.90% for containerd/containerd.
- The valid duration fraction per repository ranges from 92.36% to 98.94%.

## Interpretation Notes

- `workflow_runs` is the raw table. The modelling panel uses 115,368 canonical rows after filtering, so the raw total must not be treated as the model's effective sample size.
- `pr_ci_links` is a many-to-many table. The number of links is not the same as the number of PRs with CI.
- `workflow_jobs` dominates the row count because one workflow run can contain many jobs and attempts.
- The MAD alarm in the modelling panel is a baseline signal for the next four weeks, not observed ground truth.
- Repository comparisons should be normalized per week, PR, or run because activity levels and workflow structures vary substantially.

## Metadata Repositories Outside the Research Sample

apache/airflow, cli/cli, grafana/grafana, home-assistant/core, open-telemetry/opentelemetry-collector, pandas-dev/pandas
