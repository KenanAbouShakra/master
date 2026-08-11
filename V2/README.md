# CI Extractor v2

Data collection pipeline for the MSc thesis on CI degradation in GitHub Actions.

## Why it replaces the old collector

The old dataset calculated CI duration from `updated_at - run_started_at`.
That produced impossible values for rerun workflows because `updated_at`
can reflect a much later workflow-run update. v2 therefore stores:

- workflow runs;
- every available `run_attempt`;
- jobs for each attempt;
- job `started_at` and `completed_at`;
- PR details;
- PR commit SHAs;
- reviews;
- explicit PR-to-CI linkage provenance.

It also splits workflow-run date searches to avoid GitHub's documented
1,000-result cap for filtered workflow-run searches.

## Setup

```bash
python -m venv .venv
source .venv/bin/activate       # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp config.example.yaml config.yaml

export GITHUB_TOKEN="YOUR_FINE_GRAINED_TOKEN"
```

For public repositories, the token should have sufficient read access to
Actions and repository metadata. A token is strongly recommended because
bulk anonymous extraction is rate-limited.

## Workflow

### 1. Pre-screen repository candidates

```bash
python prescreen_repositories.py --config config.yaml   helm/helm pytest-dev/pytest pandas-dev/pandas grafana/grafana apache/airflow
```

Quick check only — not the final inclusion criteria.

### 2. Run the full collector

```bash
python collect_all.py --config config.yaml
```

Or run individual stages:

```bash
python extract_repositories.py --config config.yaml
python extract_workflows.py --config config.yaml
python extract_pull_requests.py --config config.yaml
python extract_workflow_runs.py --config config.yaml
python extract_attempts_jobs.py --config config.yaml
python link_pr_ci.py --config config.yaml
python build_attempt_metrics.py --config config.yaml
python validate_dataset.py --config config.yaml
```

### 3. Export normalized tables to CSV if needed

```bash
python export_core_tables.py --config config.yaml
```

SQLite is the primary store. CSV files are exports only.

## Main SQLite tables

- `repositories`
- `workflows`
- `pull_requests`
- `pr_commits`
- `pr_reviews`
- `workflow_runs`
- `run_attempts`
- `workflow_jobs`
- `pr_ci_links`

## Key design decisions

- CI duration uses job timestamps, not `updated_at`.
- Reruns are tracked via `run_attempt`.
- PR-CI linkage is based on commit SHA matching.
- Workflow-run searches are split by date to stay under the 1000-result API limit.
- The cache allows resuming interrupted runs.

## Not yet implemented

The following belong to the analysis layer:

- PR-validation workflow classification
- bot/human review filtering
- weekly aggregation and normalization
- CIDI calculation and episode detection
- regression models and thesis figures
