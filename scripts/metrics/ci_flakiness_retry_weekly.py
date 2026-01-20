import pandas as pd
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW = PROJECT_ROOT / "data" / "raw"
OUT = PROJECT_ROOT / "data" / "derived"
OUT.mkdir(parents=True, exist_ok=True)

runs = pd.read_csv(RAW / "workflow_runs.csv")

runs["run_started_dt"] = pd.to_datetime(runs["run_started_at"], utc=True, errors="coerce")
runs = runs.dropna(subset=["run_started_dt", "head_sha", "repo_full"])

# Use workflow name column
name_col = "workflow_name" if "workflow_name" in runs.columns else ("name" if "name" in runs.columns else None)
if not name_col:
    raise ValueError("No workflow name column found (expected workflow_name or name).")

# Week bucket (Monday start)
runs["week"] = (
    runs["run_started_dt"]
      .dt.tz_convert(None)
      .dt.to_period("W-MON")
      .dt.start_time
)

# Group by (sha, workflow, event) to approximate retries
per_key = (
    runs.groupby(["repo_full", "week", "head_sha", name_col, "event"])
        .size()
        .reset_index(name="runs_per_key")
)

per_key["has_retry"] = per_key["runs_per_key"] > 1

weekly = (
    per_key.groupby(["repo_full", "week"], as_index=False)
           .agg(
               share_with_retry=("has_retry", "mean"),
               avg_runs_per_key=("runs_per_key", "mean"),
               p95_runs_per_key=("runs_per_key", lambda s: s.quantile(0.95)),
               n_keys=("runs_per_key", "count"),
           )
           .sort_values("week")
)

out_path = OUT / "ci_flakiness_true_retry_weekly.csv"
weekly.to_csv(out_path, index=False)
print("Saved:", out_path)

# Quick sanity summary
print("\nSanity check summary (overall):")
for repo, sub in per_key.groupby("repo_full"):
    print(repo, sub["runs_per_key"].describe(percentiles=[0.5, 0.9, 0.95, 0.99]).to_dict())
