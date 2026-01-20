import pandas as pd
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW = PROJECT_ROOT / "data" / "raw"
OUT = PROJECT_ROOT / "data" / "derived"
OUT.mkdir(parents=True, exist_ok=True)

runs = pd.read_csv(RAW / "workflow_runs.csv")

runs["run_started_dt"] = pd.to_datetime(runs["run_started_at"], utc=True, errors="coerce")
runs = runs.dropna(subset=["run_started_dt"])

# infer failure if missing
if "is_failure" not in runs.columns:
    runs["is_failure"] = runs["conclusion"].isin(["failure","cancelled","timed_out"])
else:
    runs["is_failure"] = runs["is_failure"].astype(bool)

runs["week"] = runs["run_started_dt"].dt.to_period("W").dt.start_time

weekly = (runs.groupby(["repo_full","week"], as_index=False)
            .agg(
                failure_rate=("is_failure","mean"),
                n=("run_id","count") if "run_id" in runs.columns else ("conclusion","count")
            )
            .sort_values("week"))

# rolling std per repo (8-week window)
parts = []
for repo, sub in weekly.groupby("repo_full"):
    sub = sub.sort_values("week").copy()
    sub["failure_volatility_8w"] = sub["failure_rate"].rolling(8, min_periods=4).std()
    parts.append(sub)

out = pd.concat(parts, ignore_index=True) if parts else weekly
out.to_csv(OUT / "ci_failure_volatility_weekly.csv", index=False)
print("Saved:", OUT / "ci_failure_volatility_weekly.csv")
