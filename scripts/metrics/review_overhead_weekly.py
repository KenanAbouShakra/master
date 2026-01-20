import pandas as pd
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW = PROJECT_ROOT / "data" / "raw"
OUT = PROJECT_ROOT / "data" / "derived"
OUT.mkdir(parents=True, exist_ok=True)

prs = pd.read_csv(RAW / "prs.csv")

prs["created_at_dt"] = pd.to_datetime(prs["created_at"], utc=True, errors="coerce")
prs["merged_at_dt"] = pd.to_datetime(prs["merged_at"], utc=True, errors="coerce")
prs["closed_at_dt"] = pd.to_datetime(prs["closed_at"], utc=True, errors="coerce")
prs["first_review_dt"] = pd.to_datetime(prs["first_review_at"], utc=True, errors="coerce")

prs["done_at_dt"] = prs["merged_at_dt"].fillna(prs["closed_at_dt"])

prs["review_latency_hours"] = (prs["first_review_dt"] - prs["created_at_dt"]).dt.total_seconds() / 3600.0
prs["review_duration_hours"] = (prs["done_at_dt"] - prs["first_review_dt"]).dt.total_seconds() / 3600.0

prs["week"] = prs["created_at_dt"].dt.to_period("W").dt.start_time

weekly = (
    prs.dropna(subset=["week"])
      .groupby(["repo_full","week"], as_index=False)
      .agg(
          review_count_med=("review_count","median"),
          review_latency_med_h=("review_latency_hours","median"),
          review_duration_med_h=("review_duration_hours","median"),
          prs_total=("pr_number","count"),
      )
      .sort_values("week")
)

weekly.to_csv(OUT / "review_overhead_weekly.csv", index=False)
print("Saved:", OUT / "review_overhead_weekly.csv")
