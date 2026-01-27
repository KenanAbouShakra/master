import pandas as pd
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW = PROJECT_ROOT / "data" / "raw"
OUT = PROJECT_ROOT / "data" / "derived"
OUT.mkdir(parents=True, exist_ok=True)

prs = pd.read_csv(RAW / "prs.csv")

prs["merged_at_dt"] = pd.to_datetime(prs["merged_at"], utc=True, errors="coerce")
prs["is_merged"] = prs["state"].eq("MERGED")
prs = prs[prs["is_merged"]].dropna(subset=["merged_at_dt"])

prs["week"] = prs["merged_at_dt"].dt.to_period("W").dt.start_time

weekly = (prs.groupby(["repo_full","week"], as_index=False)
            .agg(merge_count=("pr_number","count"))
            .sort_values("week"))

weekly.to_csv(OUT / "merge_frequency_weekly.csv", index=False)
print("Saved:", OUT / "merge_frequency_weekly.csv")
