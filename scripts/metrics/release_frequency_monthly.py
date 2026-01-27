import pandas as pd
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW = PROJECT_ROOT / "data" / "raw"
OUT = PROJECT_ROOT / "data" / "derived"
OUT.mkdir(parents=True, exist_ok=True)

rels = pd.read_csv(RAW / "releases.csv")

rels["published_at_dt"] = pd.to_datetime(rels.get("published_at"), utc=True, errors="coerce")
rels["created_at_dt"] = pd.to_datetime(rels.get("created_at"), utc=True, errors="coerce")
rels["release_time_dt"] = rels["published_at_dt"].fillna(rels["created_at_dt"])

rels = rels.dropna(subset=["release_time_dt"])
rels["month"] = rels["release_time_dt"].dt.to_period("M").dt.start_time

monthly = (rels.groupby(["repo_full","month"], as_index=False)
              .agg(releases=("tag_name","count"))
              .sort_values("month"))

monthly.to_csv(OUT / "release_frequency_monthly.csv", index=False)
print("Saved:", OUT / "release_frequency_monthly.csv")
