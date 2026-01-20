import pandas as pd
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW = PROJECT_ROOT / "data" / "raw"
OUT = PROJECT_ROOT / "data" / "derived"
OUT.mkdir(parents=True, exist_ok=True)

prs = pd.read_csv(RAW / "prs.csv")
rels = pd.read_csv(RAW / "releases.csv")

prs["merged_at_dt"] = pd.to_datetime(prs["merged_at"], utc=True, errors="coerce")
prs["is_merged"] = prs["state"].eq("MERGED")
prs = prs[prs["is_merged"]].dropna(subset=["merged_at_dt"])

rels["published_at_dt"] = pd.to_datetime(rels.get("published_at"), utc=True, errors="coerce")
rels["created_at_dt"] = pd.to_datetime(rels.get("created_at"), utc=True, errors="coerce")
rels["release_time_dt"] = rels["published_at_dt"].fillna(rels["created_at_dt"])
rels = rels.dropna(subset=["release_time_dt"])

# compute per repo
rows = []
for repo, pr_sub in prs.groupby("repo_full"):
    rel_sub = rels[rels["repo_full"] == repo].sort_values("release_time_dt")
    if rel_sub.empty:
        continue
    rel_times = rel_sub["release_time_dt"].tolist()

    for t in pr_sub["merged_at_dt"].tolist():
        idx = next((i for i, rt in enumerate(rel_times) if rt >= t), None)
        if idx is not None:
            rows.append({"repo_full": repo, "merged_at_dt": t, "time_to_release_days": (rel_times[idx] - t).total_seconds()/86400.0})

ttr = pd.DataFrame(rows)
if ttr.empty:
    out = pd.DataFrame(columns=["repo_full","month","time_to_release_med_days","n"])
else:
    ttr["month"] = ttr["merged_at_dt"].dt.to_period("M").dt.start_time
    out = (ttr.groupby(["repo_full","month"], as_index=False)
              .agg(time_to_release_med_days=("time_to_release_days","median"),
                   n=("time_to_release_days","count"))
              .sort_values("month"))

out.to_csv(OUT / "time_to_release_monthly.csv", index=False)
print("Saved:", OUT / "time_to_release_monthly.csv")
