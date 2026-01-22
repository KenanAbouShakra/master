import pandas as pd
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW = PROJECT_ROOT / "data" / "raw"
OUT = PROJECT_ROOT / "data" / "derived"
OUT.mkdir(parents=True, exist_ok=True)

prs_path = RAW / "prs.csv"
rels_path = RAW / "releases.csv"

prs = pd.read_csv(prs_path)
rels = pd.read_csv(rels_path)

# --- Sanity checks ---
required_pr_cols = ["repo_full", "merged_at", "state"]
missing_pr = [c for c in required_pr_cols if c not in prs.columns]
if missing_pr:
    raise ValueError(f"prs.csv missing columns: {missing_pr}")

required_rel_cols = ["repo_full"]
missing_rel = [c for c in required_rel_cols if c not in rels.columns]
if missing_rel:
    raise ValueError(f"releases.csv missing columns: {missing_rel}")

print("PR repos:", prs["repo_full"].nunique(), prs["repo_full"].unique()[:10])
print("Release repos:", rels["repo_full"].nunique(), rels["repo_full"].unique()[:10])

# --- Parse times ---
prs["merged_at_dt"] = pd.to_datetime(prs["merged_at"], utc=True, errors="coerce")
prs["is_merged"] = prs["state"].eq("MERGED")
prs = prs[prs["is_merged"]].dropna(subset=["merged_at_dt"])

rels["published_at_dt"] = pd.to_datetime(rels["published_at"] if "published_at" in rels.columns else None, utc=True, errors="coerce")
rels["created_at_dt"] = pd.to_datetime(rels["created_at"] if "created_at" in rels.columns else None, utc=True, errors="coerce")
rels["release_time_dt"] = rels["published_at_dt"].fillna(rels["created_at_dt"])
rels = rels.dropna(subset=["release_time_dt"]).sort_values("release_time_dt")

print("Merged PR rows:", len(prs))
print("Release rows:", len(rels))

# --- Compute Time-to-Release per repo ---
rows = []
for repo, pr_sub in prs.groupby("repo_full"):
    rel_sub = rels[rels["repo_full"] == repo].sort_values("release_time_dt")
    if rel_sub.empty:
        print(f"[WARN] No releases for {repo} -> skipping")
        continue

    rel_times = rel_sub["release_time_dt"].tolist()

    for t in pr_sub["merged_at_dt"].tolist():
        idx = next((i for i, rt in enumerate(rel_times) if rt >= t), None)
        if idx is not None:
            rows.append({
                "repo_full": repo,
                "merged_at_dt": t,
                "release_time_dt": rel_times[idx],
                "time_to_release_days": (rel_times[idx] - t).total_seconds() / 86400.0
            })

ttr = pd.DataFrame(rows)
print("Matched PR->Release rows:", len(ttr))

# --- Aggregate monthly ---
if ttr.empty:
    out = pd.DataFrame(columns=["repo_full", "month", "time_to_release_med_days", "n"])
else:
    ttr["month"] = ttr["merged_at_dt"].dt.to_period("M").dt.start_time
    out = (
        ttr.groupby(["repo_full", "month"], as_index=False)
           .agg(
               time_to_release_med_days=("time_to_release_days", "median"),
               n=("time_to_release_days", "count")
           )
           .sort_values("month")
    )

out_path = OUT / "time_to_release_monthly.csv"
out.to_csv(out_path, index=False)
print("Saved:", out_path)
