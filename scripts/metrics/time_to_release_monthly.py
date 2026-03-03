import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW = PROJECT_ROOT / "data" / "raw"
OUT = PROJECT_ROOT / "data" / "derived"
FIG = PROJECT_ROOT / "figures"
OUT.mkdir(parents=True, exist_ok=True)
FIG.mkdir(parents=True, exist_ok=True)

prs_path = RAW / "prs.csv"
rels_path = RAW / "releases.csv"

prs = pd.read_csv(prs_path)
rels = pd.read_csv(rels_path)

# --- Configure which repos you want to show in plots
WANTED_REPOS = ["docker/cli", "prometheus/prometheus", "tektoncd/pipeline"]

# --- Sanity checks ---
required_pr_cols = ["repo_full", "merged_at", "state"]
missing_pr = [c for c in required_pr_cols if c not in prs.columns]
if missing_pr:
    raise ValueError(f"prs.csv missing columns: {missing_pr}")

required_rel_cols = ["repo_full"]
missing_rel = [c for c in required_rel_cols if c not in rels.columns]
if missing_rel:
    raise ValueError(f"releases.csv missing columns: {missing_rel}")

prs["repo_full"] = prs["repo_full"].astype(str).str.strip()
rels["repo_full"] = rels["repo_full"].astype(str).str.strip()

print("PR repos:", prs["repo_full"].nunique(), prs["repo_full"].unique()[:10])
print("Release repos:", rels["repo_full"].nunique(), rels["repo_full"].unique()[:10])

# --- Parse times ---
prs["merged_at_dt"] = pd.to_datetime(prs["merged_at"], utc=True, errors="coerce")
prs["is_merged"] = prs["state"].astype(str).str.upper().eq("MERGED")
prs = prs[prs["is_merged"]].dropna(subset=["merged_at_dt"])

# Releases: prefer published_at, else created_at (only if column exists)
rels["published_at_dt"] = pd.to_datetime(rels["published_at"], utc=True, errors="coerce") if "published_at" in rels.columns else pd.NaT
rels["created_at_dt"] = pd.to_datetime(rels["created_at"], utc=True, errors="coerce") if "created_at" in rels.columns else pd.NaT
rels["release_time_dt"] = rels["published_at_dt"].fillna(rels["created_at_dt"])
rels = rels.dropna(subset=["release_time_dt"]).sort_values("release_time_dt")

print("Merged PR rows:", len(prs))
print("Release rows:", len(rels))

# --- Compute Time-to-Release per repo ---
rows = []
skipped_repos = []

for repo, pr_sub in prs.groupby("repo_full"):
    rel_sub = rels[rels["repo_full"] == repo].sort_values("release_time_dt")
    if rel_sub.empty:
        print(f"[WARN] No releases for {repo} -> skipping")
        skipped_repos.append(repo)
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
           .sort_values(["repo_full", "month"])
    )

out_path = OUT / "time_to_release_monthly.csv"
out.to_csv(out_path, index=False)
print("Saved:", out_path)

# ----------------------------
# Plot helpers: create placeholders for missing repos
# ----------------------------
def add_placeholders(monthly_df: pd.DataFrame, wanted_repos):
    if monthly_df.empty:
        return monthly_df

    min_m = monthly_df["month"].min()
    max_m = monthly_df["month"].max()
    months = pd.date_range(min_m, max_m, freq="MS")

    present = set(monthly_df["repo_full"].unique())
    missing = [r for r in wanted_repos if r not in present]

    if missing:
        print("[INFO] Missing repos in Time-to-Release output:", missing)
        placeholders = []
        for repo in missing:
            for m in months:
                placeholders.append({
                    "repo_full": repo,
                    "month": m,
                    "time_to_release_med_days": np.nan,
                    "n": 0
                })
        monthly_df = pd.concat([monthly_df, pd.DataFrame(placeholders)], ignore_index=True)

    return monthly_df

out_plot = add_placeholders(out.copy(), WANTED_REPOS)

# ----------------------------
# Figure 1: Split (one subplot per repo)
# ----------------------------
repos_to_plot = [r for r in WANTED_REPOS if r in set(out_plot["repo_full"].unique())]
n = len(repos_to_plot)
cols = 1
rows_n = n

fig, axes = plt.subplots(rows_n, cols, figsize=(14, 4 * rows_n), sharex=True)
if n == 1:
    axes = [axes]

for ax, repo in zip(axes, repos_to_plot):
    sub = out_plot[out_plot["repo_full"] == repo].sort_values("month")
    ax.plot(sub["month"], sub["time_to_release_med_days"], marker="o")
    ax.set_title(repo)
    ax.set_ylabel("Median days")
    ax.grid(True, alpha=0.3)

    # Annotation if missing (docker/cli likely)
    if sub["time_to_release_med_days"].isna().all():
        ax.text(0.5, 0.5, "No release events in dataset\n(Time-to-Release not computable)",
                transform=ax.transAxes, ha="center", va="center")

axes[-1].set_xlabel("Month")
fig.suptitle("Time-to-Release (Median)", y=0.995)
fig.tight_layout()

split_path = FIG / "time_to_release_monthly_split.png"
fig.savefig(split_path, dpi=200)
plt.close(fig)
print("Saved:", split_path)

# ----------------------------
# Figure 2: Combined (all repos on one plot)
# ----------------------------
fig2 = plt.figure(figsize=(14, 5))
for repo in repos_to_plot:
    sub = out_plot[out_plot["repo_full"] == repo].sort_values("month")
    plt.plot(sub["month"], sub["time_to_release_med_days"], marker="o", label=repo)

plt.title("Median Time-to-Release Over Time (Monthly)")
plt.ylabel("Median Time-to-Release (days)")
plt.xlabel("Month")
plt.grid(True, alpha=0.3)
plt.legend()
fig2.tight_layout()

combined_path = FIG / "time_to_release_monthly_all.png"
fig2.savefig(combined_path, dpi=200)
plt.close(fig2)
print("Saved:", combined_path)