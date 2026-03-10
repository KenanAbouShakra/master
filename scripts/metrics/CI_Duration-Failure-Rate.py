import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# -----------------------------
# Paths
# -----------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data" / "raw"
FIG_DIR = PROJECT_ROOT / "figures"
FIG_DIR.mkdir(parents=True, exist_ok=True)

# -----------------------------
# Load
# -----------------------------
csv_path = DATA_DIR / "workflow_runs.csv"
if not csv_path.exists():
    raise FileNotFoundError(f"Could not find {csv_path}")

runs = pd.read_csv(csv_path)

# -----------------------------
# Timestamp handling
# -----------------------------
if "run_started_dt" in runs.columns:
    time_col = "run_started_dt"
elif "run_started_at" in runs.columns:
    time_col = "run_started_at"
elif "created_at" in runs.columns:
    time_col = "created_at"
else:
    raise ValueError("No suitable timestamp column found (run_started_dt/run_started_at/created_at).")

runs[time_col] = pd.to_datetime(runs[time_col], utc=True, errors="coerce")

# -----------------------------
# Ensure ci_duration_min exists
# -----------------------------
if "ci_duration_min" not in runs.columns:
    runs["ci_duration_min"] = pd.NA

need = runs["ci_duration_min"].isna()
if need.any() and ("run_started_at" in runs.columns) and ("updated_at" in runs.columns):
    rs = pd.to_datetime(runs.loc[need, "run_started_at"], utc=True, errors="coerce")
    up = pd.to_datetime(runs.loc[need, "updated_at"], utc=True, errors="coerce")
    runs.loc[need, "ci_duration_min"] = (up - rs).dt.total_seconds() / 60.0

runs["ci_duration_min"] = pd.to_numeric(runs["ci_duration_min"], errors="coerce")

# -----------------------------
# Failure flag
# -----------------------------
if "is_failure" not in runs.columns:
    runs["is_failure"] = runs["conclusion"].isin(["failure", "cancelled", "timed_out"])
else:
    runs["is_failure"] = runs["is_failure"].astype(bool)

# -----------------------------
# Repo id col
# -----------------------------
repo_id_col = "repo_full" if "repo_full" in runs.columns else "repo"

# -----------------------------
# Sanity print
# -----------------------------
print("Time span:")
print("  Min:", runs[time_col].min())
print("  Max:", runs[time_col].max())
print("  Days:", (runs[time_col].max() - runs[time_col].min()).days)
print("  Rows:", len(runs))
print("  Repos:", runs[repo_id_col].dropna().unique())

# -----------------------------
# Outlier handling
# -----------------------------
MAX_CI_MINUTES = 360  # 6 hours

before = len(runs)
runs = runs.dropna(subset=[time_col, "ci_duration_min", repo_id_col])
runs = runs[runs["ci_duration_min"].between(0, MAX_CI_MINUTES)]
after = len(runs)
print(f"Outlier filter: removed {before - after} rows using MAX_CI_MINUTES={MAX_CI_MINUTES}")

# -----------------------------
# Week bucket
# -----------------------------
runs["week"] = (
    runs[time_col]
    .dt.tz_convert(None)
    .dt.to_period("W-MON")
    .dt.start_time
)

# -----------------------------
# Aggregate per repo/week
# -----------------------------
agg = (
    runs.groupby([repo_id_col, "week"], as_index=False)
        .agg(
            ci_duration_med=("ci_duration_min", "median"),
            failure_rate=("is_failure", "mean"),
            n=("ci_duration_min", "count"),
        )
        .sort_values([repo_id_col, "week"])
)

# 4-week smoothing within each repo
agg["ci_duration_4w"] = (
    agg.groupby(repo_id_col)["ci_duration_med"]
       .transform(lambda s: s.rolling(window=4, min_periods=1).median())
)

agg["failure_rate_4w"] = (
    agg.groupby(repo_id_col)["failure_rate"]
       .transform(lambda s: s.rolling(window=4, min_periods=1).mean())
)

# -----------------------------
# Repo order
# -----------------------------
repo_order = [
    "docker/cli",
    "prometheus/prometheus",
    "tektoncd/pipeline",
]

available_repos = [r for r in repo_order if r in agg[repo_id_col].unique()]
if not available_repos:
    available_repos = list(agg[repo_id_col].unique())

# -----------------------------
# FIGURE 1: CI Duration faceted
# -----------------------------
fig, axes = plt.subplots(len(available_repos), 1, figsize=(14, 4 * len(available_repos)), sharex=True)

if len(available_repos) == 1:
    axes = [axes]

for ax, repo in zip(axes, available_repos):
    sub = agg[agg[repo_id_col] == repo].sort_values("week")
    ax.plot(sub["week"], sub["ci_duration_med"], alpha=0.30, label="Weekly median")
    ax.plot(sub["week"], sub["ci_duration_4w"], linewidth=2.5, label="4-week median")
    ax.set_title(repo)
    ax.set_ylabel("Minutes")
    ax.grid(True, alpha=0.3)
    ax.legend()

axes[-1].set_xlabel("Week")
fig.suptitle("CI Duration Over Time (Median, Weekly and 4-week Smoothed)", fontsize=18, y=0.995)
fig.tight_layout()

out1 = FIG_DIR / "Figure_CI_Duration_Faceted_Weekly_4wMedian.png"
fig.savefig(out1, dpi=300, bbox_inches="tight")
print("Saved:", out1)
plt.close(fig)

# -----------------------------
# FIGURE 2: CI Failure Rate faceted
# -----------------------------
fig, axes = plt.subplots(len(available_repos), 1, figsize=(14, 4 * len(available_repos)), sharex=True)

if len(available_repos) == 1:
    axes = [axes]

for ax, repo in zip(axes, available_repos):
    sub = agg[agg[repo_id_col] == repo].sort_values("week")
    ax.plot(sub["week"], sub["failure_rate"], alpha=0.30, label="Weekly rate")
    ax.plot(sub["week"], sub["failure_rate_4w"], linewidth=2.5, label="4-week mean")
    ax.set_title(repo)
    ax.set_ylabel("Failure rate")
    ax.grid(True, alpha=0.3)
    ax.legend()

axes[-1].set_xlabel("Week")
fig.suptitle("CI Failure Rate Over Time (Weekly and 4-week Smoothed)", fontsize=18, y=0.995)
fig.tight_layout()

out2 = FIG_DIR / "Figure_CI_Failure_Rate_Faceted_Weekly_4wMean.png"
fig.savefig(out2, dpi=300, bbox_inches="tight")
print("Saved:", out2)
plt.close(fig)

print("Done.")