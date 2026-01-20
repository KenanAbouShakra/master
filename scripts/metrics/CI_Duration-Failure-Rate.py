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
# Ensure ci_duration_min exists (compute if missing)
# -----------------------------
if "ci_duration_min" not in runs.columns:
    runs["ci_duration_min"] = pd.NA

need = runs["ci_duration_min"].isna()
if need.any() and ("run_started_at" in runs.columns) and ("updated_at" in runs.columns):
    rs = pd.to_datetime(runs.loc[need, "run_started_at"], utc=True, errors="coerce")
    up = pd.to_datetime(runs.loc[need, "updated_at"], utc=True, errors="coerce")
    runs.loc[need, "ci_duration_min"] = (up - rs).dt.total_seconds() / 60.0

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
print("  Repos:", runs[repo_id_col].unique())

# -----------------------------
# Outlier handling (THE FIX)
# -----------------------------
# Hard cap (recommended): CI runs longer than 6 hours are almost always metadata/outliers in OSS CI.
MAX_CI_MINUTES = 360  # 6 hours

before = len(runs)
runs = runs.dropna(subset=["ci_duration_min", time_col])
runs = runs[runs["ci_duration_min"].between(0, MAX_CI_MINUTES)]
after = len(runs)
print(f"Outlier filter: removed {before - after} rows using MAX_CI_MINUTES={MAX_CI_MINUTES}")

# If you want a percentile-based cap instead, comment the hard cap above and use:
# p99 = runs["ci_duration_min"].quantile(0.99)
# runs = runs[runs["ci_duration_min"] <= p99]
# print(f"Outlier filter: clipped at p99={p99:.2f} minutes")

# -----------------------------
# Week bucket (Monday start) - pandas-safe
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
        .sort_values("week")
)

# -----------------------------
# FIGURE A: CI duration
# -----------------------------
fig, ax = plt.subplots(figsize=(11, 5))
for repo, sub in agg.groupby(repo_id_col):
    ax.plot(sub["week"], sub["ci_duration_med"], label=repo, linewidth=2)

ax.set_ylabel("Median CI Duration (minutes)")
ax.set_xlabel("Week")
ax.set_title("CI Duration (Median) Over Time")
ax.legend()
ax.grid(True, alpha=0.3)
plt.tight_layout()

out1 = FIG_DIR / "Figure_CI_Duration_Median_Over_Time.png"
plt.savefig(out1, dpi=300, bbox_inches="tight")
print("Saved:", out1)
plt.close(fig)

# -----------------------------
# FIGURE B: CI failure rate
# -----------------------------
fig, ax = plt.subplots(figsize=(11, 5))
for repo, sub in agg.groupby(repo_id_col):
    ax.plot(sub["week"], sub["failure_rate"], label=repo, linewidth=2)

ax.set_ylabel("Failure Rate")
ax.set_xlabel("Week")
ax.set_title("CI Failure Rate Over Time")
ax.legend()
ax.grid(True, alpha=0.3)
plt.tight_layout()

out2 = FIG_DIR / "Figure_CI_Failure_Rate_Over_Time.png"
plt.savefig(out2, dpi=300, bbox_inches="tight")
print("Saved:", out2)
plt.close(fig)

print("Done.")
