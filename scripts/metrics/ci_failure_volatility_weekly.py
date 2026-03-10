import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# -----------------------------
# Paths
# -----------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW = PROJECT_ROOT / "data" / "raw"
OUT = PROJECT_ROOT / "data" / "derived"
FIG_DIR = PROJECT_ROOT / "figures"

OUT.mkdir(parents=True, exist_ok=True)
FIG_DIR.mkdir(parents=True, exist_ok=True)

# -----------------------------
# Load
# -----------------------------
runs = pd.read_csv(RAW / "workflow_runs.csv")

# -----------------------------
# Parse time
# -----------------------------
runs["run_started_dt"] = pd.to_datetime(runs["run_started_at"], utc=True, errors="coerce")
runs = runs.dropna(subset=["run_started_dt", "repo_full"])

# -----------------------------
# Failure flag
# -----------------------------
if "is_failure" not in runs.columns:
    runs["is_failure"] = runs["conclusion"].isin(["failure", "cancelled", "timed_out"])
else:
    runs["is_failure"] = runs["is_failure"].astype(bool)

# -----------------------------
# Week bucket
# -----------------------------
runs["week"] = (
    runs["run_started_dt"]
    .dt.tz_convert(None)
    .dt.to_period("W-MON")
    .dt.start_time
)

# -----------------------------
# Aggregate failure rate by repo/week
# -----------------------------
weekly = (
    runs.groupby(["repo_full", "week"], as_index=False)
        .agg(
            failure_rate=("is_failure", "mean"),
            n=("run_id", "count") if "run_id" in runs.columns else ("conclusion", "count"),
        )
        .sort_values(["repo_full", "week"])
)

# Optional: filter very small weeks
MIN_RUNS = 10
weekly = weekly[weekly["n"] >= MIN_RUNS].copy()

# -----------------------------
# Rolling 8-week std (volatility)
# -----------------------------
parts = []
for repo, sub in weekly.groupby("repo_full"):
    sub = sub.sort_values("week").copy()
    sub["failure_volatility_8w"] = sub["failure_rate"].rolling(window=8, min_periods=4).std()
    parts.append(sub)

out = pd.concat(parts, ignore_index=True) if parts else weekly

# -----------------------------
# Save CSV
# -----------------------------
csv_path = OUT / "ci_failure_volatility_weekly.csv"
out.to_csv(csv_path, index=False)
print("Saved:", csv_path)

# -----------------------------
# Repo order
# -----------------------------
repo_order = [
    "docker/cli",
    "prometheus/prometheus",
    "tektoncd/pipeline",
]

available_repos = [r for r in repo_order if r in out["repo_full"].unique()]
if not available_repos:
    available_repos = list(out["repo_full"].unique())

# ============================================================
# FIGURE A: Faceted volatility
# ============================================================
fig, axes = plt.subplots(len(available_repos), 1, figsize=(14, 4 * len(available_repos)), sharex=True)

if len(available_repos) == 1:
    axes = [axes]

for ax, repo in zip(axes, available_repos):
    sub = out[(out["repo_full"] == repo)].dropna(subset=["failure_volatility_8w"]).sort_values("week")

    if len(sub) == 0:
        ax.text(0.5, 0.5, "Not enough data", transform=ax.transAxes, ha="center", va="center")
        ax.set_title(repo)
        ax.set_ylabel("Std dev")
        ax.grid(True, alpha=0.3)
        continue

    ax.plot(sub["week"], sub["failure_volatility_8w"], linewidth=2.5, label="8-week std")
    ax.set_title(repo)
    ax.set_ylabel("Std dev")
    ax.grid(True, alpha=0.3)
    ax.legend()

axes[-1].set_xlabel("Week")
fig.suptitle("CI Failure Volatility Over Time (Rolling 8-week Std Dev)", fontsize=18, y=0.995)
fig.tight_layout()

fig_path_faceted = FIG_DIR / "Figure_CI_Failure_Volatility_Faceted_8w.png"
fig.savefig(fig_path_faceted, dpi=300, bbox_inches="tight")
print("Saved figure:", fig_path_faceted)
plt.close(fig)

# ============================================================
# FIGURE B: Combined comparison
# ============================================================
fig, ax = plt.subplots(figsize=(12, 5))

for repo in available_repos:
    sub = out[(out["repo_full"] == repo)].dropna(subset=["failure_volatility_8w"]).sort_values("week")
    if len(sub) == 0:
        continue
    ax.plot(sub["week"], sub["failure_volatility_8w"], linewidth=2.5, label=repo)

ax.set_title("CI Failure Volatility Over Time (Rolling 8-week Std Dev)")
ax.set_xlabel("Week")
ax.set_ylabel("Failure Volatility (Std Dev)")
ax.legend()
ax.grid(True, alpha=0.3)
plt.tight_layout()

fig_path_combined = FIG_DIR / "Figure_CI_Failure_Volatility_Combined_8w.png"
fig.savefig(fig_path_combined, dpi=300, bbox_inches="tight")
print("Saved figure:", fig_path_combined)
plt.close(fig)

print("Done.")