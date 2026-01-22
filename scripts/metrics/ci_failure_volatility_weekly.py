import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW = PROJECT_ROOT / "data" / "raw"
OUT = PROJECT_ROOT / "data" / "derived"
FIG_DIR = PROJECT_ROOT / "figures"

OUT.mkdir(parents=True, exist_ok=True)
FIG_DIR.mkdir(parents=True, exist_ok=True)

runs = pd.read_csv(RAW / "workflow_runs.csv")

runs["run_started_dt"] = pd.to_datetime(runs["run_started_at"], utc=True, errors="coerce")
runs = runs.dropna(subset=["run_started_dt"])

if "is_failure" not in runs.columns:
    runs["is_failure"] = runs["conclusion"].isin(["failure", "cancelled", "timed_out"])
else:
    runs["is_failure"] = runs["is_failure"].astype(bool)

runs["week"] = runs["run_started_dt"].dt.to_period("W").dt.start_time

weekly = (
    runs.groupby(["repo_full", "week"], as_index=False)
        .agg(
            failure_rate=("is_failure", "mean"),
            n=("run_id", "count") if "run_id" in runs.columns else ("conclusion", "count")
        )
        .sort_values("week")
)

parts = []
for repo, sub in weekly.groupby("repo_full"):
    sub = sub.sort_values("week").copy()
    sub["failure_volatility_8w"] = sub["failure_rate"].rolling(8, min_periods=4).std()
    parts.append(sub)

out = pd.concat(parts, ignore_index=True) if parts else weekly

csv_path = OUT / "ci_failure_volatility_weekly.csv"
out.to_csv(csv_path, index=False)
print("Saved:", csv_path)

fig, ax = plt.subplots(figsize=(11, 5))

for repo, sub in out.groupby("repo_full"):
    sub = sub.dropna(subset=["failure_volatility_8w"])
    if len(sub) == 0:
        continue
    ax.plot(sub["week"], sub["failure_volatility_8w"], label=repo, linewidth=2)

ax.set_title("CI Failure Volatility (Rolling 8-week Std of Failure Rate)")
ax.set_xlabel("Week")
ax.set_ylabel("Failure Volatility (Std Dev)")
ax.legend()
ax.grid(True, alpha=0.3)
plt.tight_layout()

fig_path = FIG_DIR / "CI_Failure_Volatility_8w.png"
plt.savefig(fig_path, dpi=300, bbox_inches="tight")
print("Saved figure:", fig_path)

plt.close(fig) 