import re
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
# CD workflow name patterns (proxy)
# -----------------------------
CD_WORKFLOW_NAME_PATTERNS = [r"deploy", r"release", r"publish", r"delivery", r"\bcd\b"]
pat = re.compile("|".join(CD_WORKFLOW_NAME_PATTERNS), re.IGNORECASE)

# -----------------------------
# Load
# -----------------------------
runs_path = RAW / "workflow_runs.csv"
if not runs_path.exists():
    raise FileNotFoundError(f"Missing {runs_path}")

runs = pd.read_csv(runs_path)

# -----------------------------
# Timestamps
# -----------------------------
runs["run_started_dt"] = pd.to_datetime(runs["run_started_at"], utc=True, errors="coerce")
runs = runs.dropna(subset=["run_started_dt"])

# -----------------------------
# Failure flag
# -----------------------------
if "is_failure" not in runs.columns:
    runs["is_failure"] = runs["conclusion"].isin(["failure", "cancelled", "timed_out"])
else:
    runs["is_failure"] = runs["is_failure"].astype(bool)

# -----------------------------
# Workflow name column
# -----------------------------
name_col = "workflow_name" if "workflow_name" in runs.columns else ("name" if "name" in runs.columns else None)
if not name_col:
    raise ValueError("No workflow name column found (expected workflow_name or name).")

# -----------------------------
# Filter CD workflows
# -----------------------------
runs["is_cd_workflow"] = runs[name_col].fillna("").apply(lambda s: bool(pat.search(str(s))))
cd = runs[runs["is_cd_workflow"]].copy()

if cd.empty:
    print("No workflows matched the CD patterns. Consider updating CD_WORKFLOW_NAME_PATTERNS.")
    # still write empty outputs
    out_csv = OUT / "cd_workflow_success_weekly.csv"
    pd.DataFrame(columns=["repo_full","week","cd_runs","cd_failure_rate","cd_duration_med_min","cd_success_rate"]).to_csv(out_csv, index=False)
    print("Saved:", out_csv)
    raise SystemExit(0)

# -----------------------------
# Week bucket (ISO week start = Monday)
# -----------------------------
iso = cd["run_started_dt"].dt.isocalendar()
cd["week"] = pd.to_datetime(
    iso["year"].astype(str) + "-W" + iso["week"].astype(str) + "-1",
    format="%G-W%V-%u",
    utc=True
)

# -----------------------------
# Aggregate weekly
# -----------------------------
weekly = (
    cd.groupby(["repo_full", "week"], as_index=False)
      .agg(
          cd_runs=("run_id", "count") if "run_id" in cd.columns else (name_col, "count"),
          cd_failure_rate=("is_failure", "mean"),
          cd_duration_med_min=("ci_duration_min", "median") if "ci_duration_min" in cd.columns else (name_col, "count"),
      )
      .sort_values("week")
)

weekly["cd_success_rate"] = 1.0 - weekly["cd_failure_rate"]

# -----------------------------
# Save CSV
# -----------------------------
out_csv = OUT / "cd_workflow_success_weekly.csv"
weekly.to_csv(out_csv, index=False)
print("Saved:", out_csv)

# -----------------------------
# Plot + Save figure
# -----------------------------
fig, ax = plt.subplots(figsize=(11, 5))

for repo, sub in weekly.groupby("repo_full"):
    ax.plot(sub["week"], sub["cd_success_rate"], label=repo, linewidth=2)

ax.set_ylabel("CD Workflow Success Rate")
ax.set_xlabel("Week")
ax.set_title("CD Workflow Success Rate Over Time (Proxy via workflow name matching)")
ax.set_ylim(0, 1)
ax.legend()
ax.grid(True, alpha=0.3)
plt.tight_layout()

out_fig = FIG_DIR / "Figure_CD_Workflow_Success_Rate_Weekly.png"
plt.savefig(out_fig, dpi=300, bbox_inches="tight")
print("Saved figure:", out_fig)

plt.show()
