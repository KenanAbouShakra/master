import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# -----------------------------
# Paths
# -----------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DERIVED = PROJECT_ROOT / "data" / "derived"
FIG_DIR = PROJECT_ROOT / "figures"
FIG_DIR.mkdir(parents=True, exist_ok=True)

in_path = DERIVED / "release_frequency_monthly.csv"
if not in_path.exists():
    raise FileNotFoundError(f"Missing {in_path}. Run release_frequency_monthly.py first.")

df = pd.read_csv(in_path)

# -----------------------------
# Parse month
# -----------------------------
df["month"] = pd.to_datetime(df["month"], errors="coerce")
df = df.dropna(subset=["month", "repo_full"]).sort_values(["repo_full", "month"])

# -----------------------------
# Repo order
# -----------------------------
repo_order = [
    "docker/cli",
    "prometheus/prometheus",
    "tektoncd/pipeline",
]

available_repos = [r for r in repo_order if r in df["repo_full"].unique()]
if not available_repos:
    available_repos = list(df["repo_full"].unique())

# -----------------------------
# Faceted bar chart
# -----------------------------
fig, axes = plt.subplots(len(available_repos), 1, figsize=(14, 4 * len(available_repos)), sharex=True)

if len(available_repos) == 1:
    axes = [axes]

for ax, repo in zip(axes, available_repos):
    sub = df[df["repo_full"] == repo].sort_values("month")

    ax.bar(sub["month"], sub["releases"], width=20)  # width in days
    ax.set_title(repo)
    ax.set_ylabel("Releases")
    ax.grid(True, axis="y", alpha=0.3)

axes[-1].set_xlabel("Month")
fig.suptitle("Release Frequency Over Time (Monthly)", fontsize=18, y=0.995)
fig.tight_layout()

out_fig = FIG_DIR / "Figure_Release_Frequency_Faceted_Monthly.png"
fig.savefig(out_fig, dpi=300, bbox_inches="tight")
plt.close(fig)

print("Saved:", out_fig)