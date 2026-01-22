import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# -----------------------------
# Paths
# -----------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA = PROJECT_ROOT / "data" / "derived"
FIG = PROJECT_ROOT / "figures"
FIG.mkdir(exist_ok=True)

# -----------------------------
# Load data
# -----------------------------
df = pd.read_csv(DATA / "review_overhead_weekly.csv", parse_dates=["week"])

# -----------------------------
# Plot: Review Duration
# -----------------------------
fig, ax = plt.subplots(figsize=(11, 5))

for repo, sub in df.groupby("repo_full"):
    ax.plot(
        sub["week"],
        sub["review_duration_med_h"],
        label=repo,
        linewidth=2
    )

ax.set_ylabel("Median Review Duration (hours)")
ax.set_xlabel("Week")
ax.set_title("Review Duration Over Time")
ax.legend()
ax.grid(True, alpha=0.3)

plt.tight_layout()
plt.savefig(FIG / "Figure_Review_Duration_Weekly.png", dpi=300)
plt.show()
