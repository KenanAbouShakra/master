import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DERIVED = PROJECT_ROOT / "data" / "derived"
FIG_DIR = PROJECT_ROOT / "figures"
FIG_DIR.mkdir(parents=True, exist_ok=True)

path = DERIVED / "time_to_release_monthly.csv"
if not path.exists():
    raise FileNotFoundError(f"Missing: {path}")

df = pd.read_csv(path)

# --- sanity checks ---
if df.empty:
    raise SystemExit(
        "time_to_release_monthly.csv is empty.\n"
        "This usually means: no releases in releases.csv, repo_full mismatch, "
        "or no 'next release after merge' matches."
    )

required = {"repo_full", "month", "time_to_release_med_days"}
missing = required - set(df.columns)
if missing:
    raise ValueError(f"Missing columns in {path.name}: {sorted(missing)}")

df["month"] = pd.to_datetime(df["month"], utc=True, errors="coerce")
df = df.dropna(subset=["month", "time_to_release_med_days"]).sort_values("month")

if df.empty:
    raise SystemExit("No valid rows after parsing month/time_to_release_med_days.")

# -----------------------------
# FIGURE 1: All repos together
# -----------------------------
fig, ax = plt.subplots(figsize=(11, 5))

for repo, sub in df.groupby("repo_full"):
    sub = sub.sort_values("month")
    ax.plot(sub["month"], sub["time_to_release_med_days"], label=repo, linewidth=2)

ax.set_title("Time-to-Release (Median) Over Time")
ax.set_xlabel("Month")
ax.set_ylabel("Median Time-to-Release (days)")
ax.legend(loc="upper left")
ax.grid(True, alpha=0.3)
plt.tight_layout()

out_all = FIG_DIR / "Time_to_Release_Median_Monthly_AllRepos.png"
plt.savefig(out_all, dpi=300, bbox_inches="tight")
print("Saved figure:", out_all)
plt.close(fig)

# -----------------------------
# FIGURE 2: comparison (one subplot per repo)
# -----------------------------
repos = sorted(df["repo_full"].unique())

if len(repos) < 2:
    print("Skipped Figure 2: repos in time_to_release_monthly.csv")
else:
    repos_2 = repos[:2]

    fig, axes = plt.subplots(2, 1, figsize=(11, 7), sharex=True)
    if not isinstance(axes, (list, tuple)) and getattr(axes, "shape", None) is None:
        axes = [axes]

    for ax, repo in zip(axes, repos_2):
        sub = df[df["repo_full"] == repo].sort_values("month")
        ax.plot(sub["month"], sub["time_to_release_med_days"], linewidth=2)
        ax.set_title(repo)
        ax.set_ylabel("Median days")
        ax.grid(True, alpha=0.3)

    axes[-1].set_xlabel("Month")
    fig.suptitle("Time-to-Release (Median)", y=1.02)
    plt.tight_layout()

    out_2rows = FIG_DIR / "Time_to_Release_Median_Monthly_2Rows.png"
    plt.savefig(out_2rows, dpi=300, bbox_inches="tight")
    print("Saved figure:", out_2rows)
    plt.close(fig)

print("Done.")