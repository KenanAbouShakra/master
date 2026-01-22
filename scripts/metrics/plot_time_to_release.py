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

# --- plot ---
fig, ax = plt.subplots(figsize=(11, 5))

for repo, sub in df.groupby("repo_full"):
    ax.plot(sub["month"], sub["time_to_release_med_days"], label=repo, linewidth=2)

ax.set_title("Time-to-Release (Median) Over Time")
ax.set_xlabel("Month")
ax.set_ylabel("Median Time-to-Release (days)")
ax.legend()
ax.grid(True, alpha=0.3)
plt.tight_layout()

out = FIG_DIR / "Time_to_Release_Median_Monthly.png"
plt.savefig(out, dpi=300, bbox_inches="tight")
print("Saved figure:", out)

plt.close(fig)  # auto-stop (no blocking window)
