import pandas as pd
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW = PROJECT_ROOT / "data" / "raw"
OUT = PROJECT_ROOT / "data" / "derived"
OUT.mkdir(parents=True, exist_ok=True)

src = RAW / "sonar_snapshots.csv"
if not src.exists():
    raise FileNotFoundError("Missing data/raw/sonar_snapshots.csv (run sonar collection first).")

sonar = pd.read_csv(src)

# Standardize types
if "snapshot_date" in sonar.columns:
    sonar["snapshot_date"] = pd.to_datetime(sonar["snapshot_date"], utc=True, errors="coerce")

# Keep key TD measures (if present)
keep = ["repo_full","snapshot_date","commit",
        "code_smells","sqale_debt_ratio","complexity","duplicated_lines_density",
        "sqale_index","sqale_rating"]
keep = [c for c in keep if c in sonar.columns]

out = sonar[keep].copy()
out.to_csv(OUT / "sonar_snapshots_tidy.csv", index=False)
print("Saved:", OUT / "sonar_snapshots_tidy.csv")
