\
from __future__ import annotations

import argparse
import csv
from pathlib import Path

from db import connect
from utils import load_config


TABLES = [
    "repositories",
    "workflows",
    "pull_requests",
    "pr_commits",
    "pr_reviews",
    "workflow_runs",
    "run_attempts",
    "workflow_jobs",
    "pr_ci_links",
]


def export_table(con, table, out_dir):
    cur = con.execute(f"SELECT * FROM {table}")
    cols = [d[0] for d in cur.description]
    path = out_dir / f"{table}.csv"
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for row in cur:
            w.writerow([row[c] for c in cols])
    print(path)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.yaml")
    args = ap.parse_args()
    cfg = load_config(args.config)
    con = connect(cfg["storage"]["sqlite_path"])
    out = Path(cfg["storage"]["export_dir"])
    out.mkdir(parents=True, exist_ok=True)
    for t in TABLES:
        export_table(con, t, out)


if __name__ == "__main__":
    main()
