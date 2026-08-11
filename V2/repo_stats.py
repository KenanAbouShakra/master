from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("repo", help="owner/name, e.g. docker/cli")
    ap.add_argument("--db", default="data/ci_research.sqlite")
    args = ap.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        print(f"Database not found: {db_path}", file=sys.stderr)
        sys.exit(1)

    con = sqlite3.connect(db_path)
    repo = args.repo

    def count(table: str, col: str = "repo_full") -> int:
        try:
            row = con.execute(f"SELECT COUNT(*) FROM {table} WHERE {col}=?", (repo,)).fetchone()
            return row[0] if row else 0
        except sqlite3.OperationalError:
            return 0

    prs       = count("pull_requests")
    reviews   = count("pr_reviews")
    commits   = count("pr_commits")
    runs      = count("workflow_runs")
    attempts  = count("run_attempts")
    jobs      = count("workflow_jobs", "repo_full")
    links     = count("pr_ci_links")
    workflows = count("workflows")

    con.close()

    print(f"| {repo} | {workflows} | {prs} | {reviews} | {commits} | {runs} | {attempts} | {jobs} | {links} |")


if __name__ == "__main__":
    main()
