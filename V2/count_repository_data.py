from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

TABLES = {
    "workflows": "workflows",
    "prs": "pull_requests",
    "reviews": "pr_reviews",
    "commits": "pr_commits",
    "runs": "workflow_runs",
    "attempts": "run_attempts",
    "jobs": "workflow_jobs",
    "links": "pr_ci_links",
}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Count collected records for one repository."
    )
    parser.add_argument("repo", help="Repository in owner/name format")
    parser.add_argument(
        "--db",
        default="data/ci_research.sqlite",
        help="Path to the SQLite database",
    )
    args = parser.parse_args()

    db_path = Path(args.db)

    if not db_path.is_file():
        print(f"ERROR: Database not found: {db_path}", file=sys.stderr)
        sys.exit(1)

    try:
        with sqlite3.connect(db_path) as con:

            def count(table: str) -> int:
                try:
                    row = con.execute(
                        f"SELECT COUNT(*) FROM {table} WHERE repo_full = ?",
                        (args.repo,),
                    ).fetchone()
                except sqlite3.OperationalError as exc:
                    print(
                        f"ERROR: Could not query {table}: {exc}",
                        file=sys.stderr,
                    )
                    sys.exit(2)
                return int(row[0]) if row else 0

            counts = {label: count(table) for label, table in TABLES.items()}

    except sqlite3.Error as exc:
        print(f"ERROR: Database failure: {exc}", file=sys.stderr)
        sys.exit(3)

    print(
        f"| {args.repo} "
        f"| {counts['workflows']} "
        f"| {counts['prs']} "
        f"| {counts['reviews']} "
        f"| {counts['commits']} "
        f"| {counts['runs']} "
        f"| {counts['attempts']} "
        f"| {counts['jobs']} "
        f"| {counts['links']} |"
    )


if __name__ == "__main__":
    main()
