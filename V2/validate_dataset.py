\
from __future__ import annotations

import argparse
import csv
from pathlib import Path

from db import connect
from utils import load_config


CHECKS = {
    "runs_missing_head_sha": """
        SELECT repo_full, COUNT(*) AS n
        FROM workflow_runs WHERE head_sha IS NULL OR head_sha=''
        GROUP BY repo_full
    """,
    "attempts_missing_run_started": """
        SELECT repo_full, COUNT(*) AS n
        FROM run_attempts WHERE run_started_at IS NULL
        GROUP BY repo_full
    """,
    "jobs_missing_start_or_complete": """
        SELECT repo_full, COUNT(*) AS n
        FROM workflow_jobs
        WHERE started_at IS NULL OR completed_at IS NULL
        GROUP BY repo_full
    """,
    "duplicate_job_ids_cross_attempt": """
        SELECT repo_full, run_id, job_id, COUNT(*) AS n
        FROM workflow_jobs
        GROUP BY repo_full, run_id, job_id
        HAVING COUNT(*) > 1
    """,
    "pr_without_commits": """
        SELECT p.repo_full, COUNT(*) AS n
        FROM pull_requests p
        LEFT JOIN pr_commits c
          ON c.repo_full=p.repo_full AND c.pr_number=p.pr_number
        WHERE c.sha IS NULL
        GROUP BY p.repo_full
    """,
    "pr_without_reviews": """
        SELECT p.repo_full, COUNT(*) AS n
        FROM pull_requests p
        LEFT JOIN pr_reviews r
          ON r.repo_full=p.repo_full AND r.pr_number=p.pr_number
        WHERE r.review_id IS NULL
        GROUP BY p.repo_full
    """,
    "pr_with_any_ci_link": """
        SELECT p.repo_full,
               COUNT(DISTINCT p.pr_number) AS total_prs,
               COUNT(DISTINCT l.pr_number) AS linked_prs,
               ROUND(100.0 * COUNT(DISTINCT l.pr_number) / NULLIF(COUNT(DISTINCT p.pr_number),0), 2) AS pct_linked
        FROM pull_requests p
        LEFT JOIN pr_ci_links l
          ON l.repo_full=p.repo_full AND l.pr_number=p.pr_number
        GROUP BY p.repo_full
    """,
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.yaml")
    args = ap.parse_args()
    cfg = load_config(args.config)
    con = connect(cfg["storage"]["sqlite_path"])

    export_dir = Path(cfg["storage"]["export_dir"])
    export_dir.mkdir(parents=True, exist_ok=True)
    out = export_dir / "data_quality_report.txt"

    lines = []
    for name, sql in CHECKS.items():
        lines.append(f"## {name}")
        rows = [dict(r) for r in con.execute(sql)]
        if not rows:
            lines.append("OK / no rows")
        else:
            for r in rows:
                lines.append(str(r))
        lines.append("")

    out.write_text("\n".join(lines), encoding="utf-8")
    print(out)
    print("\n".join(lines))


if __name__ == "__main__":
    main()
