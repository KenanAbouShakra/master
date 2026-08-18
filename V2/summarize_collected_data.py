from __future__ import annotations

import argparse
import json
import sqlite3
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Any

import yaml


CORE_TABLES = (
    "workflows",
    "pull_requests",
    "pr_reviews",
    "pr_commits",
    "workflow_runs",
    "run_attempts",
    "workflow_jobs",
    "pr_ci_links",
)


def scalar(con: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> Any:
    row = con.execute(sql, params).fetchone()
    return row[0] if row else None


def counts_by_repo(con: sqlite3.Connection, table: str) -> dict[str, int]:
    return {
        str(repo): int(count)
        for repo, count in con.execute(
            f"SELECT repo_full, COUNT(*) FROM {table} GROUP BY repo_full"
        )
    }


def percent(numerator: int | float, denominator: int | float) -> str:
    return f"{100 * numerator / denominator:.1f}%" if denominator else "n/a"


def median(values: list[float]) -> str:
    return f"{statistics.median(values):.1f}" if values else "n/a"


def markdown_table(headers: list[str], rows: list[list[Any]]) -> list[str]:
    return [
        "| " + " | ".join(headers) + " |",
        "|" + "|".join("---" for _ in headers) + "|",
        *("| " + " | ".join(str(value) for value in row) + " |" for row in rows),
    ]


def distribution(con: sqlite3.Connection, table: str, column: str) -> list[tuple[str, int]]:
    return [
        (str(value or "(missing)"), int(count))
        for value, count in con.execute(
            f"SELECT {column}, COUNT(*) AS n FROM {table} "
            f"GROUP BY {column} ORDER BY n DESC"
        )
    ]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create a Markdown summary of the collected CI research data."
    )
    parser.add_argument("--db", default="data/ci_research.sqlite")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--quality-report", default="data/exports/data_quality_report.json")
    parser.add_argument("--output", default="COLLECTED_DATA_SUMMARY.md")
    args = parser.parse_args()

    db_path = Path(args.db)
    config = yaml.safe_load(Path(args.config).read_text(encoding="utf-8"))
    quality = json.loads(Path(args.quality_report).read_text(encoding="utf-8"))
    groups = {
        repo: group
        for group, repositories in config["repository_groups"].items()
        for repo in repositories
    }
    study_repos = list(config["repositories"])

    with sqlite3.connect(db_path) as con:
        table_counts = {
            table: int(scalar(con, f"SELECT COUNT(*) FROM {table}"))
            for table in CORE_TABLES
        }
        per_repo = {table: counts_by_repo(con, table) for table in CORE_TABLES}
        metadata_repos = [
            row[0] for row in con.execute("SELECT repo_full FROM repositories ORDER BY repo_full")
        ]

        pr_metrics: dict[str, dict[str, Any]] = defaultdict(dict)
        for repo, total, merged, closed, drafts, bots in con.execute(
            """
            SELECT repo_full, COUNT(*),
                   SUM(merged_at IS NOT NULL), SUM(closed_at IS NOT NULL),
                   SUM(draft = 1), SUM(LOWER(COALESCE(author_type, '')) = 'bot')
            FROM pull_requests GROUP BY repo_full
            """
        ):
            pr_metrics[repo].update(
                total=int(total), merged=int(merged), closed=int(closed),
                drafts=int(drafts), bots=int(bots)
            )
        for repo in study_repos:
            rows = con.execute(
                """
                SELECT additions, deletions, changed_files
                FROM pull_requests WHERE repo_full = ?
                """,
                (repo,),
            ).fetchall()
            pr_metrics[repo]["median_additions"] = median(
                [float(row[0]) for row in rows if row[0] is not None]
            )
            pr_metrics[repo]["median_deletions"] = median(
                [float(row[1]) for row in rows if row[1] is not None]
            )
            pr_metrics[repo]["median_files"] = median(
                [float(row[2]) for row in rows if row[2] is not None]
            )

        linked_prs = {
            repo: int(count)
            for repo, count in con.execute(
                "SELECT repo_full, COUNT(DISTINCT pr_number) FROM pr_ci_links GROUP BY repo_full"
            )
        }
        retry_stats = {
            repo: (int(runs), int(retried), int(max_attempt))
            for repo, runs, retried, max_attempt in con.execute(
                """
                SELECT repo_full, COUNT(DISTINCT run_id),
                       COUNT(DISTINCT CASE WHEN attempt_number > 1 THEN run_id END),
                       MAX(attempt_number)
                FROM run_attempts GROUP BY repo_full
                """
            )
        }
        job_durations = {
            repo: (int(valid), float(avg_minutes))
            for repo, valid, avg_minutes in con.execute(
                """
                SELECT repo_full, COUNT(*),
                       AVG((julianday(completed_at) - julianday(started_at)) * 1440.0)
                FROM workflow_jobs
                WHERE started_at IS NOT NULL AND completed_at IS NOT NULL
                  AND completed_at >= started_at
                  AND (julianday(completed_at) - julianday(started_at)) * 1440.0 <= 1440
                GROUP BY repo_full
                """
            )
        }
        date_ranges = {
            "Pull requests": con.execute(
                "SELECT MIN(created_at), MAX(created_at) FROM pull_requests"
            ).fetchone(),
            "Workflow runs": con.execute(
                "SELECT MIN(created_at), MAX(created_at) FROM workflow_runs"
            ).fetchone(),
            "Reviews": con.execute(
                "SELECT MIN(submitted_at), MAX(submitted_at) FROM pr_reviews"
            ).fetchone(),
            "Commits": con.execute(
                "SELECT MIN(authored_at), MAX(authored_at) FROM pr_commits"
            ).fetchone(),
        }
        run_conclusions = distribution(con, "workflow_runs", "conclusion")
        run_events = distribution(con, "workflow_runs", "event")
        job_conclusions = distribution(con, "workflow_jobs", "conclusion")
        link_methods = distribution(con, "pr_ci_links", "link_method")

    lines = [
        "# Summary of Collected CI Research Data",
        "",
        "## Overview",
        "",
        f"- Configured study period: {config['study']['start_date']} to {config['study']['end_date']}.",
        f"- Research sample: {len(study_repos)} repositories, divided into 3 pilot and 3 evaluation repositories.",
        f"- Data quality status: **{quality['status']}** with {len(quality['errors'])} reported errors.",
        f"- Canonical modelling runs: {quality['canonical_run_rows']:,}; repository-weeks: {quality['panel_rows']:,}; workflow-weeks: {quality['workflow_panel_rows']:,}.",
        f"- The database contains metadata for {len(metadata_repos)} repositories. {len(metadata_repos) - len(study_repos)} are prescreening candidates outside the research sample.",
        "",
        "## Data Volume",
        "",
        *markdown_table(
            ["Data type", "Rows"],
            [[table, f"{count:,}"] for table, count in table_counts.items()],
        ),
        "",
        "## Per Repository",
        "",
        *markdown_table(
            ["Repository", "Group", "Workflows", "PRs", "Reviews", "Commits", "Runs", "Attempts", "Jobs", "PR-CI links"],
            [
                [repo, groups.get(repo, "-"), *[f"{per_repo[table].get(repo, 0):,}" for table in CORE_TABLES]]
                for repo in study_repos
            ],
        ),
        "",
        "## Pull Requests",
        "",
        *markdown_table(
            ["Repository", "PRs", "Merged", "Closed", "Draft", "Bot", "Median additions", "Median deletions", "Median files", "PRs with CI link"],
            [
                [
                    repo,
                    f"{pr_metrics[repo]['total']:,}",
                    percent(pr_metrics[repo]["merged"], pr_metrics[repo]["total"]),
                    percent(pr_metrics[repo]["closed"], pr_metrics[repo]["total"]),
                    percent(pr_metrics[repo]["drafts"], pr_metrics[repo]["total"]),
                    percent(pr_metrics[repo]["bots"], pr_metrics[repo]["total"]),
                    pr_metrics[repo]["median_additions"],
                    pr_metrics[repo]["median_deletions"],
                    pr_metrics[repo]["median_files"],
                    percent(linked_prs.get(repo, 0), pr_metrics[repo]["total"]),
                ]
                for repo in study_repos
            ],
        ),
        "",
        "## CI Attempts and Jobs",
        "",
        *markdown_table(
            ["Repository", "Runs with attempts", "Runs with retry", "Retry rate", "Maximum attempts", "Valid job durations", "Mean job minutes"],
            [
                [
                    repo,
                    f"{retry_stats[repo][0]:,}",
                    f"{retry_stats[repo][1]:,}",
                    percent(retry_stats[repo][1], retry_stats[repo][0]),
                    retry_stats[repo][2],
                    f"{job_durations[repo][0]:,}",
                    f"{job_durations[repo][1]:.1f}",
                ]
                for repo in study_repos
            ],
        ),
        "",
        "## Temporal Coverage",
        "",
        *markdown_table(
            ["Data type", "Earliest timestamp", "Latest timestamp"],
            [[name, start, end] for name, (start, end) in date_ranges.items()],
        ),
        "",
        "## Key Distributions",
        "",
        "### Workflow-run conclusions",
        "",
        *markdown_table(["Conclusion", "Count", "Share"], [[name, f"{count:,}", percent(count, table_counts['workflow_runs'])] for name, count in run_conclusions]),
        "",
        "### Workflow-run events",
        "",
        *markdown_table(["Event", "Count", "Share"], [[name, f"{count:,}", percent(count, table_counts['workflow_runs'])] for name, count in run_events]),
        "",
        "### Job conclusions",
        "",
        *markdown_table(["Conclusion", "Count", "Share"], [[name, f"{count:,}", percent(count, table_counts['workflow_jobs'])] for name, count in job_conclusions]),
        "",
        "### PR-CI link methods",
        "",
        *markdown_table(["Method", "Count", "Share"], [[name, f"{count:,}", percent(count, table_counts['pr_ci_links'])] for name, count in link_methods]),
        "",
        "## Modelling Panel and Data Quality",
        "",
        f"- Analysis weeks: {quality['date_min']} to {quality['date_max']}.",
        f"- Split: {', '.join(f'{name}={count}' for name, count in quality['rows_by_split'].items())}.",
        f"- Excluded or duplicate run rows: {quality['excluded_or_duplicate_run_rows']:,}.",
        f"- Invalid duration rows: {quality['invalid_duration_rows']:,}.",
        f"- Repository-weeks without runs: {quality['zero_run_repository_weeks']}.",
        f"- Missing training targets: {quality['training_target_missing']} (allowed limit: {quality['training_target_missing_allowed']}).",
        "- Attempt coverage is 100% in four repositories, 99.97% for tektoncd/pipeline, and 99.90% for containerd/containerd.",
        "- The valid duration fraction per repository ranges from 92.36% to 98.94%.",
        "",
        "## Interpretation Notes",
        "",
        "- `workflow_runs` is the raw table. The modelling panel uses 115,368 canonical rows after filtering, so the raw total must not be treated as the model's effective sample size.",
        "- `pr_ci_links` is a many-to-many table. The number of links is not the same as the number of PRs with CI.",
        "- `workflow_jobs` dominates the row count because one workflow run can contain many jobs and attempts.",
        "- The MAD alarm in the modelling panel is a baseline signal for the next four weeks, not observed ground truth.",
        "- Repository comparisons should be normalized per week, PR, or run because activity levels and workflow structures vary substantially.",
        "",
        "## Metadata Repositories Outside the Research Sample",
        "",
        ", ".join(repo for repo in metadata_repos if repo not in study_repos) or "None.",
        "",
    ]
    Path(args.output).write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()