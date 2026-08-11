from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

from db import connect
from utils import load_config

# Minimum ISO weeks with â‰¥1 PR-validation run â€” below this is CRITICAL
MIN_WEEKS = 30




def section(lines: list[str], title: str) -> None:
    lines.append("")
    lines.append("=" * 70)
    lines.append(f"  {title}")
    lines.append("=" * 70)


def safe_run(con, sql: str, params=(), lines: list[str] | None = None,
             label: str = "") -> list[dict] | None:
    """Execute SQL and return rows; on error append CRITICAL and return None."""
    try:
        return [dict(r) for r in con.execute(sql, params)]
    except Exception as exc:
        msg = f"  CRITICAL SQL ERROR [{label}]: {exc}"
        if lines is not None:
            lines.append(msg)
        else:
            print(msg)
        return None


def safe_one(con, sql: str, params=(), lines: list[str] | None = None,
             label: str = "") -> dict | None:
    rows = safe_run(con, sql, params, lines, label)
    if rows is None:
        return None
    return rows[0] if rows else {}


# ISO week Monday: subtract ((weekday+6) % 7) days so Monday = 0 offset
_ISO_MONDAY = "date(created_at, '-' || ((cast(strftime('%w', created_at) AS INTEGER) + 6) % 7) || ' days')"


def check_repos_present(con, lines: list[str],
                        study_repos: list[tuple[str, str]]) -> list[str]:
    """CRITICAL: all study repos must have rows in workflow_runs."""
    section(lines, "1. REPO PRESENCE")
    critical: list[str] = []
    rows = safe_run(con, "SELECT DISTINCT repo_full FROM workflow_runs",
                    lines=lines, label="repo_presence")
    if rows is None:
        critical.append("workflow_runs table unreadable")
        return critical
    present = {r["repo_full"] for r in rows}
    for repo, group in study_repos:
        if repo in present:
            lines.append(f"  OK  {repo}  ({group})")
        else:
            lines.append(f"  CRITICAL MISSING  {repo}  ({group})")
            critical.append(f"repo missing from workflow_runs: {repo}")
    return critical


def check_date_coverage(con, lines: list[str],
                        study_repos: list[tuple[str, str]],
                        study_start: str, study_end: str,
                        pr_events_sql: str) -> list[str]:
    """CRITICAL if weekly coverage < MIN_WEEKS."""
    section(lines, "2. DATE COVERAGE & WEEKLY DENSITY")
    critical: list[str] = []

    # Also check created_at NULL in workflow_runs
    for repo, _ in study_repos:
        null_created = safe_one(con, """
            SELECT COUNT(*) AS n FROM workflow_runs
            WHERE repo_full=? AND (created_at IS NULL OR created_at='')
        """, (repo,), lines, f"null_created_at:{repo}")
        n_null = null_created["n"] if null_created else "ERR"
        if isinstance(n_null, int) and n_null > 0:
            lines.append(f"  WARN {repo}: {n_null} runs with NULL created_at")

    for repo, _ in study_repos:
        r = safe_one(con, """
            SELECT MIN(created_at) AS first_run,
                   MAX(created_at) AS last_run,
                   COUNT(*) AS total_runs
            FROM workflow_runs WHERE repo_full=?
        """, (repo,), lines, f"date_range:{repo}")
        if r is None or not r.get("first_run"):
            lines.append(f"  {repo}: NO DATA")
            continue

        wk_row = safe_one(con, f"""
            SELECT COUNT(DISTINCT {_ISO_MONDAY}) AS weeks
            FROM workflow_runs
            WHERE repo_full=? AND event IN {pr_events_sql}
              AND created_at >= ? AND created_at <= ?
        """, (repo, study_start, study_end), lines, f"week_count:{repo}")
        wk = wk_row["weeks"] if wk_row else 0

        flag = ""
        if isinstance(wk, int) and wk < MIN_WEEKS:
            flag = f"  <-- CRITICAL: only {wk} weeks, need >= {MIN_WEEKS}"
            critical.append(f"{repo}: only {wk} ISO weeks with PR-validation runs")

        lines.append(
            f"  {repo}: first={str(r['first_run'])[:10]}"
            f"  last={str(r['last_run'])[:10]}"
            f"  total_runs={r['total_runs']}"
            f"  pr_validation_weeks={wk}{flag}"
        )
    return critical


def check_table_counts(con, lines: list[str],
                       study_repos: list[tuple[str, str]]) -> None:
    """Row counts across all tables per repo."""
    section(lines, "3. TABLE ROW COUNTS")
    tables = [
        "workflow_runs", "run_attempts", "workflow_jobs",
        "pull_requests", "pr_commits", "pr_reviews", "pr_ci_links",
    ]
    header = f"  {'repo':<30}" + "".join(f"  {t[:14]:<14}" for t in tables)
    lines.append(header)
    for repo, _ in study_repos:
        counts = []
        for table in tables:
            r = safe_one(con,
                f"SELECT COUNT(*) AS n FROM {table} WHERE repo_full=?",
                (repo,), lines, f"count:{table}:{repo}")
            counts.append(r["n"] if r is not None else "ERR")
        lines.append(
            f"  {repo:<30}" + "".join(f"  {str(c):<14}" for c in counts)
        )


def check_workflow_identity(con, lines: list[str],
                            study_repos: list[tuple[str, str]]) -> list[str]:
    """CRITICAL if >5% of workflow_runs have NULL workflow_id."""
    section(lines, "4. WORKFLOW IDENTITY (workflow_id + path)")
    critical: list[str] = []
    for repo, _ in study_repos:
        total_r = safe_one(con,
            "SELECT COUNT(*) AS n FROM workflow_runs WHERE repo_full=?",
            (repo,), lines, f"wf_total:{repo}")
        if total_r is None:
            continue
        total = total_r["n"]

        miss_id = safe_one(con, """
            SELECT COUNT(*) AS n FROM workflow_runs
            WHERE repo_full=? AND (workflow_id IS NULL OR workflow_id=0)
        """, (repo,), lines, f"wf_miss_id:{repo}")
        miss_path = safe_one(con, """
            SELECT COUNT(*) AS n FROM workflow_runs
            WHERE repo_full=? AND (path IS NULL OR path='')
        """, (repo,), lines, f"wf_miss_path:{repo}")
        dist_id = safe_one(con, """
            SELECT COUNT(DISTINCT workflow_id) AS n FROM workflow_runs
            WHERE repo_full=? AND workflow_id IS NOT NULL AND workflow_id!=0
        """, (repo,), lines, f"wf_dist:{repo}")

        n_miss_id   = miss_id["n"]   if miss_id   else 0
        n_miss_path = miss_path["n"] if miss_path else 0
        n_dist      = dist_id["n"]   if dist_id   else 0

        flag_id = flag_path = ""
        if total and n_miss_id / total > 0.05:
            flag_id = "  <-- CRITICAL"
            critical.append(f"{repo}: >5% runs missing workflow_id")
        elif total and n_miss_id / total > 0.01:
            flag_id = "  <-- WARN"
        if total and n_miss_path / total > 0.10:
            flag_path = "  <-- WARN"

        lines.append(
            f"  {repo}: distinct_workflow_ids={n_dist}"
            f"  missing_id={n_miss_id}{flag_id}"
            f"  missing_path={n_miss_path}{flag_path}"
        )
    return critical


def check_missing_timestamps(con, lines: list[str],
                             study_repos: list[tuple[str, str]]) -> list[str]:
    """CRITICAL if >5% of run_attempts missing run_started_at."""
    section(lines, "5. MISSING TIMESTAMPS & CONCLUSIONS")
    critical: list[str] = []
    for repo, _ in study_repos:
        a_total_r = safe_one(con,
            "SELECT COUNT(*) AS n FROM run_attempts WHERE repo_full=?",
            (repo,), lines, f"ts_attempts:{repo}")
        if a_total_r is None:
            continue
        a_total = a_total_r["n"]

        a_no_start = safe_one(con, """
            SELECT COUNT(*) AS n FROM run_attempts
            WHERE repo_full=? AND run_started_at IS NULL
        """, (repo,), lines, f"ts_no_start:{repo}")
        a_no_conc = safe_one(con, """
            SELECT COUNT(*) AS n FROM run_attempts
            WHERE repo_full=? AND (conclusion IS NULL OR conclusion='')
              AND status='completed'
        """, (repo,), lines, f"ts_no_conc:{repo}")
        j_total_r = safe_one(con,
            "SELECT COUNT(*) AS n FROM workflow_jobs WHERE repo_full=?",
            (repo,), lines, f"ts_jobs:{repo}")
        j_no_start = safe_one(con, """
            SELECT COUNT(*) AS n FROM workflow_jobs
            WHERE repo_full=? AND started_at IS NULL
        """, (repo,), lines, f"ts_j_start:{repo}")
        j_no_end = safe_one(con, """
            SELECT COUNT(*) AS n FROM workflow_jobs
            WHERE repo_full=? AND completed_at IS NULL
        """, (repo,), lines, f"ts_j_end:{repo}")

        n_no_start = a_no_start["n"] if a_no_start else 0
        n_no_conc  = a_no_conc["n"]  if a_no_conc  else 0
        j_total    = j_total_r["n"]  if j_total_r  else 0
        n_j_start  = j_no_start["n"] if j_no_start else 0
        n_j_end    = j_no_end["n"]   if j_no_end   else 0

        flag = ""
        if a_total and n_no_start / a_total > 0.05:
            flag = "  <-- CRITICAL"
            critical.append(f"{repo}: >5% run_attempts missing run_started_at")
        elif a_total and n_no_start / a_total > 0.01:
            flag = "  <-- WARN"

        lines.append(
            f"  {repo}: attempts={a_total}"
            f"  no_run_started={n_no_start}{flag}"
            f"  no_conclusion={n_no_conc}"
            f"  jobs={j_total}"
            f"  no_job_started={n_j_start}  no_job_completed={n_j_end}"
        )
    return critical


def check_conclusion_distribution(con, lines: list[str],
                                  study_repos: list[tuple[str, str]],
                                  pr_events_sql: str) -> None:
    """Run conclusion breakdown for PR-validation events only."""
    section(lines, "6. RUN CONCLUSION DISTRIBUTION (PR-validation events only)")
    for repo, _ in study_repos:
        rows = safe_run(con, f"""
            SELECT conclusion, COUNT(*) AS n
            FROM workflow_runs
            WHERE repo_full=? AND event IN {pr_events_sql}
            GROUP BY conclusion ORDER BY n DESC
        """, (repo,), lines, f"conc_dist:{repo}")
        if rows is None:
            continue
        total = sum(r["n"] for r in rows)
        parts = "  ".join(
            f"{r['conclusion'] or 'NULL'}={r['n']}" for r in rows
        )
        lines.append(f"  {repo} (total_pr_validation={total}): {parts}")


def check_rerun_coverage(con, lines: list[str],
                         study_repos: list[tuple[str, str]]) -> None:
    """Fraction of runs with >1 attempt; also verify run_attempts table."""
    section(lines, "7. RERUN COVERAGE (current_run_attempt > 1)")
    for repo, _ in study_repos:
        total_r = safe_one(con,
            "SELECT COUNT(*) AS n FROM workflow_runs WHERE repo_full=?",
            (repo,), lines, f"rerun_total:{repo}")
        rerun_r = safe_one(con, """
            SELECT COUNT(*) AS n FROM workflow_runs
            WHERE repo_full=? AND current_run_attempt > 1
        """, (repo,), lines, f"rerun_count:{repo}")
        multi_r = safe_one(con, """
            SELECT COUNT(*) AS n FROM run_attempts
            WHERE repo_full=? AND attempt_number > 1
        """, (repo,), lines, f"rerun_attempt:{repo}")

        total = total_r["n"] if total_r else 0
        rerun = rerun_r["n"] if rerun_r else 0
        multi = multi_r["n"] if multi_r else 0
        pct   = round(100 * rerun / total, 2) if total else 0
        lines.append(
            f"  {repo}: rerun_runs={rerun}/{total} ({pct}%)"
            f"  attempt_number>1_rows={multi}"
        )


def check_feedback_latency_computability(con, lines: list[str],
                                         study_repos: list[tuple[str, str]]) -> list[str]:
    """CRITICAL if <30% of attempts have computable latency.

    Counts DISTINCT attempts that have at least one job with completed_at,
    not the number of joined rows (avoids inflated >100% figures).
    """
    section(lines, "8. FEEDBACK LATENCY COMPUTABILITY")
    lines.append("  Definition: run_started_at (attempt) â†’ MAX(completed_at) across jobs")
    lines.append("  Unit of measurement: attempt (one latency value per attempt)")
    critical: list[str] = []
    for repo, _ in study_repos:
        total_r = safe_one(con,
            "SELECT COUNT(*) AS n FROM run_attempts WHERE repo_full=?",
            (repo,), lines, f"lat_total:{repo}")
        if total_r is None:
            continue
        total = total_r["n"]

        computable_r = safe_one(con, """
            SELECT COUNT(DISTINCT a.run_id || '-' || a.attempt_number) AS n
            FROM run_attempts a
            WHERE a.repo_full=?
              AND a.run_started_at IS NOT NULL
              AND EXISTS (
                  SELECT 1 FROM workflow_jobs j
                  WHERE j.repo_full=a.repo_full
                    AND j.run_id=a.run_id
                    AND j.attempt_number=a.attempt_number
                    AND j.completed_at IS NOT NULL
              )
        """, (repo,), lines, f"lat_computable:{repo}")
        if computable_r is None:
            continue
        computable = computable_r["n"]
        pct = round(100 * computable / total, 1) if total else 0

        flag = ""
        if pct < 30:
            flag = "  <-- CRITICAL"
            critical.append(f"{repo}: <30% attempts have computable feedback latency")
        elif pct < 60:
            flag = "  <-- WARN"

        lines.append(
            f"  {repo}: computable_attempts={computable}/{total} ({pct}%){flag}"
        )
    return critical


def check_pr_ci_linkage(con, lines: list[str],
                        study_repos: list[tuple[str, str]]) -> list[str]:
    """CRITICAL if <20% of PRs have any CI link."""
    section(lines, "9. PR-CI LINK METHOD BREAKDOWN")
    critical: list[str] = []
    for repo, _ in study_repos:
        method_rows = safe_run(con, """
            SELECT link_method,
                   COUNT(DISTINCT pr_number) AS prs,
                   COUNT(*) AS links
            FROM pr_ci_links WHERE repo_full=?
            GROUP BY link_method ORDER BY prs DESC
        """, (repo,), lines, f"link_method:{repo}")
        total_r    = safe_one(con,
            "SELECT COUNT(*) AS n FROM pull_requests WHERE repo_full=?",
            (repo,), lines, f"link_total_pr:{repo}")
        linked_r   = safe_one(con,
            "SELECT COUNT(DISTINCT pr_number) AS n FROM pr_ci_links WHERE repo_full=?",
            (repo,), lines, f"link_linked:{repo}")

        total_prs  = total_r["n"]  if total_r  else 0
        linked_prs = linked_r["n"] if linked_r else 0
        pct = round(100 * linked_prs / total_prs, 1) if total_prs else 0

        flag = ""
        if pct < 20:
            flag = "  <-- CRITICAL"
            critical.append(f"{repo}: <20% PRs linked to any CI run")
        elif pct < 35:
            flag = "  <-- WARN"

        method_str = ""
        if method_rows:
            method_str = "  methods: " + "  ".join(
                f"{r['link_method']}(prs={r['prs']},links={r['links']})"
                for r in method_rows
            )

        lines.append(
            f"  {repo}: linked_prs={linked_prs}/{total_prs} ({pct}%){flag}{method_str}"
        )
    return critical


def check_bot_reviews(con, lines: list[str],
                      study_repos: list[tuple[str, str]]) -> list[str]:
    """CRITICAL if a repo has 0 human reviews (RQ2 depends on review latency)."""
    section(lines, "10. REVIEW AUTHORSHIP (human vs bot)")
    critical: list[str] = []
    for repo, _ in study_repos:
        human_r = safe_one(con, """
            SELECT COUNT(*) AS n FROM pr_reviews
            WHERE repo_full=? AND (user_type IS NULL OR user_type != 'Bot')
        """, (repo,), lines, f"review_human:{repo}")
        bot_r = safe_one(con, """
            SELECT COUNT(*) AS n FROM pr_reviews
            WHERE repo_full=? AND user_type = 'Bot'
        """, (repo,), lines, f"review_bot:{repo}")

        human = human_r["n"] if human_r else 0
        bot   = bot_r["n"]   if bot_r   else 0
        total = human + bot
        pct_bot = round(100 * bot / total, 1) if total else 0

        flag = ""
        if human == 0:
            flag = "  <-- CRITICAL: no human reviews"
            critical.append(f"{repo}: 0 human reviews â€” RQ2 review latency impossible")

        lines.append(
            f"  {repo}: human={human}  bot={bot}  bot%={pct_bot}{flag}"
        )
    return critical


def check_duplicate_keys(con, lines: list[str],
                         study_repos: list[tuple[str, str]]) -> list[str]:
    """CRITICAL if any primary key duplicates exist."""
    section(lines, "11. DUPLICATE PRIMARY KEYS")
    critical: list[str] = []
    checks = [
        ("workflow_runs",  "repo_full, run_id"),
        ("run_attempts",   "repo_full, run_id, attempt_number"),
        ("workflow_jobs",  "repo_full, run_id, attempt_number, job_id"),
        ("pull_requests",  "repo_full, pr_number"),
    ]
    for table, keys in checks:
        rows = safe_run(con, f"""
            SELECT {keys}, COUNT(*) AS n FROM {table}
            GROUP BY {keys} HAVING COUNT(*) > 1 LIMIT 5
        """, (), lines, f"dup:{table}")
        if rows is None:
            critical.append(f"{table}: could not check duplicates (table missing?)")
        elif rows:
            lines.append(
                f"  CRITICAL {table}: {len(rows)} duplicate key groups (showing â‰¤5)"
            )
            for r in rows:
                lines.append(f"    {r}")
            critical.append(f"{table} has duplicate primary keys")
        else:
            lines.append(f"  OK  {table}")
    return critical


def check_metric_definitions(con, lines: list[str],
                              study_repos: list[tuple[str, str]],
                              failure_conclusions: list[str]) -> None:
    """Document the failure definition and verify values exist in data.

    Failure indicator level:
      - run_level: conclusion of workflow_runs (for MAD baseline)
      - attempt_level: conclusion of run_attempts (for HMM rerun_rate)
      - job_level: job conclusions (for fine-grained failure breakdown)
    """
    section(lines, "12. FAILURE CONCLUSION VALUES & METRIC LEVEL")
    fail_sql = "(" + ",".join(f"'{c}'" for c in failure_conclusions) + ")"
    lines.append(f"  Configured failure_conclusions: {failure_conclusions}")
    lines.append("  Metric level decisions (explicit):")
    lines.append("    failure_rate  â†’ run-level: failed_runs / total_runs (PR-validation)")
    lines.append("    rerun_rate    â†’ run-level: runs with attempt>1 / total_runs")
    lines.append("    feedback_latency â†’ attempt-level: run_started_at â†’ last job completed_at")
    lines.append("")
    for repo, _ in study_repos:
        rows = safe_run(con, f"""
            SELECT conclusion, COUNT(*) AS n FROM workflow_runs
            WHERE repo_full=?
            GROUP BY conclusion ORDER BY n DESC
        """, (repo,), lines, f"metric_def:{repo}")
        if rows is None:
            continue
        total = sum(r["n"] for r in rows)
        fail_total = sum(
            r["n"] for r in rows
            if r["conclusion"] in failure_conclusions
        )
        pct = round(100 * fail_total / total, 1) if total else 0
        parts = "  ".join(
            f"{r['conclusion'] or 'NULL'}={r['n']}" for r in rows
        )
        lines.append(
            f"  {repo}: overall_failure_rate={pct}%  breakdown: {parts}"
        )


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Validate raw SQLite data before analysis"
    )
    ap.add_argument("--config", default="config.yaml")
    args = ap.parse_args()

    cfg: dict[str, Any] = load_config(args.config)
    db_path    = cfg["storage"]["sqlite_path"]
    export_dir = Path(cfg["storage"]["export_dir"])
    export_dir.mkdir(parents=True, exist_ok=True)
    out_path   = export_dir / "raw_data_validation_report.txt"

    study_start = cfg.get("study", {}).get("start_date", "2024-01-01")
    study_end   = cfg.get("study", {}).get("end_date",   "2026-08-07")

    groups: dict[str, list[str]] = cfg.get("repository_groups", {})
    study_repos: list[tuple[str, str]] = (
        [(r, "pilot")      for r in groups.get("pilot",      [])] +
        [(r, "evaluation") for r in groups.get("evaluation", [])]
    )
    # fall back to flat repositories list if no groups defined
    if not study_repos:
        study_repos = [(r, "study") for r in cfg.get("repositories", [])]

    model_cfg          = cfg.get("model", {})
    pr_validation_events: list[str] = model_cfg.get(
        "pr_validation_events",
        ["pull_request", "pull_request_target", "merge_group"],
    )
    failure_conclusions: list[str]  = model_cfg.get(
        "failure_conclusions", ["failure", "timed_out"]
    )
    pr_events_sql = "(" + ",".join(f"'{e}'" for e in pr_validation_events) + ")"

    con = connect(db_path)

    lines: list[str] = [
        "RAW DATA VALIDATION REPORT",
        f"Database : {db_path}",
        f"Study window: {study_start} â€“ {study_end}",
        f"Study repos : {[r for r, _ in study_repos]}",
        f"PR-validation events : {pr_validation_events}",
        f"Failure conclusions  : {failure_conclusions}",
    ]

    all_critical: list[str] = []

    all_critical += check_repos_present(con, lines, study_repos)
    all_critical += check_date_coverage(
        con, lines, study_repos, study_start, study_end, pr_events_sql
    )
    check_table_counts(con, lines, study_repos)
    all_critical += check_workflow_identity(con, lines, study_repos)
    all_critical += check_missing_timestamps(con, lines, study_repos)
    check_conclusion_distribution(
        con, lines, study_repos, pr_events_sql
    )
    check_rerun_coverage(con, lines, study_repos)
    all_critical += check_feedback_latency_computability(con, lines, study_repos)
    all_critical += check_pr_ci_linkage(con, lines, study_repos)
    all_critical += check_bot_reviews(con, lines, study_repos)
    all_critical += check_duplicate_keys(con, lines, study_repos)
    check_metric_definitions(
        con, lines, study_repos, failure_conclusions
    )

    section(lines, "SUMMARY")
    if all_critical:
        lines.append(
            "  RESULT: CRITICAL ISSUES FOUND â€” do not proceed to panel building"
        )
        for c in all_critical:
            lines.append(f"    - {c}")
    else:
        lines.append("  RESULT: ALL CRITICAL CHECKS PASSED â€” safe to proceed")

    report = "\n".join(lines)
    out_path.write_text(report, encoding="utf-8")
    print(report)
    print(f"\n[report] {out_path}")

    if all_critical:
        sys.exit(1)


if __name__ == "__main__":
    main()

