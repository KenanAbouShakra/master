from __future__ import annotations

import os
from datetime import date, timedelta
from typing import Any

import requests

from db import connect
from utils import load_config


REPOS = (
    "docker/cli",
    "prometheus/prometheus",
    "tektoncd/pipeline",
    "pytest-dev/pytest",
    "helm/helm",
)
PR_EVENTS = ("pull_request", "pull_request_target", "merge_group")

API_VERSION = "2026-03-10"
TRAINING_LAST_WEEK = date(2026, 2, 23)
HOLDOUT_FIRST_WEEK = date(2026, 3, 2)
DEFAULT_STUDY_START = date(2025, 6, 11)
DEFAULT_STUDY_END = date(2026, 8, 7)


def iso_monday_of(day: date) -> date:
    return day - timedelta(days=day.weekday())


def first_full_week_monday(period_start: date) -> date:
    monday = iso_monday_of(period_start)
    return monday if period_start == monday else monday + timedelta(weeks=1)


def last_full_week_monday(period_end: date) -> date:
    monday = iso_monday_of(period_end)
    return monday if monday + timedelta(days=6) <= period_end else monday - timedelta(weeks=1)


def mondays_between(first_monday: date, last_monday: date) -> list[date]:
    if first_monday > last_monday:
        return []
    result: list[date] = []
    current = first_monday
    while current <= last_monday:
        result.append(current)
        current += timedelta(weeks=1)
    return result


def full_week_mondays(period_start: date, period_end: date) -> list[date]:
    return mondays_between(
        first_full_week_monday(period_start),
        last_full_week_monday(period_end),
    )


def week_stats(
    week_counts: dict[date, int], period_start: date, period_end: date
) -> dict[str, Any]:
    expected = full_week_mondays(period_start, period_end)
    weeks_with_runs = [week for week in expected if week_counts.get(week, 0) > 0]
    no_stored_runs = [week for week in expected if week_counts.get(week, 0) == 0]
    low_volume = [week for week in expected if 0 < week_counts.get(week, 0) <= 2]
    return {
        "expected_full_weeks": len(expected),
        "weeks_with_runs": len(weeks_with_runs),
        "no_stored_runs": len(no_stored_runs),
        "low_volume_weeks": len(low_volume),
        "no_stored_run_dates": no_stored_runs,
    }


def first_week_with_no_later_empty_weeks(
    week_counts: dict[date, int], last_full_week: date
) -> date | None:
    """Diagnostic only: first stored week followed by no empty stored weeks."""
    candidates = sorted(week for week in week_counts if week <= last_full_week)
    for candidate in candidates:
        expected = mondays_between(candidate, last_full_week)
        if all(week_counts.get(week, 0) > 0 for week in expected):
            return candidate
    return None


def api_event_counts_for_week(
    session: requests.Session,
    repo: str,
    week_start: date,
) -> tuple[int | None, dict[str, int | str]]:
    """Return API total_count separately for each relevant event."""
    week_end = week_start + timedelta(days=6)
    created = (
        f"{week_start.isoformat()}T00:00:00Z.."
        f"{week_end.isoformat()}T23:59:59Z"
    )
    url = f"https://api.github.com/repos/{repo}/actions/runs"
    counts: dict[str, int | str] = {}
    total = 0

    for event in PR_EVENTS:
        try:
            response = session.get(
                url,
                params={"created": created, "event": event, "per_page": 1},
                timeout=30,
            )
        except requests.RequestException as exc:
            counts[event] = f"REQUEST_ERROR: {exc.__class__.__name__}"
            return None, counts

        if not response.ok:
            counts[event] = f"HTTP {response.status_code}"
            return None, counts

        value = response.json().get("total_count")
        if not isinstance(value, int):
            counts[event] = "INVALID_RESPONSE"
            return None, counts

        counts[event] = value
        total += value

    return total, counts


def print_period_stats(
    week_counts: dict[date, int],
    label: str,
    period_start: date,
    period_end: date,
) -> None:
    if period_start > period_end:
        print(f"    {label:<10}: start after end — skipped")
        return

    stats = week_stats(week_counts, period_start, period_end)
    print(
        f"    {label:<10}: expected_full_weeks={stats['expected_full_weeks']}"
        f"  weeks_with_runs={stats['weeks_with_runs']}"
        f"  no_stored_runs={stats['no_stored_runs']}"
        f"  low_volume(1-2)={stats['low_volume_weeks']}"
    )
    dates = stats["no_stored_run_dates"]
    if dates:
        preview = ", ".join(map(str, dates[:8]))
        if len(dates) > 8:
            preview += ", ..."
        print(f"      weeks with no stored PR-validation runs: {preview}")


def main() -> None:
    config = load_config("config.yaml")
    study = config.get("study", {})
    study_start = date.fromisoformat(
        study.get("start_date", DEFAULT_STUDY_START.isoformat())
    )
    study_end = date.fromisoformat(
        study.get("end_date", DEFAULT_STUDY_END.isoformat())
    )
    if study_start > study_end:
        raise ValueError("study.start_date must not be later than study.end_date")

    first_full_week = first_full_week_monday(study_start)
    last_full_week = last_full_week_monday(study_end)
    training_last_week = min(TRAINING_LAST_WEEK, last_full_week)
    holdout_first_week = max(HOLDOUT_FIRST_WEEK, first_full_week)

    token = os.environ.get("GITHUB_TOKEN")
    session = requests.Session()
    session.headers.update({
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": API_VERSION,
        "User-Agent": "msc-ci-coverage-validation/1.0",
    })
    if token:
        session.headers["Authorization"] = f"Bearer {token}"

    event_placeholders = ",".join("?" for _ in PR_EVENTS)
    summary: list[tuple[str, str, str, str, str, int]] = []
    connection = connect(config["storage"]["sqlite_path"])

    print("=" * 92)
    print("COVERAGE DIAGNOSTIC — read-only")
    print(f"Configured study window : {study_start} through {study_end}")
    print(f"Full calendar weeks     : {first_full_week} through {last_full_week}")
    print(f"Training weeks          : through Monday {training_last_week}")
    print(f"Holdout weeks           : Monday {holdout_first_week} through {last_full_week}")
    print("=" * 92)

    try:
        for repo in REPOS:
            print(f"\n--- {repo} ---")

            row_any = connection.execute(
                "SELECT MIN(created_at) FROM workflow_runs WHERE repo_full = ?",
                (repo,),
            ).fetchone()
            first_any = (
                date.fromisoformat(row_any[0][:10]) if row_any and row_any[0] else None
            )

            row_pr = connection.execute(
                f"SELECT MIN(created_at) FROM workflow_runs"
                f" WHERE repo_full = ? AND event IN ({event_placeholders})",
                (repo, *PR_EVENTS),
            ).fetchone()
            first_pr = (
                date.fromisoformat(row_pr[0][:10]) if row_pr and row_pr[0] else None
            )

            rows = connection.execute(
                f"""
                SELECT
                    date(
                        created_at,
                        '-' || (
                            (CAST(strftime('%w', created_at) AS INTEGER) + 6) % 7
                        ) || ' days'
                    ) AS week_monday,
                    COUNT(*) AS run_count
                FROM workflow_runs
                WHERE repo_full = ? AND event IN ({event_placeholders})
                GROUP BY week_monday
                ORDER BY week_monday
                """,
                (repo, *PR_EVENTS),
            ).fetchall()
            week_counts = {date.fromisoformat(row[0]): int(row[1]) for row in rows}
            observed_weeks = sorted(week_counts)

            first_observed = observed_weeks[0] if observed_weeks else None
            first_full_after_run = first_full_week_monday(first_pr) if first_pr else None
            diagnostic_week = first_week_with_no_later_empty_weeks(
                week_counts, last_full_week
            )

            print(f"  Earliest stored run (any event)           : {first_any}")
            print(f"  Earliest stored PR-validation run         : {first_pr}")
            print(f"  First observed PR-validation week         : {first_observed}")
            print(f"  First full calendar week after first run  : {first_full_after_run}")
            print(f"  Diagnostic no-later-empty-week candidate  : {diagnostic_week}")
            print("  NOTE: This candidate does not prove collection completeness.")

            print("\n  First 8 observed PR-validation weeks:")
            if not observed_weeks:
                print("    none")
            for week in observed_weeks[:8]:
                annotation = ""
                if week == first_observed and first_pr and first_pr > week:
                    annotation = "  [first run occurs after Monday; completeness unknown]"
                print(f"    {week}: n={week_counts[week]}{annotation}")

            print("\n  Last 2 observed PR-validation weeks:")
            if not observed_weeks:
                print("    none")
            for week in observed_weeks[-2:]:
                annotation = ""
                if week + timedelta(days=6) > study_end:
                    annotation = "  [partial relative to configured study end]"
                print(f"    {week}: n={week_counts[week]}{annotation}")

            analysis_start = max(
                first_full_after_run or first_full_week,
                first_full_week,
            )
            print("\n  Stored-run statistics (full calendar weeks only):")
            print_period_stats(week_counts, "Full", analysis_start, study_end)
            print_period_stats(
                week_counts, "Training", analysis_start,
                training_last_week + timedelta(days=6)
            )
            print_period_stats(week_counts, "Holdout", holdout_first_week, study_end)

            if token and first_pr:
                print("\n  Fresh API boundary checks by PR event:")
                boundary_weeks = (
                    iso_monday_of(first_pr) - timedelta(weeks=1),
                    iso_monday_of(first_pr),
                )
                for week in boundary_weeks:
                    api_total, event_counts = api_event_counts_for_week(
                        session, repo, week
                    )
                    db_total = week_counts.get(week, 0)
                    print(
                        f"    {week}..{week + timedelta(days=6)}:"
                        f" API_PR_total={api_total} DB_PR_total={db_total}"
                        f" events={event_counts}"
                    )
            elif not token:
                print("\n  API checks skipped: GITHUB_TOKEN is not set.")

            summary.append((
                repo,
                str(first_any) if first_any else "N/A",
                str(first_pr) if first_pr else "N/A",
                str(first_full_after_run) if first_full_after_run else "N/A",
                str(diagnostic_week) if diagnostic_week else "N/A",
                len(observed_weeks),
            ))
    finally:
        connection.close()
        session.close()

    print("\n" + "=" * 92)
    print("SUMMARY")
    print("=" * 92)
    print(
        f"{'Repository':<28} {'First any':<12} {'First PR':<12} "
        f"{'First full week':<16} {'Diagnostic week':<16} {'Obs.wks':>7}"
    )
    for repo, first_any, first_pr, first_full, diagnostic, observed in summary:
        print(
            f"{repo:<28} {first_any:<12} {first_pr:<12} "
            f"{first_full:<16} {diagnostic:<16} {observed:>7}"
        )

    diagnostic_dates = [
        date.fromisoformat(row[4]) for row in summary if row[4] != "N/A"
    ]
    if len(diagnostic_dates) == len(REPOS):
        print(
            "\nDiagnostic common candidate based only on stored PR-run presence: "
            f"{max(diagnostic_dates)}"
        )
    else:
        print("\nNo diagnostic common candidate could be calculated for all repositories.")

    print("This output alone must not be used to claim API completeness or alter config.yaml.")


if __name__ == "__main__":
    main()
