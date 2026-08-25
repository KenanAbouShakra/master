from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd


EXPECTED = {
    "rq2_pr_cohort.csv": "fbfe6a28ced42f11653609c9e196e0b5596429d7b7467c8c3277c3f17bc43663",
    "rq2_transition_events.csv": "96a66d0273570a63b63600a572d1358362c4dbe7caa8aa1f5ab07c3359aa6f76",
}


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def main() -> int:
    root = Path.cwd()
    rq2 = root / "analysis_outputs" / "rq2_preparation"
    failures: dict[str, object] = {}
    for name, expected in EXPECTED.items():
        path = rq2 / name
        if not path.is_file():
            failures[name] = "missing"
        elif sha256(path) != expected:
            failures[name] = "hash mismatch"
    commits_path = root / "data" / "exports" / "pr_commits.csv"
    if not commits_path.is_file():
        failures["pr_commits.csv"] = "missing"
    if failures:
        print(json.dumps({"status": "FAIL", "failures": failures}, indent=2))
        return 2

    cohort = pd.read_csv(rq2 / "rq2_pr_cohort.csv", low_memory=False)
    events = pd.read_csv(rq2 / "rq2_transition_events.csv", low_memory=False)
    commits = pd.read_csv(commits_path, low_memory=False)
    required = {"repo_full", "pr_number", "sha", "authored_at", "committed_at"}
    missing = sorted(required - set(commits.columns))
    if missing:
        print(json.dumps({"status": "FAIL", "failures": {"commit_columns": missing}}, indent=2))
        return 2

    cohort["created_at_ts"] = pd.to_datetime(cohort["created_at_ts"], utc=True)
    commits["committed_at_ts"] = pd.to_datetime(commits["committed_at"], errors="coerce", utc=True)
    commits["authored_at_ts"] = pd.to_datetime(commits["authored_at"], errors="coerce", utc=True)
    c = commits.merge(
        cohort[["repo_full", "pr_number", "created_at_ts"]],
        on=["repo_full", "pr_number"], how="left", validate="many_to_one",
        indicator=True,
    )
    c["best_commit_time"] = c["committed_at_ts"].fillna(c["authored_at_ts"])
    c["known_at_creation"] = c["best_commit_time"].notna() & (c["best_commit_time"] <= c["created_at_ts"])
    c["after_creation"] = c["best_commit_time"].notna() & (c["best_commit_time"] > c["created_at_ts"])

    counts = c.groupby(["repo_full", "pr_number"]).agg(
        commit_rows=("sha", "size"),
        commits_known_at_creation=("known_at_creation", "sum"),
        commits_after_creation=("after_creation", "sum"),
        commits_without_time=("best_commit_time", lambda s: int(s.isna().sum())),
    ).reset_index()
    enriched = cohort[["repo_full", "pr_number", "prior_week_ci_context_available"]].merge(
        counts, on=["repo_full", "pr_number"], how="left", validate="one_to_one"
    )
    for col in ["commit_rows", "commits_known_at_creation", "commits_after_creation", "commits_without_time"]:
        enriched[col] = enriched[col].fillna(0).astype(int)

    zero = events[pd.to_numeric(events["duration_hours"], errors="coerce").eq(0)].copy()
    zero_rows = []
    for row in zero.itertuples(index=False):
        zero_rows.append({
            "repo_full": row.repo_full,
            "pr_number": int(row.pr_number),
            "transition": row.transition,
            "event": int(row.event),
            "start_at": str(row.start_at),
            "stop_at": str(row.stop_at),
        })

    dist = enriched["commits_known_at_creation"].describe(percentiles=[.25, .5, .75, .9, .95]).to_dict()
    result = {
        "status": "PASS",
        "failures": {},
        "zero_duration_transition_rows": zero_rows,
        "commit_timing": {
            "rows": int(len(commits)),
            "unique_prs_in_commit_file": int(commits[["repo_full", "pr_number"]].drop_duplicates().shape[0]),
            "unmatched_commit_rows": int(c["_merge"].ne("both").sum()),
            "invalid_committed_at": int(commits["committed_at_ts"].isna().sum()),
            "invalid_authored_at": int(commits["authored_at_ts"].isna().sum()),
            "commit_rows_known_at_creation": int(c["known_at_creation"].sum()),
            "commit_rows_after_creation": int(c["after_creation"].sum()),
            "commit_rows_without_usable_time": int(c["best_commit_time"].isna().sum()),
            "prs_with_any_commit_record": int(enriched["commit_rows"].gt(0).sum()),
            "prs_with_commit_known_at_creation": int(enriched["commits_known_at_creation"].gt(0).sum()),
            "prs_without_commit_known_at_creation": int(enriched["commits_known_at_creation"].eq(0).sum()),
            "ci_eligible_prs_with_commit_known_at_creation": int((
                enriched["prior_week_ci_context_available"].eq(1)
                & enriched["commits_known_at_creation"].gt(0)
            ).sum()),
            "creation_commit_count_distribution": {
                str(k): float(v) for k, v in dist.items()
            },
        },
        "decision_rule": (
            "Use commits_known_at_creation only if timestamps are valid and coverage is adequate; "
            "never substitute final commits_count in the primary predictive model."
        ),
    }
    print("RQ2 COMMIT-TIMING AND ZERO-DURATION AUDIT")
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())