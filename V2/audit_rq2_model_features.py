from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
import yaml


EXPECTED = {
    "rq2_pr_cohort.csv": "fbfe6a28ced42f11653609c9e196e0b5596429d7b7467c8c3277c3f17bc43663",
    "rq2_transition_events.csv": "96a66d0273570a63b63600a572d1358362c4dbe7caa8aa1f5ab07c3359aa6f76",
    "rq2_preparation_audit.json": "0afe13857ff574ba6d4c7faf5181fb3579a70811de59c87fae99e6aadd83baf5",
}


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def summarize(group: pd.DataFrame) -> dict:
    transitions = group["transition"].value_counts()
    return {
        "transition_rows": int(len(group)),
        "unique_pull_requests": int(group[["repo_full", "pr_number"]].drop_duplicates().shape[0]),
        "observed_events": int(group["event"].sum()),
        "transition_counts": {str(k): int(v) for k, v in transitions.items()},
    }


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
    if failures:
        print(json.dumps({"status": "FAIL", "failures": failures}, indent=2))
        return 2

    cfg = yaml.safe_load((root / "analysis_config.yaml").read_text(encoding="utf-8-sig"))
    dev = set(cfg["study"]["development_repositories"])
    external = set(cfg["study"]["external_repositories"])
    train_end = pd.Timestamp(cfg["study"]["train_end"], tz="UTC") + pd.Timedelta(days=7)
    holdout_start = pd.Timestamp(cfg["study"]["holdout_start"], tz="UTC")

    cohort = pd.read_csv(rq2 / "rq2_pr_cohort.csv", low_memory=False)
    events = pd.read_csv(rq2 / "rq2_transition_events.csv", low_memory=False)
    cohort["created_at_ts"] = pd.to_datetime(cohort["created_at_ts"], utc=True)
    events = events.merge(
        cohort[["repo_full", "pr_number", "created_at_ts", "prior_week_ci_context_available"]],
        on=["repo_full", "pr_number"], how="left", validate="many_to_one",
    )

    def population(repo: str, created: pd.Timestamp) -> str:
        if repo in dev and created < train_end:
            return "development_training"
        if repo in dev and created >= holdout_start:
            return "development_temporal_holdout"
        if repo in dev:
            return "development_purge"
        if repo in external:
            return "external_evaluation"
        return "outside_contract"

    cohort["rq2_population"] = [
        population(r, t) for r, t in zip(cohort["repo_full"], cohort["created_at_ts"])
    ]
    events["rq2_population"] = [
        population(r, t) for r, t in zip(events["repo_full"], events["created_at_ts"])
    ]

    candidate_features = [
        "author_prior_pr_count", "author_prior_resolved_count",
        "author_prior_merged_count", "author_prior_merge_rate", "author_newcomer",
        "repo_open_prs_at_creation", "repo_pr_arrivals_28d", "repo_merges_28d",
        "draft",
        "prior_week_ci_attempts_total", "prior_week_ci_failure_rate",
        "prior_week_ci_rerun_rate", "prior_week_ci_latency_log",
        "prior_week_ci_workflow_concentration_hhi",
    ]
    feature_rows = []
    for feature in candidate_features:
        if feature not in cohort.columns:
            feature_rows.append({"feature": feature, "present": False})
            continue
        numeric = pd.to_numeric(cohort[feature], errors="coerce")
        feature_rows.append({
            "feature": feature,
            "present": True,
            "nonmissing": int(numeric.notna().sum()),
            "missing": int(numeric.isna().sum()),
            "unique_nonmissing": int(numeric.nunique(dropna=True)),
            "minimum": None if numeric.dropna().empty else float(numeric.min()),
            "median": None if numeric.dropna().empty else float(numeric.median()),
            "maximum": None if numeric.dropna().empty else float(numeric.max()),
        })

    population_rows = []
    for pop, g in cohort.groupby("rq2_population", sort=True):
        ci = g["prior_week_ci_context_available"].eq(1)
        population_rows.append({
            "population": pop,
            "pull_requests": int(len(g)),
            "repositories": int(g["repo_full"].nunique()),
            "created_min": g["created_at_ts"].min().isoformat(),
            "created_max": g["created_at_ts"].max().isoformat(),
            "merged": int(g["event_type"].eq("merged").sum()),
            "closed_unmerged": int(g["event_type"].eq("closed_unmerged").sum()),
            "censored": int(g["event_type"].eq("censored").sum()),
            "qualified_review": int(g["received_qualified_review"].sum()),
            "ci_context_available": int(ci.sum()),
            "ci_context_coverage": float(ci.mean()),
        })

    transition_by_population = {
        pop: summarize(g) for pop, g in events.groupby("rq2_population", sort=True)
    }
    ci_events = events[events["prior_week_ci_context_available"].eq(1)]
    ci_transition_by_population = {
        pop: summarize(g) for pop, g in ci_events.groupby("rq2_population", sort=True)
    }

    zero_duration = int((pd.to_numeric(events["duration_hours"], errors="coerce") == 0).sum())
    result = {
        "status": "PASS" if not failures else "FAIL",
        "failures": failures,
        "split_contract": {
            "development_training_created_before": train_end.isoformat(),
            "development_holdout_created_from": holdout_start.isoformat(),
            "gap_is_purge": True,
            "external_repositories_are_not_for_feature_selection": True,
        },
        "population_coverage": population_rows,
        "transitions_all_prs": transition_by_population,
        "transitions_with_prior_ci_context": ci_transition_by_population,
        "candidate_feature_audit": feature_rows,
        "zero_duration_transition_rows": zero_duration,
        "primary_model_warning": (
            "The non-CI and CI-context models must be compared on the same CI-eligible rows. "
            "A separate all-PR baseline may be reported but is not the incremental-CI comparator."
        ),
    }
    print("RQ2 MODEL-FEATURE FEASIBILITY AUDIT")
    print(json.dumps(result, indent=2))
    return 0 if result["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())