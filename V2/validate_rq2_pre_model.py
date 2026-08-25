from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd


EXPECTED_RQ1_MANIFEST_HASH = "d871b9c08870c57486985e601d448466c0baecbbd505679dce767ddf385bf55d"
EXPECTED_CENSOR_AT = pd.Timestamp("2026-08-08T00:00:00Z")


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def main() -> int:
    root = Path.cwd()
    out = root / "analysis_outputs"
    rq2 = out / "rq2_preparation"
    cohort_path = rq2 / "rq2_pr_cohort.csv"
    events_path = rq2 / "rq2_transition_events.csv"
    audit_path = rq2 / "rq2_preparation_audit.json"
    manifest_path = out / "RQ1_BASELINES_FROZEN.json"
    failures: dict[str, object] = {}

    for path in (cohort_path, events_path, audit_path, manifest_path):
        if not path.is_file():
            failures[str(path)] = "missing"
    if failures:
        print(json.dumps({"status": "FAIL", "failures": failures}, indent=2))
        return 2

    cohort = pd.read_csv(
        cohort_path,
        parse_dates=[
            "created_at_ts", "closed_at_ts", "merged_at_ts", "resolution_at",
            "observation_end_at", "first_qualified_review_at", "ci_context_week",
        ],
        low_memory=False,
    )
    events = pd.read_csv(
        events_path, parse_dates=["start_at", "stop_at"], low_memory=False
    )
    audit = json.loads(audit_path.read_text(encoding="utf-8"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    if sha256(manifest_path) != EXPECTED_RQ1_MANIFEST_HASH:
        failures["rq1_manifest"] = "manifest hash mismatch"
    for relative, record in manifest.get("files", {}).items():
        path = root / relative
        if not path.is_file():
            failures[relative] = "missing"
        elif path.stat().st_size != record["bytes"]:
            failures[relative] = "size mismatch"
        elif sha256(path) != record["sha256"]:
            failures[relative] = "hash mismatch"

    if len(cohort) != 14283:
        failures["cohort_rows"] = len(cohort)
    if len(events) != 25266:
        failures["transition_rows"] = len(events)

    duplicate_prs = int(cohort.duplicated(["repo_full", "pr_number"]).sum())
    if duplicate_prs:
        failures["duplicate_prs"] = duplicate_prs

    negative_durations = int((events["duration_hours"] < 0).sum())
    if negative_durations:
        failures["negative_transition_durations"] = negative_durations

    timestamp_duration = (
        events["stop_at"] - events["start_at"]
    ).dt.total_seconds() / 3600
    duration_close = np.isclose(
        timestamp_duration.to_numpy(float),
        events["duration_hours"].to_numpy(float),
        rtol=1e-10,
        atol=1e-8,
        equal_nan=False,
    )
    duration_disagreement = int((~duration_close).sum())
    if duration_disagreement:
        failures["duration_disagreement"] = duration_disagreement

    observed_allowed = {"0->1", "0->2", "0->3", "1->2", "1->3"}
    censored_allowed = {"0->censored", "1->censored"}
    invalid_observed = sorted(
        set(events.loc[events["event"].eq(1), "transition"]) - observed_allowed
    )
    invalid_censored = sorted(
        set(events.loc[events["event"].eq(0), "transition"]) - censored_allowed
    )
    if invalid_observed:
        failures["invalid_observed_transitions"] = invalid_observed
    if invalid_censored:
        failures["invalid_censor_transitions"] = invalid_censored

    rows_per_pr = events.groupby(["repo_full", "pr_number"]).size()
    invalid_rows_per_pr = int((~rows_per_pr.isin([1, 2])).sum())
    if invalid_rows_per_pr:
        failures["invalid_rows_per_pr"] = invalid_rows_per_pr

    reviewed = cohort["received_qualified_review"].eq(1)
    expected_event_rows = int((reviewed.astype(int) + 1).sum())
    if len(events) != expected_event_rows:
        failures["review_transition_row_contract"] = {
            "expected": expected_event_rows,
            "actual": len(events),
        }

    review_before_creation = int((
        cohort["first_qualified_review_at"].notna()
        & (cohort["first_qualified_review_at"] < cohort["created_at_ts"])
    ).sum())
    review_after_observation = int((
        cohort["first_qualified_review_at"].notna()
        & (cohort["first_qualified_review_at"] > cohort["observation_end_at"])
    ).sum())
    if review_before_creation:
        failures["review_before_creation"] = review_before_creation
    if review_after_observation:
        failures["review_after_observation"] = review_after_observation

    wrong_censor_time = int((
        cohort["event_type"].eq("censored")
        & (cohort["observation_end_at"] != EXPECTED_CENSOR_AT)
    ).sum())
    if wrong_censor_time:
        failures["wrong_censor_time"] = wrong_censor_time

    context_flag = cohort["prior_week_ci_context_available"]
    invalid_context_flags = int((~context_flag.isin([0, 1])).sum())
    if invalid_context_flags:
        failures["invalid_context_flags"] = invalid_context_flags

    ci_columns = [
        column for column in cohort.columns
        if column.startswith("prior_week_ci_")
        and column != "prior_week_ci_context_available"
    ]
    if not ci_columns:
        failures["prior_week_ci_columns"] = "none found"
        context_without_values = values_without_context = -1
    else:
        context_without_values = int((
            context_flag.eq(1) & cohort[ci_columns].isna().all(axis=1)
        ).sum())
        values_without_context = int((
            context_flag.eq(0) & cohort[ci_columns].notna().any(axis=1)
        ).sum())
        if context_without_values:
            failures["context_flag_without_ci_values"] = context_without_values
        if values_without_context:
            failures["ci_values_without_context_flag"] = values_without_context

    if audit.get("status") != "PASS":
        failures["preparation_audit_status"] = audit.get("status")
    if audit.get("administrative_censor_at") != EXPECTED_CENSOR_AT.isoformat():
        failures["audit_censor_at"] = audit.get("administrative_censor_at")

    transition_counts = {
        str(k): int(v) for k, v in events["transition"].value_counts().items()
    }
    event_counts = {
        str(k): int(v) for k, v in cohort["event_type"].value_counts().items()
    }
    result = {
        "status": "PASS" if not failures else "FAIL",
        "failures": failures,
        "cohort_rows": int(len(cohort)),
        "transition_rows": int(len(events)),
        "unique_pull_requests": int(
            cohort[["repo_full", "pr_number"]].drop_duplicates().shape[0]
        ),
        "qualified_review_prs": int(reviewed.sum()),
        "event_type_counts": event_counts,
        "transition_counts": transition_counts,
        "prior_week_ci_context": {
            "available": int(context_flag.sum()),
            "unavailable": int(context_flag.eq(0).sum()),
            "coverage": float(context_flag.mean()),
        },
        "rq2_output_hashes": {
            cohort_path.name: sha256(cohort_path),
            events_path.name: sha256(events_path),
            audit_path.name: sha256(audit_path),
        },
        "rq1_files_verified": int(len(manifest.get("files", {}))),
    }
    print("RQ2 PRE-MODEL VALIDATION")
    print(json.dumps(result, indent=2))
    return 0 if result["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())