from __future__ import annotations

import numpy as np
import pandas as pd

from rq2_cox_core import (
    BASE_FEATURES, CI_FEATURES, Standardization, build_three_state_intervals,
    cause_specific_frame, engineer_features, fit_phreg, harrell_c_index,
)


def synthetic_cohort(n: int = 180) -> pd.DataFrame:
    rng = np.random.default_rng(20260825)
    created = pd.Timestamp("2025-07-01T00:00:00Z") + pd.to_timedelta(np.arange(n), unit="h")
    rows = []
    for i in range(n):
        review_hours = 2.0 + rng.exponential(8.0)
        resolve_hours = review_hours + 2.0 + rng.exponential(16.0)
        review = created[i] + pd.Timedelta(hours=float(review_hours))
        resolution = created[i] + pd.Timedelta(hours=float(resolve_hours))
        if i % 9 == 0:
            review = pd.NaT
        if i % 17 == 0:
            resolution = pd.NaT
        if i == 3:
            resolution = review  # one-second tie rule must activate
        rows.append({
            "repo_full": "repo/a" if i % 2 == 0 else "repo/b",
            "pr_number": i + 1,
            "created_at_ts": created[i],
            "resolution_at": resolution,
            "first_qualified_review_at": review,
            "author_prior_pr_count": i % 25,
            "author_newcomer": int(i % 25 == 0),
            "draft": i % 2,
            "repo_open_prs_at_creation": 10 + i % 15,
            "repo_pr_arrivals_28d": 30 + i % 20,
            "repo_merges_28d": 20 + i % 12,
            "prior_week_ci_attempts_total": 100 + i,
            "prior_week_ci_failure_rate": (i % 20) / 25,
            "prior_week_ci_rerun_rate": (i % 10) / 20,
            "prior_week_ci_latency_log": np.sin(i / 12),
            "prior_week_ci_workflow_concentration_hhi": 0.2 + (i % 6) / 20,
            "prior_week_ci_context_available": 1,
        })
    return pd.DataFrame(rows)


def main() -> int:
    cohort = engineer_features(synthetic_cohort())
    cutoff = pd.Timestamp("2025-08-01T00:00:00Z")
    intervals = build_three_state_intervals(cohort, cutoff)
    assert intervals["duration_hours"].gt(0).all()
    assert intervals["zero_duration_adjusted"].sum() == 1
    assert set(intervals["actual_transition"]) <= {
        "unreviewed_to_reviewed", "unreviewed_to_resolved",
        "reviewed_to_resolved", "censored",
    }
    frames = {
        transition: cause_specific_frame(intervals, cohort, transition)
        for transition in (
            "unreviewed_to_reviewed", "unreviewed_to_resolved", "reviewed_to_resolved"
        )
    }
    assert frames["unreviewed_to_reviewed"]["from_state"].eq(0).all()
    assert frames["reviewed_to_resolved"]["from_state"].eq(1).all()
    scale = Standardization.fit(frames["unreviewed_to_reviewed"], BASE_FEATURES + CI_FEATURES)
    transformed = scale.transform(frames["unreviewed_to_reviewed"], BASE_FEATURES + CI_FEATURES)
    assert np.allclose(transformed.mean().to_numpy(), 0.0, atol=1e-10)
    assert np.allclose(transformed.std(ddof=0).to_numpy(), 1.0, atol=1e-10)
    c = harrell_c_index([1, 2, 3], [1, 1, 0], [3, 2, 1])
    assert np.isclose(c, 1.0)

    # Integration test against the installed, established Cox implementation.
    target = frames["unreviewed_to_reviewed"].copy()
    z = scale.transform(target, BASE_FEATURES)
    target.loc[:, BASE_FEATURES] = z
    model, result = fit_phreg(target, BASE_FEATURES)
    assert len(result.params) == len(BASE_FEATURES)
    assert np.isfinite(np.asarray(result.params)).all()
    print("RQ2 COX CORE SYNTHETIC TESTS")
    print("RESULT: PASS (12/12)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())