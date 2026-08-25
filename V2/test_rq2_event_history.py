from __future__ import annotations

import pandas as pd

from rq2_build_event_history import prepare_pr_cohort


def ts(value: str) -> pd.Timestamp:
    return pd.Timestamp(value, tz="UTC")


def synthetic_inputs():
    prs = pd.DataFrame([
        # Reviewed then merged.
        {"repo_full":"r/a","pr_number":1,"created_at":"2026-01-01T00:00:00Z","closed_at":"2026-01-04T00:00:00Z","merged_at":"2026-01-04T00:00:00Z","author_login":"alice","author_type":"User","draft":0},
        # Merged without a qualified human review.
        {"repo_full":"r/a","pr_number":2,"created_at":"2026-01-02T00:00:00Z","closed_at":"2026-01-03T00:00:00Z","merged_at":"2026-01-03T00:00:00Z","author_login":"bob","author_type":"User","draft":0},
        # Reviewed then closed without merge.
        {"repo_full":"r/a","pr_number":3,"created_at":"2026-01-05T00:00:00Z","closed_at":"2026-01-10T00:00:00Z","merged_at":None,"author_login":"alice","author_type":"User","draft":0},
        # Still open at censoring.
        {"repo_full":"r/a","pr_number":4,"created_at":"2026-01-06T00:00:00Z","closed_at":None,"merged_at":None,"author_login":"carol","author_type":"User","draft":0},
    ])
    reviews = pd.DataFrame([
        {"repo_full":"r/a","pr_number":1,"review_id":11,"user_login":"reviewer","user_type":"User","state":"APPROVED","submitted_at":"2026-01-02T00:00:00Z"},
        # Bot must not qualify PR 2.
        {"repo_full":"r/a","pr_number":2,"review_id":12,"user_login":"ci-bot[bot]","user_type":"Bot","state":"APPROVED","submitted_at":"2026-01-02T12:00:00Z"},
        {"repo_full":"r/a","pr_number":3,"review_id":13,"user_login":"reviewer","user_type":"User","state":"CHANGES_REQUESTED","submitted_at":"2026-01-06T00:00:00Z"},
        # Review after closure must not qualify.
        {"repo_full":"r/a","pr_number":3,"review_id":14,"user_login":"late","user_type":"User","state":"APPROVED","submitted_at":"2026-01-11T00:00:00Z"},
    ])
    weekly = pd.DataFrame([
        {"repo_full":"r/a","week":"2025-12-22T00:00:00Z","attempts_total":10,"failure_rate":0.1,"rerun_rate":0.0,"latency_log":1.0},
        {"repo_full":"r/a","week":"2025-12-29T00:00:00Z","attempts_total":20,"failure_rate":0.2,"rerun_rate":0.1,"latency_log":1.2},
    ])
    return prs, reviews, weekly


def main() -> int:
    prs, reviews, weekly = synthetic_inputs()
    cohort, events = prepare_pr_cohort(prs, reviews, weekly, ts("2026-01-15T00:00:00"))
    by_pr = cohort.set_index("pr_number")
    assert by_pr.loc[1, "event_type"] == "merged"
    assert by_pr.loc[1, "received_qualified_review"] == 1
    assert by_pr.loc[2, "received_qualified_review"] == 0
    assert by_pr.loc[3, "event_type"] == "closed_unmerged"
    assert by_pr.loc[3, "first_qualified_review_at"] == ts("2026-01-06T00:00:00")
    assert by_pr.loc[4, "event_type"] == "censored"
    assert by_pr.loc[4, "observation_end_at"] == ts("2026-01-15T00:00:00")
    assert by_pr.loc[3, "author_prior_pr_count"] == 1
    # PR 1 was resolved before PR 3 creation and is a known prior merge.
    assert by_pr.loc[3, "author_prior_resolved_count"] == 1
    assert by_pr.loc[3, "author_prior_merged_count"] == 1

    transitions = events.groupby("pr_number")["transition"].apply(list).to_dict()
    assert transitions[1] == ["0->1", "1->2"]
    assert transitions[2] == ["0->2"]
    assert transitions[3] == ["0->1", "1->3"]
    assert transitions[4] == ["0->censored"]
    assert (events["duration_hours"] >= 0).all()
    assert len(cohort) == 4 and len(events) == 6
    print("RQ2 EVENT-HISTORY SYNTHETIC TESTS")
    print("RESULT: PASS (14/14)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())