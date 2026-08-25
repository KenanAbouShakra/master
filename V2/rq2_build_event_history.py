from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
import yaml


RQ1_MANIFEST_SHA256 = "d871b9c08870c57486985e601d448466c0baecbbd505679dce767ddf385bf55d"
DEFAULT_REVIEW_STATES = ("APPROVED", "CHANGES_REQUESTED", "COMMENTED")
KEY = ["repo_full", "pr_number"]


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def utc(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce", utc=True)


def monday(series: pd.Series) -> pd.Series:
    normalized = series.dt.normalize()
    return normalized - pd.to_timedelta(normalized.dt.weekday, unit="D")


def require_columns(frame: pd.DataFrame, required: set[str], label: str) -> None:
    missing = sorted(required - set(frame.columns))
    if missing:
        raise RuntimeError(f"{label} is missing required columns: {missing}")


def verify_rq1_freeze(root: Path) -> dict:
    manifest_path = root / "analysis_outputs" / "RQ1_BASELINES_FROZEN.json"
    if not manifest_path.is_file():
        raise RuntimeError("RQ1_BASELINES_FROZEN.json is missing; RQ2 remains blocked.")
    actual = sha256(manifest_path)
    if actual != RQ1_MANIFEST_SHA256:
        raise RuntimeError(f"Unexpected RQ1 manifest hash: {actual}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("status") != "FROZEN":
        raise RuntimeError("RQ1 manifest status is not FROZEN.")
    failures = {}
    for relative, record in manifest.get("files", {}).items():
        path = root / relative
        if not path.is_file():
            failures[relative] = "missing"
        elif path.stat().st_size != record["bytes"]:
            failures[relative] = "size mismatch"
        elif sha256(path) != record["sha256"]:
            failures[relative] = "hash mismatch"
    if failures:
        raise RuntimeError("RQ1 freeze verification failed: " + json.dumps(failures))
    prohibited = [
        "hmm_results.csv", "hmm_model.json", "hmm_start_diagnostics.csv",
        "hmm_split_summary.csv",
    ]
    present = [name for name in prohibited if (root / "analysis_outputs" / name).exists()]
    if present:
        raise RuntimeError(f"Prohibited HMM outputs are present: {present}")
    return {"manifest_sha256": actual, "files_verified": len(manifest.get("files", {}))}


def qualified_reviews(
    reviews: pd.DataFrame,
    prs: pd.DataFrame,
    allowed_states: tuple[str, ...],
) -> pd.DataFrame:
    require_columns(
        reviews,
        {"repo_full", "pr_number", "review_id", "user_login", "user_type", "state", "submitted_at"},
        "pr_reviews.csv",
    )
    r = reviews.copy()
    r["submitted_at"] = utc(r["submitted_at"])
    r["state_normalized"] = r["state"].fillna("").astype(str).str.upper()
    login = r["user_login"].fillna("").astype(str).str.lower()
    r["is_bot"] = (
        r["user_type"].fillna("").astype(str).str.lower().eq("bot")
        | login.str.contains(r"\[bot\]$|bot$", regex=True)
    )
    authors = prs[KEY + ["author_login", "created_at_ts", "observation_end_at"]].copy()
    r = r.merge(authors, on=KEY, how="inner", validate="many_to_one")
    r["is_self_review"] = login.eq(r["author_login"].fillna("").astype(str).str.lower())
    r["is_qualified"] = (
        r["state_normalized"].isin(allowed_states)
        & ~r["is_bot"]
        & ~r["is_self_review"]
        & r["submitted_at"].notna()
        & (r["submitted_at"] >= r["created_at_ts"])
    )
    return r.loc[r["is_qualified"]].copy()


def add_contributor_history(prs: pd.DataFrame) -> pd.DataFrame:
    """Creation-time history only; never uses outcomes unavailable at creation."""
    x = prs.sort_values(["repo_full", "created_at_ts", "pr_number"]).copy()
    x["author_login_key"] = x["author_login"].fillna("<missing>").astype(str).str.lower()
    group = x.groupby(["repo_full", "author_login_key"], sort=False)
    x["author_prior_pr_count"] = group.cumcount().astype(int)

    # A prior outcome counts only if it occurred before the focal PR was created.
    prior_merged = np.zeros(len(x), dtype=int)
    prior_resolved = np.zeros(len(x), dtype=int)
    for _, positions in x.groupby(["repo_full", "author_login_key"], sort=False).indices.items():
        pos = np.asarray(positions, dtype=int)
        created = x.iloc[pos]["created_at_ts"].astype("int64").to_numpy()
        resolved = x.iloc[pos]["resolution_at"].astype("int64").to_numpy()
        merged = x.iloc[pos]["event_type"].eq("merged").to_numpy()
        for local_i, global_i in enumerate(pos):
            earlier_resolution = resolved[:local_i]
            known = earlier_resolution < created[local_i]
            prior_resolved[global_i] = int(known.sum())
            prior_merged[global_i] = int((known & merged[:local_i]).sum())
    x["author_prior_resolved_count"] = prior_resolved
    x["author_prior_merged_count"] = prior_merged
    rate = np.full(len(x), np.nan, dtype=float)
    np.divide(prior_merged, prior_resolved, out=rate, where=prior_resolved > 0)
    x["author_prior_merge_rate"] = rate
    x["author_newcomer"] = x["author_prior_pr_count"].eq(0).astype(int)
    return x


def add_repository_workload(prs: pd.DataFrame, window_days: int = 28) -> pd.DataFrame:
    x = prs.sort_values(["repo_full", "created_at_ts", "pr_number"]).copy()
    x["repo_open_prs_at_creation"] = 0
    x[f"repo_pr_arrivals_{window_days}d"] = 0
    x[f"repo_merges_{window_days}d"] = 0
    delta = pd.Timedelta(days=window_days)
    for _, idx in x.groupby("repo_full", sort=False).groups.items():
        idx = list(idx)
        g = x.loc[idx]
        created = g["created_at_ts"].astype("int64").to_numpy()
        resolved = g["resolution_at"].astype("int64").to_numpy()
        nat_value = pd.NaT.value
        merged_times = g.loc[g["event_type"].eq("merged"), "resolution_at"].dropna().sort_values().astype("int64").to_numpy()
        delta_ns = int(pd.Timedelta(days=window_days).value)
        for row_idx, t in zip(idx, created):
            previous_created = created < t
            still_open = previous_created & ((resolved == nat_value) | (resolved >= t))
            x.at[row_idx, "repo_open_prs_at_creation"] = int(still_open.sum())
            x.at[row_idx, f"repo_pr_arrivals_{window_days}d"] = int(
                ((created < t) & (created >= t - delta_ns)).sum()
            )
            x.at[row_idx, f"repo_merges_{window_days}d"] = int(
                ((merged_times < t) & (merged_times >= t - delta_ns)).sum()
            )
    return x


def join_prior_week_ci(prs: pd.DataFrame, weekly: pd.DataFrame) -> pd.DataFrame:
    require_columns(weekly, {"repo_full", "week"}, "repository_week_panel.csv")
    w = weekly.copy()
    w["ci_context_week"] = utc(w["week"])
    w = w.drop(columns=["week"])
    w["prior_week_ci_context_available"] = 1
    rename = {
        c: f"prior_week_ci_{c}"
        for c in w.columns
        if c not in {"repo_full", "ci_context_week", "prior_week_ci_context_available"}
    }
    w = w.rename(columns=rename)
    x = prs.copy()
    x["ci_context_week"] = monday(x["created_at_ts"]) - pd.Timedelta(days=7)
    x = x.merge(w, on=["repo_full", "ci_context_week"], how="left", validate="many_to_one")
    x["prior_week_ci_context_available"] = (
        x["prior_week_ci_context_available"].fillna(0).astype(int)
    )
    return x


def prepare_pr_cohort(
    prs: pd.DataFrame,
    reviews: pd.DataFrame,
    weekly: pd.DataFrame,
    censor_at: pd.Timestamp,
    allowed_review_states: tuple[str, ...] = DEFAULT_REVIEW_STATES,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    require_columns(
        prs,
        {"repo_full", "pr_number", "created_at", "closed_at", "merged_at", "author_login", "author_type", "draft"},
        "pull_requests.csv",
    )
    x = prs.copy()
    if x.duplicated(KEY).any():
        raise RuntimeError("pull_requests.csv contains duplicate repository/PR keys.")
    x["created_at_ts"] = utc(x["created_at"])
    x["closed_at_ts"] = utc(x["closed_at"])
    x["merged_at_ts"] = utc(x["merged_at"])
    if x["created_at_ts"].isna().any():
        raise RuntimeError(f"Invalid PR created_at values: {int(x['created_at_ts'].isna().sum())}")
    if (x["created_at_ts"] >= censor_at).any():
        raise RuntimeError("At least one PR was created at or after the administrative censor time.")

    merged = x["merged_at_ts"].notna()
    x["event_type"] = np.where(merged, "merged", np.where(x["closed_at_ts"].notna(), "closed_unmerged", "censored"))
    x["resolution_at"] = x["merged_at_ts"].where(merged, x["closed_at_ts"])
    invalid_resolution = x["resolution_at"].notna() & (x["resolution_at"] < x["created_at_ts"])
    if invalid_resolution.any():
        raise RuntimeError(f"Resolution precedes creation for {int(invalid_resolution.sum())} PRs.")
    after_cutoff = x["resolution_at"].notna() & (x["resolution_at"] > censor_at)
    x.loc[after_cutoff, "resolution_at"] = pd.NaT
    x.loc[after_cutoff, "event_type"] = "censored"
    x["observation_end_at"] = x["resolution_at"].fillna(censor_at)

    q = qualified_reviews(reviews, x, allowed_review_states)
    q = q[q["submitted_at"] <= q["observation_end_at"]]
    first = q.groupby(KEY, as_index=False)["submitted_at"].min().rename(columns={"submitted_at": "first_qualified_review_at"})
    x = x.merge(first, on=KEY, how="left", validate="one_to_one")
    x["received_qualified_review"] = x["first_qualified_review_at"].notna().astype(int)

    x = add_contributor_history(x)
    x = add_repository_workload(x)
    x = join_prior_week_ci(x, weekly)

    x["time_to_first_review_hours"] = (
        (x["first_qualified_review_at"] - x["created_at_ts"]).dt.total_seconds() / 3600
    )
    x["time_to_resolution_hours"] = (
        (x["observation_end_at"] - x["created_at_ts"]).dt.total_seconds() / 3600
    )
    x["resolution_observed"] = x["event_type"].ne("censored").astype(int)

    events = build_transition_events(x)
    return x, events


def build_transition_events(cohort: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict] = []
    for pr in cohort.itertuples(index=False):
        base = {"repo_full": pr.repo_full, "pr_number": int(pr.pr_number)}
        created = pr.created_at_ts
        end = pr.observation_end_at
        review = pr.first_qualified_review_at
        if pd.notna(review):
            rows.append({**base, "from_state": 0, "to_state": 1, "start_at": created, "stop_at": review, "event": 1})
            if pr.event_type == "merged":
                rows.append({**base, "from_state": 1, "to_state": 2, "start_at": review, "stop_at": end, "event": 1})
            elif pr.event_type == "closed_unmerged":
                rows.append({**base, "from_state": 1, "to_state": 3, "start_at": review, "stop_at": end, "event": 1})
            else:
                rows.append({**base, "from_state": 1, "to_state": pd.NA, "start_at": review, "stop_at": end, "event": 0})
        else:
            if pr.event_type == "merged": to_state, event = 2, 1
            elif pr.event_type == "closed_unmerged": to_state, event = 3, 1
            else: to_state, event = pd.NA, 0
            rows.append({**base, "from_state": 0, "to_state": to_state, "start_at": created, "stop_at": end, "event": event})
    out = pd.DataFrame(rows)
    out["duration_hours"] = (out["stop_at"] - out["start_at"]).dt.total_seconds() / 3600
    if (out["duration_hours"] < 0).any():
        raise RuntimeError("Negative transition duration detected.")
    out["transition"] = (
        out["from_state"].astype(str) + "->" + out["to_state"].astype("Int64").astype(str)
    ).where(out["event"].eq(1), out["from_state"].astype(str) + "->censored")
    return out


def audit(cohort: pd.DataFrame, events: pd.DataFrame, freeze: dict, censor_at: pd.Timestamp) -> dict:
    per_repo = []
    for repo, g in cohort.groupby("repo_full", sort=True):
        per_repo.append({
            "repo_full": repo,
            "pull_requests": int(len(g)),
            "merged": int(g["event_type"].eq("merged").sum()),
            "closed_unmerged": int(g["event_type"].eq("closed_unmerged").sum()),
            "right_censored": int(g["event_type"].eq("censored").sum()),
            "qualified_review_observed": int(g["received_qualified_review"].sum()),
            "prior_week_ci_context_rows": int(g["prior_week_ci_context_available"].sum()),
            "prior_week_ci_context_coverage": float(g["prior_week_ci_context_available"].mean()),
        })
    return {
        "status": "PASS",
        "claim_scope": "prediction and conditional association; not causation",
        "rq1_freeze": freeze,
        "administrative_censor_at": censor_at.isoformat(),
        "review_state_contract": list(DEFAULT_REVIEW_STATES),
        "state_contract": {
            "0": "open_without_qualified_human_review",
            "1": "qualified_human_review_received",
            "2": "merged_absorbing",
            "3": "closed_unmerged_absorbing",
        },
        "pull_requests": int(len(cohort)),
        "transition_rows": int(len(events)),
        "prior_week_ci_context_rows": int(cohort["prior_week_ci_context_available"].sum()),
        "prior_week_ci_context_coverage": float(cohort["prior_week_ci_context_available"].mean()),
        "event_type_counts": {str(k): int(v) for k, v in cohort["event_type"].value_counts().items()},
        "transition_counts": {str(k): int(v) for k, v in events["transition"].value_counts().items()},
        "per_repository": per_repo,
        "leakage_declarations": {
            "final_additions_deletions_changed_files_commits": "retained for descriptive/sensitivity use only; excluded from the creation-time primary model unless timestamped history proves availability",
            "contributor_history": "uses only PR information observable before focal PR creation",
            "repository_workload": "uses only earlier creations and resolutions",
            "ci_context": "latest fully completed prior repository week",
            "direct_pr_ci": "not yet included; secondary time-varying linked-subset stage",
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="analysis_config.yaml")
    parser.add_argument("--exports-dir", default=None)
    parser.add_argument("--output-dir", default="analysis_outputs/rq2_preparation")
    parser.add_argument(
        "--censor-at",
        required=True,
        help="Exclusive administrative observation cutoff, e.g. 2026-08-08T00:00:00Z",
    )
    parser.add_argument("--confirm-write", action="store_true")
    args = parser.parse_args()
    if not args.confirm_write:
        raise RuntimeError("Refusing to write. Re-run only after review with --confirm-write.")

    root = Path.cwd()
    cfg = yaml.safe_load(Path(args.config).read_text(encoding="utf-8-sig"))
    exports = root / (args.exports_dir or cfg.get("storage", {}).get("export_dir", "data/exports"))
    out = root / args.output_dir
    if out.exists() and any(out.iterdir()):
        raise RuntimeError(f"Output directory is not empty: {out}")

    freeze = verify_rq1_freeze(root)
    prs = pd.read_csv(exports / "pull_requests.csv", low_memory=False)
    reviews = pd.read_csv(exports / "pr_reviews.csv", low_memory=False)
    weekly = pd.read_csv(root / "analysis_outputs" / "repository_week_panel.csv", low_memory=False)
    censor_at = pd.Timestamp(args.censor_at)
    if censor_at.tzinfo is None:
        raise RuntimeError("--censor-at must include an explicit timezone, preferably Z/UTC.")
    censor_at = censor_at.tz_convert("UTC")
    states = tuple(cfg.get("measurement", {}).get("qualified_review_states", DEFAULT_REVIEW_STATES))

    cohort, events = prepare_pr_cohort(prs, reviews, weekly, censor_at, states)
    report = audit(cohort, events, freeze, censor_at)
    report["review_state_contract"] = list(states)

    out.mkdir(parents=True, exist_ok=False)
    cohort.to_csv(out / "rq2_pr_cohort.csv", index=False)
    events.to_csv(out / "rq2_transition_events.csv", index=False)
    (out / "rq2_preparation_audit.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    print("RQ2 EVENT-HISTORY PREPARATION")
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())