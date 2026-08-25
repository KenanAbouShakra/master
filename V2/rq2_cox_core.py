from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


BASE_FEATURES = (
    "log_author_prior_pr_count",
    "author_newcomer",
    "draft",
    "log_repo_open_prs",
    "log_repo_pr_arrivals_28d",
    "log_repo_merges_28d",
)

CI_FEATURES = (
    "log_prior_week_ci_attempts",
    "prior_week_ci_failure_rate",
    "prior_week_ci_rerun_rate",
    "prior_week_ci_latency_log",
    "prior_week_ci_workflow_concentration_hhi",
)

TRANSITIONS = (
    "unreviewed_to_reviewed",
    "unreviewed_to_resolved",
    "reviewed_to_resolved",
)


def engineer_features(cohort: pd.DataFrame) -> pd.DataFrame:
    x = cohort.copy()
    required = {
        "author_prior_pr_count", "author_newcomer", "draft",
        "repo_open_prs_at_creation", "repo_pr_arrivals_28d", "repo_merges_28d",
        "prior_week_ci_attempts_total", "prior_week_ci_failure_rate",
        "prior_week_ci_rerun_rate", "prior_week_ci_latency_log",
        "prior_week_ci_workflow_concentration_hhi",
    }
    missing = sorted(required - set(x.columns))
    if missing:
        raise RuntimeError(f"Missing feature inputs: {missing}")
    x["log_author_prior_pr_count"] = np.log1p(x["author_prior_pr_count"].astype(float))
    x["log_repo_open_prs"] = np.log1p(x["repo_open_prs_at_creation"].astype(float))
    x["log_repo_pr_arrivals_28d"] = np.log1p(x["repo_pr_arrivals_28d"].astype(float))
    x["log_repo_merges_28d"] = np.log1p(x["repo_merges_28d"].astype(float))
    x["log_prior_week_ci_attempts"] = np.log1p(x["prior_week_ci_attempts_total"].astype(float))
    return x


def build_three_state_intervals(
    cohort: pd.DataFrame,
    administrative_cutoff: pd.Timestamp,
    minimum_interval_seconds: float = 1.0,
) -> pd.DataFrame:
    """Re-censor outcomes at cutoff and build the primary three-state process."""
    cutoff = pd.Timestamp(administrative_cutoff)
    if cutoff.tzinfo is None:
        raise RuntimeError("administrative_cutoff must include a timezone")
    cutoff = cutoff.tz_convert("UTC")
    rows: list[dict] = []
    for pr in cohort.itertuples(index=False):
        created = pd.Timestamp(pr.created_at_ts)
        if created >= cutoff:
            continue
        resolution = pd.Timestamp(pr.resolution_at) if pd.notna(pr.resolution_at) else pd.NaT
        resolution_observed = pd.notna(resolution) and resolution <= cutoff
        observation_end = resolution if resolution_observed else cutoff
        review = (
            pd.Timestamp(pr.first_qualified_review_at)
            if pd.notna(pr.first_qualified_review_at) else pd.NaT
        )
        review_observed = pd.notna(review) and review <= observation_end
        base = {"repo_full": pr.repo_full, "pr_number": int(pr.pr_number)}
        if review_observed:
            rows.append({
                **base, "from_state": 0, "actual_transition": "unreviewed_to_reviewed",
                "start_at": created, "stop_at": review, "event": 1,
            })
            rows.append({
                **base, "from_state": 1,
                "actual_transition": "reviewed_to_resolved" if resolution_observed else "censored",
                "start_at": review, "stop_at": observation_end,
                "event": int(resolution_observed),
            })
        else:
            rows.append({
                **base, "from_state": 0,
                "actual_transition": "unreviewed_to_resolved" if resolution_observed else "censored",
                "start_at": created, "stop_at": observation_end,
                "event": int(resolution_observed),
            })
    out = pd.DataFrame(rows)
    raw_seconds = (out["stop_at"] - out["start_at"]).dt.total_seconds()
    if (raw_seconds < 0).any():
        raise RuntimeError("Negative interval after administrative re-censoring")
    out["zero_duration_adjusted"] = raw_seconds.eq(0).astype(int)
    adjusted_seconds = raw_seconds.mask(raw_seconds.eq(0), minimum_interval_seconds)
    out["duration_hours"] = adjusted_seconds / 3600.0
    return out


def cause_specific_frame(
    intervals: pd.DataFrame,
    cohort_features: pd.DataFrame,
    transition: str,
) -> pd.DataFrame:
    if transition not in TRANSITIONS:
        raise ValueError(f"Unknown transition: {transition}")
    from_state = 0 if transition.startswith("unreviewed") else 1
    risk = intervals.loc[intervals["from_state"].eq(from_state)].copy()
    risk["status"] = risk["actual_transition"].eq(transition).astype(int)
    keys = ["repo_full", "pr_number"]
    feature_cols = list(BASE_FEATURES + CI_FEATURES) + ["prior_week_ci_context_available"]
    risk = risk.merge(
        cohort_features[keys + feature_cols], on=keys, how="left", validate="many_to_one"
    )
    return risk


@dataclass(frozen=True)
class Standardization:
    means: dict[str, float]
    scales: dict[str, float]

    @classmethod
    def fit(cls, frame: pd.DataFrame, columns: tuple[str, ...]) -> "Standardization":
        means, scales = {}, {}
        for column in columns:
            values = pd.to_numeric(frame[column], errors="coerce")
            if values.isna().any():
                raise RuntimeError(f"Training feature contains missing values: {column}")
            mean = float(values.mean())
            scale = float(values.std(ddof=0))
            if not np.isfinite(scale) or scale <= 1e-12:
                raise RuntimeError(f"Training feature is constant or invalid: {column}")
            means[column], scales[column] = mean, scale
        return cls(means, scales)

    def transform(self, frame: pd.DataFrame, columns: tuple[str, ...]) -> pd.DataFrame:
        out = pd.DataFrame(index=frame.index)
        for column in columns:
            values = pd.to_numeric(frame[column], errors="coerce")
            out[column] = (values - self.means[column]) / self.scales[column]
        return out


def harrell_c_index(duration, status, risk_score) -> float:
    """Harrell's C for right-censored data; higher risk means earlier event."""
    t = np.asarray(duration, dtype=float)
    e = np.asarray(status, dtype=int)
    r = np.asarray(risk_score, dtype=float)
    permissible = concordant = 0.0
    n = len(t)
    for i in range(n):
        if e[i] != 1:
            continue
        comparable = np.where(t > t[i])[0]
        if comparable.size == 0:
            continue
        permissible += float(comparable.size)
        concordant += float((r[i] > r[comparable]).sum())
        concordant += 0.5 * float((r[i] == r[comparable]).sum())
    return float(concordant / permissible) if permissible else float("nan")


def fit_phreg(
    train: pd.DataFrame,
    features: tuple[str, ...],
    regularization_alpha: float = 0.0,
):
    from statsmodels.duration.hazard_regression import PHReg

    design = train.loc[:, features].to_numpy(float)
    duration = train["duration_hours"].to_numpy(float)
    status = train["status"].to_numpy(int)
    if not np.isfinite(design).all():
        raise RuntimeError("Non-finite model matrix")
    if status.sum() < max(20, 5 * len(features)):
        raise RuntimeError(
            f"Insufficient events ({status.sum()}) for {len(features)} features"
        )
    model = PHReg(duration, design, status=status, ties="efron")
    if regularization_alpha > 0:
        result = model.fit_regularized(alpha=regularization_alpha, L1_wt=0.0)
    else:
        result = model.fit(disp=False)
    return model, result