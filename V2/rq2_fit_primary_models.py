from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import chi2, norm

from rq2_cox_core import (
    BASE_FEATURES, CI_FEATURES, TRANSITIONS, Standardization,
    build_three_state_intervals, cause_specific_frame, engineer_features,
    fit_phreg, harrell_c_index,
)


EXPECTED_INPUT_HASHES = {
    "rq2_pr_cohort.csv": "fbfe6a28ced42f11653609c9e196e0b5596429d7b7467c8c3277c3f17bc43663",
    "rq2_transition_events.csv": "96a66d0273570a63b63600a572d1358362c4dbe7caa8aa1f5ab07c3359aa6f76",
    "rq2_preparation_audit.json": "0afe13857ff574ba6d4c7faf5181fb3579a70811de59c87fae99e6aadd83baf5",
}
DEVELOPMENT = {"docker/cli", "prometheus/prometheus", "tektoncd/pipeline"}
EXTERNAL = {"containerd/containerd", "helm/helm", "pytest-dev/pytest"}
TRAIN_CUTOFF = pd.Timestamp("2026-03-02T00:00:00Z")
HOLDOUT_START = pd.Timestamp("2026-03-30T00:00:00Z")
FINAL_CUTOFF = pd.Timestamp("2026-08-08T00:00:00Z")


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def load_inputs(root: Path) -> pd.DataFrame:
    rq2 = root / "analysis_outputs" / "rq2_preparation"
    failures = {}
    for name, expected in EXPECTED_INPUT_HASHES.items():
        path = rq2 / name
        if not path.is_file(): failures[name] = "missing"
        elif sha256(path) != expected: failures[name] = "hash mismatch"
    if failures:
        raise RuntimeError("RQ2 input verification failed: " + json.dumps(failures))
    cohort = pd.read_csv(rq2 / "rq2_pr_cohort.csv", low_memory=False)
    for column in ["created_at_ts", "resolution_at", "first_qualified_review_at"]:
        cohort[column] = pd.to_datetime(cohort[column], errors="coerce", utc=True)
    return engineer_features(cohort)


def populations(cohort: pd.DataFrame) -> dict[str, pd.DataFrame]:
    ci = cohort["prior_week_ci_context_available"].eq(1)
    created = cohort["created_at_ts"]
    repo = cohort["repo_full"]
    return {
        "development_training": cohort.loc[
            ci & repo.isin(DEVELOPMENT) & created.lt(TRAIN_CUTOFF)
        ].copy(),
        "development_temporal_holdout": cohort.loc[
            ci & repo.isin(DEVELOPMENT) & created.ge(HOLDOUT_START)
        ].copy(),
        "external_evaluation": cohort.loc[
            ci & repo.isin(EXTERNAL)
        ].copy(),
    }


def risk_sets(cohort: pd.DataFrame) -> dict[str, dict[str, pd.DataFrame]]:
    pops = populations(cohort)
    intervals = {
        "development_training": build_three_state_intervals(
            pops["development_training"], TRAIN_CUTOFF
        ),
        "development_temporal_holdout": build_three_state_intervals(
            pops["development_temporal_holdout"], FINAL_CUTOFF
        ),
        "external_evaluation": build_three_state_intervals(
            pops["external_evaluation"], FINAL_CUTOFF
        ),
    }
    return {
        population: {
            transition: cause_specific_frame(table, pops[population], transition)
            for transition in TRANSITIONS
        }
        for population, table in intervals.items()
    }


def matrix_audit(frame: pd.DataFrame, features: tuple[str, ...]) -> dict:
    x = frame.loc[:, features].apply(pd.to_numeric, errors="coerce")
    complete = x.notna().all(axis=1)
    if complete.all():
        z = (x - x.mean()) / x.std(ddof=0)
        matrix = z.to_numpy(float)
        rank = int(np.linalg.matrix_rank(matrix))
        condition = float(np.linalg.cond(matrix))
    else:
        rank, condition = None, None
    return {
        "rows": int(len(frame)),
        "events": int(frame["status"].sum()),
        "missing_rows": int((~complete).sum()),
        "features": len(features),
        "matrix_rank": rank,
        "condition_number": condition,
        "zero_duration_adjustments": int(frame["zero_duration_adjusted"].sum()),
    }


def preflight(risks: dict[str, dict[str, pd.DataFrame]]) -> dict:
    checks = {}
    failures = {}
    all_features = BASE_FEATURES + CI_FEATURES
    for transition in TRANSITIONS:
        checks[transition] = {}
        for population in risks:
            frame = risks[population][transition]
            entry = {
                "baseline": matrix_audit(frame, BASE_FEATURES),
                "ci_context": matrix_audit(frame, all_features),
            }
            checks[transition][population] = entry
            for model, audit in entry.items():
                key = f"{transition}:{population}:{model}"
                if audit["missing_rows"]:
                    failures[key] = "missing model values"
                if audit["matrix_rank"] != audit["features"]:
                    failures[key] = "rank deficient"
                if audit["condition_number"] is None or audit["condition_number"] > 100:
                    failures[key] = f"condition number {audit['condition_number']}"
                if population == "development_training" and audit["events"] < 5 * audit["features"]:
                    failures[key] = "insufficient training events"
    return {
        "status": "PASS" if not failures else "FAIL",
        "failures": failures,
        "contract": {
            "primary_states": ["unreviewed", "reviewed", "resolved"],
            "transitions": list(TRANSITIONS),
            "training_outcomes_recensored_at": TRAIN_CUTOFF.isoformat(),
            "holdout_created_from": HOLDOUT_START.isoformat(),
            "final_observation_cutoff": FINAL_CUTOFF.isoformat(),
            "comparison_population": "prior-week-CI-eligible rows only",
            "ties": "Efron; exact zero-duration intervals receive one second and are flagged",
            "claim": "prediction and conditional association, not causation",
        },
        "checks": checks,
    }


def fit_all(risks: dict[str, dict[str, pd.DataFrame]]) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    estimates, performance, metadata = [], [], {"scalers": {}, "likelihood_ratio_tests": {}}
    full_features = BASE_FEATURES + CI_FEATURES
    for transition in TRANSITIONS:
        train = risks["development_training"][transition].copy()
        scaler = Standardization.fit(train, full_features)
        metadata["scalers"][transition] = {
            "means": scaler.means, "scales": scaler.scales,
        }
        fitted = {}
        for model_name, features in (("baseline", BASE_FEATURES), ("ci_context", full_features)):
            train_model = train.copy()
            standardized = scaler.transform(train_model, features)
            for column in features:
                train_model[column] = standardized[column].astype(float)
            model, result = fit_phreg(train_model, features)
            params = np.asarray(result.params, dtype=float)
            bse = np.asarray(result.bse, dtype=float)
            fitted[model_name] = {"model": model, "result": result, "features": features}
            for feature, beta, se in zip(features, params, bse):
                estimates.append({
                    "transition": transition, "model": model_name, "feature": feature,
                    "coefficient": float(beta), "standard_error": float(se),
                    "hazard_ratio_per_training_sd": float(np.exp(beta)),
                    "ci95_low": float(np.exp(beta - norm.ppf(.975) * se)),
                    "ci95_high": float(np.exp(beta + norm.ppf(.975) * se)),
                })
            for population in ("development_training", "development_temporal_holdout", "external_evaluation"):
                frame = risks[population][transition].copy()
                z = scaler.transform(frame, features)
                risk_score = z.to_numpy(float) @ params
                performance.append({
                    "transition": transition, "model": model_name, "population": population,
                    "n": int(len(frame)), "events": int(frame["status"].sum()),
                    "harrell_c": harrell_c_index(
                        frame["duration_hours"], frame["status"], risk_score
                    ),
                })
        base = fitted["baseline"]["result"]
        full = fitted["ci_context"]["result"]
        statistic = max(0.0, 2.0 * (float(full.llf) - float(base.llf)))
        df = len(CI_FEATURES)
        metadata["likelihood_ratio_tests"][transition] = {
            "statistic": statistic, "degrees_of_freedom": df,
            "p_value": float(chi2.sf(statistic, df)),
            "interpretation": "training incremental fit only; external predictive performance is primary",
        }
    return pd.DataFrame(estimates), pd.DataFrame(performance), metadata


def main() -> int:
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--preflight-only", action="store_true")
    mode.add_argument("--confirm-fit", action="store_true")
    parser.add_argument("--output-dir", default="analysis_outputs/rq2_primary_models")
    args = parser.parse_args()
    root = Path.cwd()
    cohort = load_inputs(root)
    risks = risk_sets(cohort)
    report = preflight(risks)
    print("RQ2 PRIMARY MODEL PREFLIGHT")
    print(json.dumps(report, indent=2))
    if report["status"] != "PASS":
        return 2
    if args.preflight_only:
        return 0
    output = root / args.output_dir
    if output.exists():
        raise RuntimeError(f"Refusing to overwrite existing model directory: {output}")
    estimates, performance, metadata = fit_all(risks)
    output.mkdir(parents=True, exist_ok=False)
    estimates.to_csv(output / "rq2_cox_estimates.csv", index=False)
    performance.to_csv(output / "rq2_cox_performance.csv", index=False)
    payload = {
        "status": "PASS", "preflight": report, "model_metadata": metadata,
        "input_hashes": EXPECTED_INPUT_HASHES,
    }
    (output / "rq2_cox_metadata.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print("RQ2 PRIMARY MODEL FIT")
    print(json.dumps({
        "status": "PASS", "estimate_rows": int(len(estimates)),
        "performance_rows": int(len(performance)),
        "output_directory": str(output),
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())