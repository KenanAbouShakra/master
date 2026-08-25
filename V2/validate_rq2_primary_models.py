from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd


EXPECTED_INPUTS = {
    "rq2_pr_cohort.csv": "fbfe6a28ced42f11653609c9e196e0b5596429d7b7467c8c3277c3f17bc43663",
    "rq2_transition_events.csv": "96a66d0273570a63b63600a572d1358362c4dbe7caa8aa1f5ab07c3359aa6f76",
    "rq2_preparation_audit.json": "0afe13857ff574ba6d4c7faf5181fb3579a70811de59c87fae99e6aadd83baf5",
}
CI_FEATURES = {
    "log_prior_week_ci_attempts", "prior_week_ci_failure_rate",
    "prior_week_ci_rerun_rate", "prior_week_ci_latency_log",
    "prior_week_ci_workflow_concentration_hhi",
}


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def main() -> int:
    root = Path.cwd()
    model_dir = root / "analysis_outputs" / "rq2_primary_models"
    prep_dir = root / "analysis_outputs" / "rq2_preparation"
    estimates_path = model_dir / "rq2_cox_estimates.csv"
    performance_path = model_dir / "rq2_cox_performance.csv"
    metadata_path = model_dir / "rq2_cox_metadata.json"
    failures: dict[str, object] = {}

    for path in (estimates_path, performance_path, metadata_path):
        if not path.is_file(): failures[path.name] = "missing"
    for name, expected in EXPECTED_INPUTS.items():
        path = prep_dir / name
        if not path.is_file(): failures[name] = "missing"
        elif sha256(path) != expected: failures[name] = "hash mismatch"
    if failures:
        print(json.dumps({"status": "FAIL", "failures": failures}, indent=2))
        return 2

    estimates = pd.read_csv(estimates_path)
    performance = pd.read_csv(performance_path)
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    if len(estimates) != 51: failures["estimate_rows"] = len(estimates)
    if len(performance) != 18: failures["performance_rows"] = len(performance)
    if metadata.get("status") != "PASS": failures["metadata_status"] = metadata.get("status")
    if metadata.get("input_hashes") != EXPECTED_INPUTS:
        failures["metadata_input_hashes"] = "not equal to frozen inputs"

    numeric_estimate = [
        "coefficient", "standard_error", "hazard_ratio_per_training_sd",
        "ci95_low", "ci95_high",
    ]
    nonfinite_estimates = int((~np.isfinite(estimates[numeric_estimate].to_numpy(float))).sum())
    if nonfinite_estimates: failures["nonfinite_estimate_values"] = nonfinite_estimates
    invalid_se = int((estimates["standard_error"] < 0).sum())
    invalid_ci = int((
        (estimates["ci95_low"] <= 0)
        | (estimates["ci95_high"] <= 0)
        | (estimates["ci95_low"] > estimates["hazard_ratio_per_training_sd"])
        | (estimates["ci95_high"] < estimates["hazard_ratio_per_training_sd"])
    ).sum())
    if invalid_se: failures["negative_standard_errors"] = invalid_se
    if invalid_ci: failures["invalid_hazard_ratio_intervals"] = invalid_ci

    invalid_c = int((
        performance["harrell_c"].isna()
        | ~performance["harrell_c"].between(0, 1)
    ).sum())
    if invalid_c: failures["invalid_harrell_c"] = invalid_c

    expected_models = {"baseline", "ci_context"}
    expected_populations = {
        "development_training", "development_temporal_holdout", "external_evaluation"
    }
    expected_transitions = {
        "unreviewed_to_reviewed", "unreviewed_to_resolved", "reviewed_to_resolved"
    }
    if set(performance["model"]) != expected_models: failures["models"] = sorted(set(performance["model"]))
    if set(performance["population"]) != expected_populations: failures["populations"] = sorted(set(performance["population"]))
    if set(performance["transition"]) != expected_transitions: failures["transitions"] = sorted(set(performance["transition"]))

    paired = performance.pivot_table(
        index=["transition", "population"], columns="model",
        values=["n", "events", "harrell_c"], aggfunc="first"
    )
    unequal_n = int((paired[("n", "baseline")] != paired[("n", "ci_context")]).sum())
    unequal_events = int((paired[("events", "baseline")] != paired[("events", "ci_context")]).sum())
    if unequal_n: failures["unequal_comparison_n"] = unequal_n
    if unequal_events: failures["unequal_comparison_events"] = unequal_events

    lrt = metadata.get("model_metadata", {}).get("likelihood_ratio_tests", {})
    for transition in expected_transitions:
        item = lrt.get(transition)
        if not item:
            failures[f"lrt:{transition}"] = "missing"
        elif (
            not np.isfinite(float(item["statistic"]))
            or float(item["statistic"]) < 0
            or not 0 <= float(item["p_value"]) <= 1
            or int(item["degrees_of_freedom"]) != 5
        ):
            failures[f"lrt:{transition}"] = "invalid"

    display = performance.pivot_table(
        index=["transition", "population"], columns="model",
        values="harrell_c", aggfunc="first"
    ).reset_index()
    display["delta_ci_minus_baseline"] = display["ci_context"] - display["baseline"]
    ci_estimates = estimates[
        estimates["model"].eq("ci_context") & estimates["feature"].isin(CI_FEATURES)
    ][[
        "transition", "feature", "coefficient", "standard_error",
        "hazard_ratio_per_training_sd", "ci95_low", "ci95_high",
    ]].sort_values(["transition", "feature"])

    result = {
        "status": "PASS" if not failures else "FAIL",
        "failures": failures,
        "output_hashes": {
            estimates_path.name: sha256(estimates_path),
            performance_path.name: sha256(performance_path),
            metadata_path.name: sha256(metadata_path),
        },
        "estimate_rows": int(len(estimates)),
        "performance_rows": int(len(performance)),
    }
    print("RQ2 PRIMARY MODEL VALIDATION")
    print(json.dumps(result, indent=2))
    print("\nOUT-OF-SAMPLE DISCRIMINATION")
    print(display.to_string(index=False))
    print("\nCI-CONTEXT COEFFICIENTS")
    print(ci_estimates.to_string(index=False))
    print("\nTRAINING LIKELIHOOD-RATIO TESTS")
    print(json.dumps(lrt, indent=2))
    return 0 if result["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())