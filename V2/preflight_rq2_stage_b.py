from __future__ import annotations

import gzip
import hashlib
import json
from pathlib import Path

import pandas as pd
import yaml


CORE_METRICS = ("latency_log", "failure_rate", "rerun_rate")
EXPECTED_STAGE_A_DETECTOR_COLUMNS = {
    "scenario_id", "repo_full", "detector_family", "detector_variant",
    "metric", "scenario_type", "affected_metrics", "magnitude_level",
    "duration_weeks", "volume_condition", "missingness_condition",
    "repetition", "precision", "recall", "false_alarm_rate",
    "episode_detection_rate", "detection_delay_weeks",
    "boundary_overlap", "alarm_duration_weeks", "unevaluable_fraction",
}


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def stable_seed(base_seed: int, *parts: object) -> int:
    text = "|".join([str(base_seed), *(str(part) for part in parts)])
    return int.from_bytes(hashlib.sha256(text.encode("utf-8")).digest()[:8], "big")


def load_yaml(path: Path) -> dict:
    value = yaml.safe_load(path.read_text(encoding="utf-8-sig"))
    if not isinstance(value, dict):
        raise RuntimeError(f"YAML root is not a mapping: {path}")
    return value


def verify_manifest(root: Path, path: Path) -> tuple[dict, dict[str, str]]:
    manifest = json.loads(path.read_text(encoding="utf-8"))
    failures: dict[str, str] = {}
    for relative, record in manifest.get("files", {}).items():
        target = root / relative
        if not target.is_file():
            failures[relative] = "missing"
        elif target.stat().st_size != int(record["bytes"]):
            failures[relative] = "size mismatch"
        elif sha256(target) != str(record["sha256"]).lower():
            failures[relative] = "hash mismatch"
    return manifest, failures


def valid_starts(frame: pd.DataFrame, duration: int, minimum_prior: int) -> list[int]:
    eligible = frame["external_evaluation_eligible"].fillna(False).astype(bool).to_numpy()
    observed = pd.to_numeric(frame["attempts_total"], errors="coerce").fillna(0).gt(0).to_numpy()
    starts = []
    for start in range(len(frame) - duration + 1):
        if int(observed[:start].sum()) < minimum_prior:
            continue
        if bool(eligible[start:start + duration].all()):
            starts.append(start)
    return starts


def main() -> int:
    root = Path.cwd()
    config_path = root / "stage_b_confirmatory_config.yaml"
    contract_path = root / "RQ2_Stage_B_Confirmatory_Contract.md"
    failures: dict[str, object] = {}

    if not config_path.is_file():
        failures[config_path.name] = "missing"
    if not contract_path.is_file():
        failures[contract_path.name] = "missing"
    if failures:
        print("RQ2 STAGE-B PREFLIGHT")
        print(json.dumps({"status": "FAIL", "failures": failures}, indent=2))
        return 2

    cfg = load_yaml(config_path)
    if cfg.get("protocol_status") != "draft_not_authorized_for_execution":
        failures["protocol_status"] = "must remain draft_not_authorized_for_execution"

    approved = cfg.get("approved_frozen_inputs", {})
    required_approved = {
        "stage_a_manifest", "rq1_manifest", "analysis_config",
        "baseline_source", "baseline_metadata", "baseline_results",
        "stage_a_evaluator_source",
    }
    missing_approved = sorted(required_approved - set(approved))
    if missing_approved:
        failures["approved_frozen_inputs"] = {"missing": missing_approved}

    input_checks = {}
    for label, item in approved.items():
        path = root / item["path"]
        actual = sha256(path) if path.is_file() else None
        expected = str(item["sha256"]).lower()
        input_checks[label] = {
            "path": item["path"],
            "exists": path.is_file(),
            "expected_sha256": expected,
            "actual_sha256": actual,
            "matches": actual == expected,
        }
        if actual != expected:
            failures[f"input:{label}"] = "missing or hash mismatch"

    stage_manifest_path = root / approved["stage_a_manifest"]["path"]
    rq1_manifest_path = root / approved["rq1_manifest"]["path"]
    stage_manifest, stage_freeze_failures = verify_manifest(root, stage_manifest_path)
    rq1_manifest, rq1_freeze_failures = verify_manifest(root, rq1_manifest_path)
    if stage_freeze_failures:
        failures["stage_a_freeze"] = stage_freeze_failures
    if rq1_freeze_failures:
        failures["rq1_freeze"] = rq1_freeze_failures

    if stage_manifest.get("stage_a", {}).get("status") != "FROZEN":
        failures["stage_a_status"] = stage_manifest.get("stage_a", {}).get("status")
    if stage_manifest.get("declarations", {}).get("parameter_tuning_authorized") is not False:
        failures["stage_a_parameter_tuning"] = "must be false"
    if stage_manifest.get("declarations", {}).get("winning_detector_selected") is not False:
        failures["stage_a_winner_selection"] = "must be false"

    detector_gz = root / "analysis_outputs" / "stage_a_synthetic_detector_results.csv.gz"
    if detector_gz.is_file():
        with gzip.open(detector_gz, "rt", encoding="utf-8", newline="") as stream:
            header = stream.readline().rstrip("\r\n").split(",")
        missing_header = sorted(EXPECTED_STAGE_A_DETECTOR_COLUMNS - set(header))
        if missing_header:
            failures["stage_a_detector_header"] = {"missing": missing_header}
    else:
        header = []
        failures["stage_a_detector_results"] = "missing"

    analysis_cfg = load_yaml(root / approved["analysis_config"]["path"])
    evidence = cfg["evidence_domain"]
    expected_external = list(analysis_cfg["study"]["external_repositories"])
    if set(evidence["repositories"]) != set(expected_external):
        failures["external_repository_set"] = {
            "configured": evidence["repositories"],
            "expected": expected_external,
        }

    detector_contract = cfg["detector_contract"]
    frozen_mad = stage_manifest["primary_mad_specification"]
    mad = detector_contract["causal_rolling_mad"]
    mad_checks = {
        "window_weeks": int(mad["window_weeks"]) == int(frozen_mad["window_weeks"]),
        "threshold": float(mad["threshold_scaled_mad"]) == float(frozen_mad["threshold_scaled_mad"]),
        "persistence": int(mad["persistence_weeks"]) == int(frozen_mad["persistence_weeks"]),
        "variants": list(mad["variants"]) == list(frozen_mad["variants"]),
    }
    if not all(mad_checks.values()):
        failures["mad_contract"] = mad_checks

    frozen_mewma = stage_manifest["frozen_mewma_specification"]
    mewma = detector_contract["mewma"]
    mewma_checks = {
        "lambda": float(mewma["lambda"]) == float(frozen_mewma["lambda"]),
        "limit": float(mewma["empirical_control_limit"]) == float(frozen_mewma["empirical_control_limit"]),
        "refit_prohibited": mewma.get("refit_authorized") is False,
    }
    if not all(mewma_checks.values()):
        failures["mewma_contract"] = mewma_checks

    scenario_path = root / "analysis_outputs" / "stage_a_synthetic_scenarios.csv"
    stage_scenarios = pd.read_csv(scenario_path, usecols=["seed"])
    stage_seeds = set(stage_scenarios["seed"].astype("uint64").tolist())

    panel = pd.read_csv(
        root / approved["baseline_results"]["path"],
        parse_dates=["week"],
        low_memory=False,
    )
    required_panel = {
        "repo_full", "week", "split", "attempts_total",
        "external_calibration", "external_evaluation_eligible", *CORE_METRICS,
    }
    missing_panel = sorted(required_panel - set(panel.columns))
    if missing_panel:
        failures["baseline_results_columns"] = missing_panel

    design = cfg["scenario_design"]
    base_seed = int(design["base_seed"])
    repetitions = int(design["repetitions_per_injected_cell"])
    minimum_prior = int(evidence["calibration_weeks"])
    magnitude_levels = {
        profile["name"]: list(profile["magnitude_levels"])
        for profile in design["condition_profiles"]
    }

    injected_seeds: list[int] = []
    control_seeds: list[int] = []
    repository_report = []
    planned_scenarios = 0
    planned_cells = 0
    start_counts: dict[str, dict[str, int]] = {}

    for repo in evidence["repositories"]:
        sequence = (
            panel.loc[panel["repo_full"].eq(repo)]
            .sort_values("week")
            .reset_index(drop=True)
        )
        if sequence.empty:
            failures[f"repository:{repo}"] = "absent from baseline results"
            continue
        if not sequence["split"].eq(evidence["source_split"]).all():
            failures[f"repository:{repo}:split"] = sorted(sequence["split"].astype(str).unique())

        for repetition in range(int(design["no_injection_repetitions_per_repository"])):
            control_seeds.append(stable_seed(base_seed, repo, "control", repetition))

        repo_cells = 0
        repo_scenarios = int(design["no_injection_repetitions_per_repository"])
        start_counts[repo] = {}
        for combination in design["signal_combinations"]:
            affected = "+".join(combination)
            for duration_value in design["durations_weeks"]:
                duration = int(duration_value)
                starts = valid_starts(sequence, duration, minimum_prior)
                start_counts[repo][str(duration)] = len(starts)
                if not starts:
                    failures[f"valid_starts:{repo}:duration_{duration}"] = 0
                    continue
                for profile in design["condition_profiles"]:
                    for level in magnitude_levels[profile["name"]]:
                        repo_cells += 1
                        repo_scenarios += repetitions
                        for repetition in range(repetitions):
                            seed = stable_seed(
                                base_seed, repo, affected, duration,
                                profile["name"], level, repetition,
                            )
                            injected_seeds.append(seed)
        planned_cells += repo_cells
        planned_scenarios += repo_scenarios
        repository_report.append({
            "repo_full": repo,
            "sequence_rows": int(len(sequence)),
            "calibration_rows": int(sequence["external_calibration"].fillna(False).sum()),
            "evaluation_rows": int(sequence["external_evaluation_eligible"].fillna(False).sum()),
            "injected_design_cells": repo_cells,
            "planned_scenarios_including_control": repo_scenarios,
            "valid_start_counts_by_duration": start_counts[repo],
        })

    planned_seeds = [*control_seeds, *injected_seeds]
    planned_seed_set = set(planned_seeds)
    duplicate_planned_seeds = len(planned_seeds) - len(planned_seed_set)
    overlap = planned_seed_set & stage_seeds
    if duplicate_planned_seeds:
        failures["duplicate_stage_b_seeds"] = duplicate_planned_seeds
    if overlap:
        failures["stage_a_stage_b_seed_overlap"] = len(overlap)

    detector_count = len(mad["variants"]) + 1
    expected_detector_rows = planned_scenarios * detector_count
    output_directory = root / cfg["outputs"]["directory"]
    if output_directory.exists():
        failures["output_directory"] = f"already exists: {output_directory}"

    result = {
        "status": "PASS" if not failures else "FAIL",
        "failures": failures,
        "protocol_status": cfg.get("protocol_status"),
        "config_sha256": sha256(config_path),
        "contract_sha256": sha256(contract_path),
        "input_checks": input_checks,
        "stage_a_files_verified": len(stage_manifest.get("files", {})),
        "rq1_files_verified": len(rq1_manifest.get("files", {})),
        "stage_a_raw_detector_rows_declared": stage_manifest.get("stage_a", {}).get("detector_result_rows"),
        "stage_a_detector_header_columns": len(header),
        "detector_contract": {
            "mad": mad_checks,
            "mewma": mewma_checks,
            "primary_detector_count": detector_count,
        },
        "planned_grid": {
            "repositories": len(repository_report),
            "injected_design_cells": planned_cells,
            "injected_repetitions": len(injected_seeds),
            "control_scenarios": len(repository_report) * int(design["no_injection_repetitions_per_repository"]),
            "total_scenarios": planned_scenarios,
            "expected_raw_detector_rows": expected_detector_rows,
            "unique_stage_b_seeds": len(planned_seed_set),
            "duplicate_stage_b_seeds": duplicate_planned_seeds,
            "stage_a_stage_b_seed_overlap": len(overlap),
        },
        "per_repository": repository_report,
        "output_directory_exists": output_directory.exists(),
        "declarations": {
            "research_outputs_written": False,
            "stage_b_authorized": False,
            "detector_refitting_authorized": False,
        },
    }
    print("RQ2 STAGE-B PREFLIGHT")
    print(json.dumps(result, indent=2))
    return 0 if result["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())