from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

import validate_stage_a_baselines as stage_a


ROOT = Path(__file__).resolve().parent


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def analysis_configuration() -> dict:
    return {
        "study": {
            "development_repositories": [
                "docker/cli",
                "prometheus/prometheus",
                "tektoncd/pipeline",
            ],
            "external_repositories": [
                "pytest-dev/pytest",
                "helm/helm",
                "containerd/containerd",
            ],
        },
        "mad": {
            "primary_window_weeks": 13,
            "sensitivity_windows_weeks": [26],
            "thresholds": [2.5, 3.0, 3.5],
            "primary_threshold": 3.0,
            "persistence_weeks": [1, 2, 3, 4],
            "primary_persistence_weeks": 2,
        },
        "synthetic_validation": {
            "repetitions": 1,
            "latency_relative_shifts": [0.10, 0.25, 0.50],
            "probability_absolute_shifts": [0.05, 0.10, 0.20],
            "durations_weeks": [1, 2, 4, 8],
            "random_seed": 20260816,
        },
    }


def synthetic_panel() -> pd.DataFrame:
    repositories = [
        "docker/cli",
        "prometheus/prometheus",
        "tektoncd/pipeline",
        "pytest-dev/pytest",
    ]
    weeks = pd.date_range("2025-06-23", periods=36, freq="W-MON")
    rows = []
    for repo_index, repo in enumerate(repositories):
        for position, week in enumerate(weeks):
            development = repo != "pytest-dev/pytest"
            split = "train" if development else "external"
            rows.append({
                "repo_full": repo,
                "week": week,
                "attempts_total": 100 + position,
                "outcome_n": 80 + position,
                "failure_count": 8 + position % 5,
                "failure_rate": (8 + position % 5) / (80 + position),
                "latency_n": 75 + position,
                "logical_run_n": 90 + position,
                "rerun_count": 4 + position % 4,
                "rerun_rate": (4 + position % 4) / (90 + position),
                "latency_log": 0.02 * position + 0.01 * repo_index,
                "split": split,
                "external_calibration": bool(not development and position < 13),
                "external_evaluation_eligible": bool(not development and position >= 13),
            })
    return pd.DataFrame(rows)


def baseline_metadata(panel: pd.DataFrame) -> dict:
    standardization = {}
    for repo, group in panel.groupby("repo_full"):
        reference = group.head(13) if group["split"].eq("external").all() else group
        metrics = {}
        for metric in stage_a.CORE_METRICS:
            values = reference[metric].astype(float)
            metrics[metric] = {
                "median": float(values.median()),
                "scale": float(values.std(ddof=1)),
                "scale_method": "sample_standard_deviation",
                "reference_observations": int(len(values)),
            }
        standardization[repo] = {
            "reference": "synthetic",
            "reference_weeks": int(len(reference)),
            "metrics": metrics,
        }
    return {
        "mad": {
            "family": "causal_rolling_mad",
            "primary_window_weeks": 13,
            "sensitivity_windows_weeks": [26],
            "thresholds": [2.5, 3.0, 3.5],
            "primary_threshold": 3.0,
            "persistence_weeks": [1, 2, 3, 4],
            "primary_persistence_weeks": 2,
        },
        "mewma": {
            "lambda": 0.20,
            "empirical_limit": 7.15,
            "control_limit_reference": "development_training_rows_only",
            "standardization": standardization,
        },
    }


def stage_configuration(root: Path, hashes: dict[str, str]) -> dict:
    return {
        "version": 1,
        "approved_inputs": {
            "measurement_manifest": {
                "path": "synthetic_outputs/MEASUREMENT_FROZEN.json",
                "sha256": hashes["manifest"],
            },
            "analysis_config": {
                "path": "analysis_config.yaml",
                "sha256": hashes["analysis_config"],
            },
            "baseline_source": {
                "path": "03_fit_baselines.py",
                "sha256": hashes["baseline_source"],
            },
            "baseline_metadata": {
                "path": "synthetic_outputs/baseline_metadata.json",
                "sha256": hashes["baseline_metadata"],
            },
            "baseline_results": {
                "path": "synthetic_outputs/baseline_results.csv",
                "sha256": hashes["baseline_results"],
            },
            "workflow_week_panel": {
                "path": "synthetic_outputs/workflow_week_panel.csv",
                "sha256": hashes["workflow_week_panel"],
            },
        },
        "scenario_design": {
            "random_seed": 20260816,
            "repetitions": 1,
            "earliest_injection_index": 26,
            "latency_relative_shifts": [0.10, 0.25, 0.50],
            "failure_probability_shifts": [0.05, 0.10, 0.20],
            "rerun_probability_shifts": [0.05, 0.10, 0.20],
            "durations_weeks": [1, 2, 4, 8],
            "signal_combinations": [["latency_log"]],
            "condition_profiles": [{
                "name": "observed",
                "volume_condition": "observed",
                "missingness_condition": "none",
                "magnitude_levels": ["low"],
            }],
            "low_volume": {
                "sampling_fraction": 0.25,
                "minimum_denominator": 5,
            },
            "controlled_missingness": {
                "fraction_of_affected_cells": 0.25,
                "minimum_cells": 1,
            },
            "workflow_composition": {
                "status": "unsupported",
                "reason": "Synthetic aggregate definitions do not support valid reweighting.",
            },
        },
        "outputs": {
            "directory": "synthetic_outputs",
            "files": list(stage_a.OUTPUT_NAMES),
        },
    }


def prepare_workspace(root: Path) -> Path:
    output = root / "synthetic_outputs"
    output.mkdir()
    panel = synthetic_panel()
    development = panel[panel["split"].eq("train")].copy()
    analysis_cfg = analysis_configuration()
    analysis_path = root / "analysis_config.yaml"
    analysis_path.write_text(yaml.safe_dump(analysis_cfg, sort_keys=False), encoding="utf-8")
    source_path = root / "03_fit_baselines.py"
    shutil.copy2(ROOT / "03_fit_baselines.py", source_path)
    results_path = output / "baseline_results.csv"
    panel.to_csv(results_path, index=False, date_format="%Y-%m-%d")
    metadata_path = output / "baseline_metadata.json"
    metadata_path.write_text(json.dumps(baseline_metadata(panel), indent=2), encoding="utf-8")
    workflow_path = output / "workflow_week_panel.csv"
    development[["repo_full", "week", "attempts_total"]].to_csv(workflow_path, index=False)
    frozen_path = output / "synthetic_measurement.txt"
    frozen_path.write_text("synthetic-only", encoding="utf-8")
    manifest_path = output / "MEASUREMENT_FROZEN.json"
    manifest = {
        "files": {
            frozen_path.relative_to(root).as_posix(): {
                "sha256": digest(frozen_path),
                "bytes": frozen_path.stat().st_size,
            }
        }
    }
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    hashes = {
        "manifest": digest(manifest_path),
        "analysis_config": digest(analysis_path),
        "baseline_source": digest(source_path),
        "baseline_metadata": digest(metadata_path),
        "baseline_results": digest(results_path),
        "workflow_week_panel": digest(workflow_path),
    }
    config = stage_configuration(root, hashes)
    config_path = root / "stage_a_synthetic_config.yaml"
    config_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    return config_path


def scenario_row(**overrides) -> pd.Series:
    values = {
        "scenario_id": "S000000001",
        "repo_full": "docker/cli",
        "scenario_type": "metric_shift",
        "affected_metrics": "latency_log",
        "magnitude_level": "low",
        "magnitude": json.dumps({"latency_log": 0.10}),
        "duration_weeks": 1,
        "volume_condition": "observed",
        "missingness_condition": "none",
        "repetition": 0,
        "seed": 123,
        "start_index": 26,
        "end_index": 26,
        "status": "evaluable",
    }
    values.update(overrides)
    return pd.Series(values)


def main() -> None:
    research_before = {
        path.name: (digest(path), path.stat().st_size)
        for path in (ROOT / "analysis_outputs").iterdir()
        if path.is_file()
    }
    passed: list[str] = []
    panel = synthetic_panel()
    reference = panel[panel["repo_full"].eq("docker/cli")].reset_index(drop=True)
    config = {
        "scenario_design": {
            "low_volume": {"sampling_fraction": 0.25, "minimum_denominator": 5},
            "controlled_missingness": {"fraction_of_affected_cells": 0.25, "minimum_cells": 1},
        }
    }

    shifted, truth, weeks, _ = stage_a.apply_scenario(reference, scenario_row(), config)
    expected = reference.loc[26, "latency_log"] + np.log1p(0.10)
    assert np.isclose(shifted.loc[26, "latency_log"], expected)
    assert truth.sum() == 1 and weeks[0]["week"] == reference.loc[26, "week"].strftime("%Y-%m-%d")
    passed.append("latency uses log1p and injected weeks are exact")

    probability = scenario_row(
        affected_metrics="failure_rate+rerun_rate",
        magnitude=json.dumps({"failure_rate": 1.0, "rerun_rate": 1.0}),
    )
    clipped, _, clipping_rows, diagnostics = stage_a.apply_scenario(
        reference,
        probability,
        config,
    )
    assert clipped.loc[26, "failure_rate"] == 1.0
    assert clipped.loc[26, "rerun_rate"] == 1.0
    assert diagnostics["clipping_count"] == 2
    assert clipping_rows[0]["failure_rate_preclip"] > 1
    assert clipping_rows[0]["failure_rate_postclip"] == 1
    assert clipping_rows[0]["rerun_rate_preclip"] > 1
    assert clipping_rows[0]["rerun_rate_postclip"] == 1
    assert clipping_rows[0]["probability_clipping_count"] == 2
    passed.append("probability shifts are absolute and clipped")

    metadata = baseline_metadata(panel)
    metadata_before = json.dumps(metadata, sort_keys=True)
    stage_a.apply_frozen_mewma(reference, "docker/cli", metadata)
    assert json.dumps(metadata, sort_keys=True) == metadata_before
    passed.append("frozen MEWMA parameters and limit do not change")

    training_only = panel[panel["split"].eq("train")]
    grid = stage_a.build_scenario_grid(training_only, {
        "scenario_design": {
            "random_seed": 1,
            "repetitions": 1,
            "earliest_injection_index": 26,
            "latency_relative_shifts": [0.10, 0.25, 0.50],
            "failure_probability_shifts": [0.05, 0.10, 0.20],
            "rerun_probability_shifts": [0.05, 0.10, 0.20],
            "durations_weeks": [1],
            "signal_combinations": [["latency_log"]],
            "condition_profiles": [{"name": "observed", "volume_condition": "observed", "missingness_condition": "none", "magnitude_levels": ["low"]}],
            "workflow_composition": {"status": "unsupported", "reason": "unsupported"},
        }
    })
    assert set(grid["repo_full"]) == {"docker/cli", "prometheus/prometheus", "tektoncd/pipeline"}
    assert grid.equals(stage_a.build_scenario_grid(training_only, {
        "scenario_design": {
            "random_seed": 1, "repetitions": 1, "earliest_injection_index": 26,
            "latency_relative_shifts": [0.10, 0.25, 0.50], "failure_probability_shifts": [0.05, 0.10, 0.20], "rerun_probability_shifts": [0.05, 0.10, 0.20],
            "durations_weeks": [1], "signal_combinations": [["latency_log"]],
            "condition_profiles": [{"name": "observed", "volume_condition": "observed", "missingness_condition": "none", "magnitude_levels": ["low"]}],
            "workflow_composition": {"status": "unsupported", "reason": "unsupported"},
        }
    }))
    passed.append("training repositories remain separate and seeds reproduce the grid")

    control = scenario_row(scenario_type="no_injection", affected_metrics="none", magnitude="{}", duration_weeks=0, start_index=None, end_index=None)
    _, control_truth, control_weeks, _ = stage_a.apply_scenario(reference, control, config)
    assert not control_truth.any() and not control_weeks
    passed.append("no-injection controls have no true-positive interval")

    known_truth = pd.Series([False, True, True, False])
    known_prediction = pd.Series([False, False, True, True], dtype="boolean")
    score = stage_a.evaluate_predictions(known_truth, known_prediction)
    assert (score["tp"], score["fp"], score["tn"], score["fn"]) == (1, 1, 1, 1)
    assert score["tp"] + score["fp"] + score["tn"] + score["fn"] == score["evaluable_weeks"]
    assert score["precision"] == 0.5 and score["recall"] == 0.5 and score["false_alarm_rate"] == 0.5
    assert score["detection_delay_weeks"] == 1 and np.isclose(score["boundary_overlap"], 1 / 3)
    passed.append("confusion metrics, delay and overlap reconstruct")

    missing_prediction = pd.Series([False, pd.NA, True, False], dtype="boolean")
    missing_score = stage_a.evaluate_predictions(known_truth, missing_prediction)
    assert missing_score["unevaluable_weeks"] == 1
    assert missing_score["truth_unevaluable_weeks"] == 1
    assert sum(missing_score[key] for key in ("tp", "fp", "tn", "fn")) == 3
    passed.append("missing weeks are excluded, not negatives")

    low = scenario_row(volume_condition="low_volume", affected_metrics="failure_rate+rerun_rate", magnitude=json.dumps({"failure_rate": 0.10, "rerun_rate": 0.10}))
    low_result, _, _, _ = stage_a.apply_scenario(reference, low, config)
    row = low_result.loc[26]
    assert 0 <= row["failure_count"] <= row["outcome_n"]
    assert 0 <= row["rerun_count"] <= row["logical_run_n"]
    assert 0 <= row["latency_n"] <= row["outcome_n"]
    assert np.isclose(row["failure_rate"], row["failure_count"] / row["outcome_n"])
    assert np.isclose(row["rerun_rate"], row["rerun_count"] / row["logical_run_n"])
    _, _, low_rows, _ = stage_a.apply_scenario(reference, low, config)
    assert np.isclose(
        low_rows[0]["failure_rate_preclip"],
        low_rows[0]["failure_rate_injection_base"] + 0.10,
    )
    assert np.isclose(
        low_rows[0]["rerun_rate_preclip"],
        low_rows[0]["rerun_rate_injection_base"] + 0.10,
    )
    passed.append("low-volume counts preserve numerator/denominator consistency")

    output_hashes = []
    for _ in range(2):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config_path = prepare_workspace(root)
            result = stage_a.run(config_path)
            output = root / "synthetic_outputs"
            created = {name for name in stage_a.OUTPUT_NAMES if (output / name).is_file()}
            assert created == set(stage_a.OUTPUT_NAMES)
            assert result["frozen_detectors"]["mewma_refitted"] is False
            scenarios = pd.read_csv(output / stage_a.OUTPUT_NAMES[0])
            assert set(scenarios["repo_full"]) == {"docker/cli", "prometheus/prometheus", "tektoncd/pipeline"}
            output_hashes.append({name: digest(output / name) for name in stage_a.OUTPUT_NAMES})
    assert output_hashes[0] == output_hashes[1]
    passed.append("two fixed-seed runs create identical five-file outputs")

    research_after = {
        path.name: (digest(path), path.stat().st_size)
        for path in (ROOT / "analysis_outputs").iterdir()
        if path.is_file()
    }
    assert research_before == research_after
    passed.append("test writes no research output")

    print("STAGE-A SYNTHETIC VALIDATION TESTS")
    for item in passed:
        print(f"PASS: {item}")
    print(f"RESULT: PASS ({len(passed)}/{len(passed)})")


if __name__ == "__main__":
    main()
