from __future__ import annotations

import hashlib
import importlib.util
import json
import subprocess
import sys
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

from ci_common import CORE_METRICS, robust_training_standardize


ROOT = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location(
    "fit_baselines",
    ROOT / "03_fit_baselines.py",
)
assert SPEC is not None and SPEC.loader is not None
BASELINES = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BASELINES)


def configuration(output: str = "model_outputs") -> dict:
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
            "analysis_start": "2025-06-23",
            "analysis_end": "2026-08-03",
            "train_end": "2026-02-23",
            "purge_start": "2026-03-02",
            "purge_end": "2026-03-23",
            "holdout_start": "2026-03-30",
            "holdout_end": "2026-08-03",
            "external_calibration_weeks": 13,
        },
        "paths": {
            "sqlite": "unused.sqlite",
            "output": output,
        },
        "mad": {
            "primary_window_weeks": 13,
            "sensitivity_windows_weeks": [26],
            "thresholds": [2.5, 3.0, 3.5],
            "primary_threshold": 3.0,
            "persistence_weeks": [1, 2, 3, 4],
            "primary_persistence_weeks": 2,
        },
        "mewma": {
            "lambda": 0.20,
            "calibration_quantile": 0.99,
        },
    }


def synthetic_panel(cfg: dict) -> pd.DataFrame:
    weeks = pd.date_range(
        cfg["study"]["analysis_start"],
        cfg["study"]["analysis_end"],
        freq="W-MON",
    )
    development = cfg["study"]["development_repositories"]
    external = cfg["study"]["external_repositories"]
    rows: list[dict] = []

    for repo_index, repo in enumerate([*development, *external]):
        for position, week in enumerate(weeks):
            if repo in development:
                if position < 36:
                    split = "train"
                elif position < 40:
                    split = "purge"
                else:
                    split = "holdout"
                calibration = False
                evaluation = False
            else:
                split = "external"
                calibration = position < 13
                evaluation = position >= 13

            rows.append(
                {
                    "repo_full": repo,
                    "week": week,
                    "attempts_total": 12,
                    "split": split,
                    "external_calibration": calibration,
                    "external_evaluation_eligible": evaluation,
                    "latency_log": (
                        0.03 * position
                        + 0.18 * np.sin((position + repo_index) / 3)
                    ),
                    "failure_rate": (
                        0.08 + 0.025 * ((position + repo_index) % 6)
                    ),
                    "rerun_rate": (
                        0.03 + 0.018 * ((position + 2 * repo_index) % 5)
                    ),
                }
            )

    return pd.DataFrame(rows)


def assert_raises(callable_object, text: str) -> None:
    try:
        callable_object()
    except RuntimeError as exc:
        if text not in str(exc):
            raise AssertionError((text, str(exc))) from exc
    else:
        raise AssertionError(f"Expected RuntimeError containing: {text}")


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def write_synthetic_freeze(root: Path, cfg: dict, panel: pd.DataFrame) -> Path:
    output = root / cfg["paths"]["output"]
    output.mkdir(parents=True)
    config_path = root / "analysis_config.yaml"
    config_path.write_text(
        yaml.safe_dump(cfg, sort_keys=False),
        encoding="utf-8",
    )
    panel_path = output / "repository_week_panel_audited.csv"
    panel.to_csv(panel_path, index=False, date_format="%Y-%m-%d")
    quality_path = output / "data_quality_report.json"
    quality_path.write_text(
        json.dumps({"status": "PASS"}),
        encoding="utf-8",
    )

    files = {}
    for path in (config_path, panel_path, quality_path):
        relative = path.relative_to(root).as_posix()
        files[relative] = {
            "sha256": digest(path),
            "bytes": path.stat().st_size,
        }

    manifest = {
        "manifest_version": 1,
        "freeze": {},
        "files": files,
        "methodology": {},
        "repositories": {
            "development": cfg["study"]["development_repositories"],
            "external": cfg["study"]["external_repositories"],
        },
        "external_calibration_declaration": "synthetic",
        "modelling_declaration": "synthetic",
    }
    manifest_path = output / "MEASUREMENT_FROZEN.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2),
        encoding="utf-8",
    )
    return config_path


def main() -> None:
    cfg = configuration()
    panel = synthetic_panel(cfg)
    passed: list[str] = []

    history = pd.Series([*map(float, range(13)), 100.0])
    changed_current = history.copy()
    changed_current.iloc[-1] = 200.0
    score, _ = BASELINES.causal_mad(history, 13, 3.0)
    changed_score, _ = BASELINES.causal_mad(
        changed_current,
        13,
        3.0,
    )
    center = float(np.median(np.arange(13, dtype=float)))
    mad = float(np.median(np.abs(np.arange(13) - center)))
    scale = 1.4826 * mad
    assert np.isclose(score.iloc[-1], (100 - center) / scale)
    assert np.isclose(changed_score.iloc[-1], (200 - center) / scale)
    passed.append("01 causal MAD uses past observations only")
    passed.append("02 current value cannot change its reference center or scale")

    for window in (13, 26):
        values = pd.Series(np.arange(window + 2, dtype=float))
        warmup, _ = BASELINES.causal_mad(values, window, 3.0)
        assert warmup.iloc[:window].isna().all()
    passed.append("03 incomplete 13/26-week warm-up remains NA")

    zero_mad = pd.Series([*[1.0] * 13, 2.0])
    zero_score, zero_alarm = BASELINES.causal_mad(
        zero_mad,
        13,
        3.0,
    )
    assert pd.isna(zero_score.iloc[-1])
    assert pd.isna(zero_alarm.iloc[-1])
    passed.append("04 zero-MAD reference remains NA")

    persisted = BASELINES.persistence(
        pd.Series([True, pd.NA], dtype="boolean"),
        2,
    )
    assert pd.isna(persisted.iloc[-1])
    passed.append("05 persistence remains NA across unevaluable weeks")

    mad_input = panel.copy(deep=True)
    target_index = mad_input[
        mad_input["repo_full"].eq("docker/cli")
    ].sort_values("week").index[20]
    mad_input.loc[target_index, "latency_log"] = 100.0
    mad_original = mad_input.copy(deep=True)
    mad_result, mad_summary = BASELINES.fit_mad(mad_input, cfg)
    pd.testing.assert_frame_equal(mad_input, mad_original)
    assert bool(mad_result.loc[target_index, "mad_w13_t3_latency_log_k1"])
    assert "mad_w13_t3_failure_rate_k1" in mad_result
    assert "mad_w13_t3_rerun_rate_k1" in mad_result
    passed.append("06 single-metric persistence is separate")

    missing_index = mad_input[
        mad_input["repo_full"].eq("prometheus/prometheus")
    ].sort_values("week").index[20]
    missing_component = mad_input.copy(deep=True)
    missing_component.loc[missing_index, "failure_rate"] = np.nan
    missing_result, _ = BASELINES.fit_mad(missing_component, cfg)
    assert pd.isna(missing_result.loc[missing_index, "mad_w13_t3_union_k1"])
    assert pd.isna(
        missing_result.loc[missing_index, "mad_w13_t3_two_of_three_k1"]
    )
    passed.append("07 union/two-of-three require all components")
    passed.append("08 missing components do not become non-alarms")

    mewma_input = panel.copy(deep=True)
    all_missing_index = mewma_input[
        mewma_input["repo_full"].eq("docker/cli")
    ].sort_values("week").index[20]
    mewma_input.loc[all_missing_index, list(CORE_METRICS)] = np.nan
    mewma_original = mewma_input.copy(deep=True)
    mewma_result, mewma_meta, mewma_summary = BASELINES.fit_mewma(
        mewma_input,
        cfg,
    )
    pd.testing.assert_frame_equal(mewma_input, mewma_original)
    assert pd.isna(mewma_result.loc[all_missing_index, "mewma_stat"])
    assert pd.isna(mewma_result.loc[all_missing_index, "mewma_alarm"])
    passed.append("09 all-missing MEWMA row remains NA")

    holdout_changed = panel.copy(deep=True)
    holdout_mask = holdout_changed["split"].eq("holdout")
    holdout_changed.loc[holdout_mask, list(CORE_METRICS)] = (
        holdout_changed.loc[holdout_mask, list(CORE_METRICS)] * 31 + 17
    )
    base_mewma, base_meta, _ = BASELINES.fit_mewma(panel, cfg)
    changed_mewma, changed_meta, _ = BASELINES.fit_mewma(
        holdout_changed,
        cfg,
    )
    assert base_meta["empirical_limit"] == changed_meta["empirical_limit"]
    training_mask = panel["split"].eq("train")
    pd.testing.assert_series_equal(
        base_mewma.loc[training_mask, "mewma_stat"],
        changed_mewma.loc[training_mask, "mewma_stat"],
    )
    passed.append("10 holdout changes cannot alter MEWMA fitting")

    _, parameters = robust_training_standardize(panel, cfg)
    post_calibration = panel.copy(deep=True)
    post_mask = post_calibration["external_evaluation_eligible"]
    post_calibration.loc[post_mask, list(CORE_METRICS)] = (
        post_calibration.loc[post_mask, list(CORE_METRICS)] * 17 + 11
    )
    _, post_parameters = robust_training_standardize(post_calibration, cfg)
    for repo in cfg["study"]["external_repositories"]:
        assert parameters[repo] == post_parameters[repo]
    passed.append("11 post-calibration changes preserve external parameters")

    external_changed = panel.copy(deep=True)
    external_mask = external_changed["split"].eq("external")
    external_changed.loc[external_mask, list(CORE_METRICS)] = (
        external_changed.loc[external_mask, list(CORE_METRICS)] * 19 + 7
    )
    _, external_parameters = robust_training_standardize(
        external_changed,
        cfg,
    )
    _, external_mewma_meta, _ = BASELINES.fit_mewma(
        external_changed,
        cfg,
    )
    for repo in cfg["study"]["development_repositories"]:
        assert parameters[repo] == external_parameters[repo]
    assert (
        base_meta["empirical_limit"]
        == external_mewma_meta["empirical_limit"]
    )
    passed.append("12 external values cannot alter development fitting")

    assert "external_calibration" not in set(mad_summary["split"])
    assert "external_calibration" not in set(mewma_summary["split"])
    passed.append("13 calibration weeks are absent from external summaries")
    external_rows = mad_summary[
        mad_summary["split"].eq("external_evaluation")
    ]
    assert not external_rows.empty
    assert external_rows["external_scoring_eligible"].all()
    passed.append("14 only eligible external weeks are scored")

    independent = panel.copy(deep=True)
    first = cfg["study"]["development_repositories"][0]
    second = cfg["study"]["development_repositories"][1]
    first_values = independent[
        independent["repo_full"].eq(first)
    ][list(CORE_METRICS)].to_numpy()
    independent.loc[
        independent["repo_full"].eq(second),
        list(CORE_METRICS),
    ] = first_values
    independent_result, _, _ = BASELINES.fit_mewma(independent, cfg)
    first_stat = independent_result[
        independent_result["repo_full"].eq(first)
    ]["mewma_stat"].reset_index(drop=True)
    second_stat = independent_result[
        independent_result["repo_full"].eq(second)
    ]["mewma_stat"].reset_index(drop=True)
    pd.testing.assert_series_equal(
        first_stat,
        second_stat,
        check_names=False,
    )
    passed.append("15 repository sequences are processed independently")
    passed.append("16 fit_mad/fit_mewma do not mutate inputs")

    with tempfile.TemporaryDirectory() as temporary:
        temp = Path(temporary)
        output = temp / "output"
        output.mkdir()
        frozen = output / "baseline_results.csv"
        frozen.write_text("frozen", encoding="utf-8")
        collision_manifest = {
            "files": {
                frozen.relative_to(temp).as_posix(): {
                    "sha256": digest(frozen),
                    "bytes": frozen.stat().st_size,
                }
            }
        }
        assert_raises(
            lambda: BASELINES.validate_output_paths(
                temp,
                output,
                collision_manifest,
            ),
            "collides with a frozen file",
        )
        passed.append("17 frozen output collision is rejected")

        assert_raises(
            lambda: BASELINES.validate_output_paths(
                temp,
                output,
                {"files": {}},
                ("duplicate.csv", "duplicate.csv"),
            ),
            "contain duplicates",
        )
        assert_raises(
            lambda: BASELINES.validate_output_paths(
                temp,
                output,
                {"files": {}},
                ("../outside.csv",),
            ),
            "not a plain filename",
        )
        passed.append("18 duplicate/outside output paths are rejected")

    piecewise = np.r_[np.zeros(4), np.ones(4) * 20, np.zeros(4)]
    points, _ = BASELINES.optimal_partitioning_change_points(
        piecewise,
        0.1,
    )
    boundaries = [0, *points, len(piecewise)]
    assert all(
        end - start >= BASELINES.MINIMUM_SEGMENT_LENGTH
        for start, end in zip(boundaries, boundaries[1:])
    )
    passed.append("19 optimal-partitioning segments have length >= 2")

    with tempfile.TemporaryDirectory() as temporary:
        temp = Path(temporary)
        run_cfg = configuration()
        run_panel = synthetic_panel(run_cfg)
        config_path = write_synthetic_freeze(temp, run_cfg, run_panel)
        output = temp / run_cfg["paths"]["output"]
        before = {path.name for path in output.iterdir()}
        process = subprocess.run(
            [
                sys.executable,
                str(ROOT / "03_fit_baselines.py"),
                "--config",
                str(config_path),
            ],
            cwd=temp,
            check=True,
            capture_output=True,
            text=True,
        )
        after = {path.name for path in output.iterdir()}
        created = after - before
        assert created == set(BASELINES.OUTPUT_NAMES), created
        print("SYNTHETIC STEP-03 OUTPUT")
        print(process.stdout.strip())
    passed.append("20 synthetic run creates exactly five outputs")

    print("SYNTHETIC BASELINE INVARIANCE TESTS")
    for item in passed:
        print(f"PASS: {item}")
    print(f"RESULT: PASS ({len(passed)}/20)")


if __name__ == "__main__":
    main()
