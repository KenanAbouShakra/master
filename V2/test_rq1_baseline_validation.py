from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

import freeze_rq1_baselines as freezer
import validate_rq1_baselines as rq1


ROOT = Path(__file__).resolve().parent
FROZEN_MEWMA_LIMIT = 7.150034791553729


def assert_raises(expected, function, *args, **kwargs):
    try:
        function(*args, **kwargs)
    except expected:
        return
    names = ", ".join(item.__name__ for item in expected) if isinstance(
        expected, tuple
    ) else expected.__name__
    raise AssertionError(f"Expected {names}")


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def write_manifest(root: Path, name: str, records: list[str], extra: dict | None = None) -> Path:
    payload = {"files": {}}
    for relative in records:
        path = root / relative
        payload["files"][relative] = {
            "sha256": digest(path),
            "bytes": path.stat().st_size,
        }
    if extra:
        payload.update(extra)
    path = root / "analysis_outputs" / name
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path


def fixture_panel() -> pd.DataFrame:
    repos = ["dev/a", "dev/b", "dev/c", "ext/a", "ext/b", "ext/c"]
    rows = []
    for repo_index, repo in enumerate(repos):
        for position, week in enumerate(pd.date_range("2025-06-23", periods=59, freq="W-MON")):
            development = repo.startswith("dev/")
            if development:
                split = "train" if position < 36 else ("purge" if position < 40 else "holdout")
                calibration = evaluation = False
            else:
                split = "external"
                calibration = position < 13
                evaluation = position >= 13
            scores = [0.5 + position / 20, 1.5 + position / 20, 1.0 + position / 20]
            rows.append({
                "repo_full": repo,
                "week": week,
                "split": split,
                "attempts_total": 100 + position,
                "outcome_n": 80 + position,
                "failure_count": 8 + position % 4,
                "latency_n": 75 + position,
                "logical_run_n": 90 + position,
                "rerun_count": 4 + position % 3,
                "workflow_count": 3 + repo_index % 2,
                "workflow_concentration_hhi": 0.3 + 0.01 * repo_index,
                "release_count": np.nan,
                "low_outcome_support": False,
                "low_latency_support": False,
                "missing_core_metric_count": 0,
                "external_calibration": calibration,
                "external_evaluation_eligible": evaluation,
                "mad_w13_t3_latency_log_score": scores[0],
                "mad_w13_t3_failure_rate_score": scores[1],
                "mad_w13_t3_rerun_rate_score": scores[2],
                "mad_w13_t3_latency_log_k2": position % 7 in (0, 1),
                "mad_w13_t3_failure_rate_k2": position % 11 in (0, 1),
                "mad_w13_t3_rerun_rate_k2": position % 13 in (0, 1),
                "mad_w13_t3_union_k2": position % 5 in (0, 1),
                "mad_w13_t3_two_of_three_k2": position % 17 in (0, 1),
                "mewma_stat": 5.0 + position / 5,
                "mewma_limit": FROZEN_MEWMA_LIMIT,
                "mewma_alarm": (5.0 + position / 5) > FROZEN_MEWMA_LIMIT,
            })
    return pd.DataFrame(rows)


def stage_repository_table() -> pd.DataFrame:
    rows = []
    for repo in ("dev/a", "dev/b", "dev/c"):
        for family, variant in [
            ("causal_rolling_mad", "latency_log"),
            ("causal_rolling_mad", "failure_rate"),
            ("causal_rolling_mad", "rerun_rate"),
            ("causal_rolling_mad", "union"),
            ("causal_rolling_mad", "two_of_three"),
            ("mewma", "mewma"),
        ]:
            row = {
                "repo_full": repo,
                "detector_family": family,
                "detector_variant": variant,
                "scenario_count": 100,
            }
            for metric in rq1.SYNTHETIC_METRICS:
                row[f"mean_{metric}"] = 0.5
                row[f"{metric}_valid_observations"] = 80 if metric == "precision" else 100
            rows.append(row)
    return pd.DataFrame(rows)


def prepare_workspace(root: Path) -> Path:
    output = root / "analysis_outputs"
    output.mkdir()
    for source_name in (
        "validate_rq1_baselines.py",
        "test_rq1_baseline_validation.py",
        "freeze_rq1_baselines.py",
    ):
        (root / source_name).write_bytes((ROOT / source_name).read_bytes())
    panel_path = output / "baseline_results.csv"
    fixture_panel().to_csv(panel_path, index=False, date_format="%Y-%m-%d")
    stage_table = output / "stage_a_table_primary_by_repository.csv"
    stage_repository_table().to_csv(stage_table, index=False)
    config_path = root / "analysis_config.yaml"
    config = {
        "study": {
            "development_repositories": ["dev/a", "dev/b", "dev/c"],
            "external_repositories": ["ext/a", "ext/b", "ext/c"],
        },
        "paths": {"output": "analysis_outputs"},
    }
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")
    measurement = write_manifest(root, "MEASUREMENT_FROZEN.json", ["analysis_config.yaml"])
    stage = write_manifest(
        root,
        "STAGE_A_FROZEN.json",
        [
            "analysis_outputs/baseline_results.csv",
            "analysis_outputs/stage_a_table_primary_by_repository.csv",
        ],
        {
            "primary_mad_specification": {
                "current_week_excluded": True,
                "interpretation": "detector signal, not ground truth",
                "persistence_weeks": 2,
                "threshold_scaled_mad": 3.0,
                "variants": [
                    "latency_log", "failure_rate", "rerun_rate", "union", "two_of_three"
                ],
                "window_weeks": 13,
            },
            "frozen_mewma_specification": {
                "lambda": 0.2,
                "empirical_control_limit": FROZEN_MEWMA_LIMIT,
                "refitted_during_synthetic_evaluation": False,
            },
        },
    )
    rejection = {
        "status": "HMM_REJECTED",
        "decision": {
            "confirmatory_rq1_inclusion": False,
            "supported_primary_detectors": ["causal rolling MAD", "MEWMA"],
        },
        "model_use": {"model_selected": False, "model_used_for_repository_scoring": False},
        "provenance": {},
    }
    (output / "HMM_REJECTION_REPORT.json").write_text(
        json.dumps(rejection), encoding="utf-8"
    )
    return config_path


def main() -> None:
    frozen_before = {}
    for manifest_name in (
        "analysis_outputs/MEASUREMENT_FROZEN.json",
        "analysis_outputs/STAGE_A_FROZEN.json",
    ):
        manifest = json.loads((ROOT / manifest_name).read_text(encoding="utf-8"))
        frozen_before[manifest_name] = {
            relative: (digest(ROOT / relative), (ROOT / relative).stat().st_size)
            for relative in manifest["files"]
        }
    research_before = {
        path.name: (digest(path), path.stat().st_size)
        for path in (ROOT / "analysis_outputs").iterdir()
        if path.is_file()
    }
    passed = []

    panel = fixture_panel()
    contract_manifest = {
        "primary_mad_specification": {
            "current_week_excluded": True,
            "interpretation": "detector signal, not ground truth",
            "persistence_weeks": 2,
            "threshold_scaled_mad": 3.0,
            "variants": [
                "latency_log", "failure_rate", "rerun_rate", "union", "two_of_three"
            ],
            "window_weeks": 13,
        },
        "frozen_mewma_specification": {
            "lambda": 0.2,
            "empirical_control_limit": FROZEN_MEWMA_LIMIT,
            "refitted_during_synthetic_evaluation": False,
        },
    }
    contract = rq1.load_detector_contract(contract_manifest, set(panel.columns))
    assert contract["mad"]["variants"] == contract_manifest["primary_mad_specification"]["variants"]
    assert contract["specification_role"] == "primary"
    passed.append("primary contract loaded directly from Stage-A manifest")

    for key, bad_value in (
        ("window_weeks", 12),
        ("threshold_scaled_mad", 2.5),
        ("persistence_weeks", 1),
        ("current_week_excluded", False),
        ("variants", ["latency_log", "union"]),
        ("interpretation", "ground truth"),
    ):
        bad = json.loads(json.dumps(contract_manifest))
        bad["primary_mad_specification"][key] = bad_value
        assert_raises(RuntimeError, rq1.load_detector_contract, bad, set(panel.columns))
    for key, bad_value in (
        ("lambda", 0.3),
        ("empirical_control_limit", 7.0),
        ("refitted_during_synthetic_evaluation", True),
    ):
        bad = json.loads(json.dumps(contract_manifest))
        bad["frozen_mewma_specification"][key] = bad_value
        assert_raises(RuntimeError, rq1.load_detector_contract, bad, set(panel.columns))
    passed.append("wrong MAD and MEWMA contracts are rejected")

    mewma_validation = rq1.validate_frozen_mewma_panel(panel, contract)
    assert mewma_validation["alarm_consistency"] == "PASS"
    wrong_limit = panel.copy()
    wrong_limit.loc[0, "mewma_limit"] = 7.0
    assert_raises(
        RuntimeError, rq1.validate_frozen_mewma_panel, wrong_limit, contract
    )
    wrong_alarm = panel.copy()
    wrong_alarm.loc[0, "mewma_alarm"] = not bool(wrong_alarm.loc[0, "mewma_alarm"])
    assert_raises(
        RuntimeError, rq1.validate_frozen_mewma_panel, wrong_alarm, contract
    )
    wrong_missing = panel.copy()
    wrong_missing.loc[0, "mewma_stat"] = np.nan
    wrong_missing.loc[0, "mewma_alarm"] = False
    assert_raises(
        RuntimeError, rq1.validate_frozen_mewma_panel, wrong_missing, contract
    )
    passed.append("frozen MEWMA limit, statistic, alarm, and evaluability agree")

    example = panel.iloc[0].copy()
    example["mad_w13_t3_latency_log_score"] = 1.0
    example["mad_w13_t3_failure_rate_score"] = 5.0
    example["mad_w13_t3_rerun_rate_score"] = 3.0
    assert rq1.mad_magnitude(example, "latency_log", contract["mad"]) == 1.0
    assert rq1.mad_magnitude(example, "union", contract["mad"]) == 5.0
    assert rq1.mad_magnitude(example, "two_of_three", contract["mad"]) == 3.0
    passed.append("detector-aligned MAD magnitudes")

    config = {
        "study": {
            "development_repositories": ["dev/a", "dev/b", "dev/c"],
            "external_repositories": ["ext/a", "ext/b", "ext/c"],
        }
    }
    populations = rq1.evaluation_population(panel, config)
    totals, repository_totals = rq1.validate_evaluation_denominators(
        panel, populations, config
    )
    assert totals == {
        "external_evaluation": 138,
        "holdout": 57,
    }
    assert set(repository_totals.values()) == {19, 46}
    assert not populations[panel["split"].isin(["train", "purge"])].notna().any()
    assert not populations[panel["external_calibration"]].notna().any()
    passed.append("exact holdout and external evaluation denominators")

    reduced = panel.drop(
        panel[
            panel["repo_full"].eq("dev/a") & panel["split"].eq("holdout")
        ].index[:1]
    ).reset_index(drop=True)
    reduced_population = rq1.evaluation_population(reduced, config)
    assert_raises(
        RuntimeError,
        rq1.validate_evaluation_denominators,
        reduced,
        reduced_population,
        config,
    )
    passed.append("wrong repository-specific denominator is rejected")

    week_results = rq1.build_week_results(panel, populations, contract)
    assert len(week_results) == 1170
    mewma = week_results[week_results["detector_family"].eq("mewma")].iloc[0]
    assert np.isclose(
        mewma["magnitude"], mewma["raw_mewma_stat"] / FROZEN_MEWMA_LIMIT
    )
    assert week_results["causal_history_policy"].str.contains("purge|calibration").all()
    passed.append("frozen detector outputs and causal history policies retained")

    assert rq1.validate_week_detector_rows(week_results)["actual_rows"] == 1170
    assert_raises(
        RuntimeError, rq1.validate_week_detector_rows, week_results.iloc[:-1]
    )
    passed.append("wrong 1170 week-detector row count is rejected")

    weeks = pd.date_range("2026-01-05", periods=4, freq="W-MON")
    episode_input = pd.DataFrame({
        "repo_full": ["dev/a"] * 4,
        "week": weeks,
        "evaluation_population": ["holdout"] * 4,
        "detector_family": ["mewma"] * 4,
        "detector_variant": ["mewma"] * 4,
        "metric": ["composite"] * 4,
        "alarm": pd.Series([True, True, pd.NA, True], dtype="boolean"),
        "magnitude": [2.0, 3.0, np.nan, 4.0],
        "attempts_total": [10] * 4,
        "outcome_n": [8] * 4,
        "latency_n": [7] * 4,
        "logical_run_n": [9] * 4,
        "workflow_count": [2] * 4,
        "workflow_concentration_hhi": [0.5] * 4,
        "low_outcome_support": [False] * 4,
        "low_latency_support": [False] * 4,
        "missing_core_metric_count": [0, 0, 1, 0],
    })
    full_alarm = pd.Series(
        [True, True, True, pd.NA, True],
        index=[weeks[0] - pd.Timedelta(days=7), *weeks],
        dtype="boolean",
    )
    episodes = rq1.construct_episodes(episode_input, full_alarm)
    assert len(episodes) == 2
    assert bool(episodes.iloc[0]["left_censored"])
    assert not bool(episodes.iloc[0]["right_censored"])
    assert episodes.iloc[0]["observed_duration_weeks"] == 2
    assert bool(episodes.iloc[1]["right_censored"])
    assert episodes.iloc[1]["observed_duration_weeks"] == 1
    passed.append("episode NA breaks, adjacency, duration, and boundary censoring")

    observed_summary = rq1.summarize_observed(week_results, pd.DataFrame(columns=rq1.EPISODE_COLUMNS))
    observed = observed_summary[observed_summary["evidence_domain"].eq("observed_research")]
    assert observed[["precision", "recall", "false_alarm_rate"]].isna().all().all()
    assert observed[["precision_valid_observations", "recall_valid_observations"]].eq(0).all().all()
    assert {"attempts_total_sum", "outcome_n_sum", "latency_n_sum", "logical_run_n_sum"}.issubset(observed.columns)
    synthetic = rq1.synthetic_summary(stage_repository_table())
    assert synthetic["evidence_domain"].eq("synthetic_stage_a").all()
    assert synthetic["precision_valid_observations"].eq(80).all()
    assert synthetic[["alarm_prevalence", "alarm_weeks"]].isna().all().all()
    passed.append("observed and synthetic evidence domains remain separate")

    agreement_input = pd.DataFrame([
        {"repo_full": "dev/a", "week": weeks[0], "evaluation_population": "holdout", "detector_family": "causal_rolling_mad", "detector_variant": "union", "evaluable": True, "alarm": True},
        {"repo_full": "dev/a", "week": weeks[1], "evaluation_population": "holdout", "detector_family": "causal_rolling_mad", "detector_variant": "union", "evaluable": False, "alarm": pd.NA},
        {"repo_full": "dev/a", "week": weeks[0], "evaluation_population": "holdout", "detector_family": "mewma", "detector_variant": "mewma", "evaluable": True, "alarm": True},
        {"repo_full": "dev/a", "week": weeks[1], "evaluation_population": "holdout", "detector_family": "mewma", "detector_variant": "mewma", "evaluable": True, "alarm": False},
    ])
    agreement = rq1.detector_agreement(agreement_input)
    assert agreement.iloc[0]["both_evaluable_weeks"] == 1
    assert agreement.iloc[0]["both_alarm_weeks"] == 1
    assert agreement.iloc[0]["interpretation"] == "detector agreement, not ground truth"
    passed.append("agreement uses pairwise evaluable weeks only")

    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        config_path = prepare_workspace(root)
        status = rq1.run(config_path)
        output = root / "analysis_outputs"
        assert status["status"] == "PASS"
        assert {name for name in rq1.OUTPUT_NAMES if (output / name).exists()} == set(rq1.OUTPUT_NAMES)
        results = pd.read_csv(output / "rq1_baseline_week_results.csv")
        summary = pd.read_csv(output / "rq1_baseline_summary.csv")
        support = pd.read_csv(output / "rq1_baseline_support_diagnostics.csv")
        validation = json.loads((output / "rq1_baseline_validation_status.json").read_text())
        assert len(results) == 1170
        assert set(results["evaluation_population"]) == {"holdout", "external_evaluation"}
        assert {"attempts_total_sum", "outcome_n_sum", "latency_n_sum", "logical_run_n_sum"}.issubset(support.columns)
        assert validation["declarations"]["observed_confusion_metrics_reported"] is False
        assert validation["declarations"]["hmm_excluded_before_confirmatory_scoring"] is True
        assert validation["week_detector_contract"]["actual_rows"] == 1170
        assert len(validation["output_artifacts"]) == 5
        assert_raises(RuntimeError, freezer.freeze, config_path, False)
        (output / "hmm_results.csv").write_text("blocked", encoding="utf-8")
        assert_raises(RuntimeError, freezer.freeze, config_path, True)
        (output / "hmm_results.csv").unlink()
        frozen = freezer.freeze(config_path, True)
        assert frozen["status"] == "PASS"
        freeze_payload = json.loads((output / "RQ1_BASELINES_FROZEN.json").read_text())
        assert freeze_payload["declarations"]["hmm_included"] is False
    passed.append("temporary end-to-end validation and baseline-only freeze")

    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        config_path = prepare_workspace(root)
        rq1.run(config_path)
        output = root / "analysis_outputs"
        status_path = output / "rq1_baseline_validation_status.json"
        status = json.loads(status_path.read_text(encoding="utf-8"))
        status["provenance"]["analysis_config_sha256"] = "stale"
        status_path.write_text(json.dumps(status), encoding="utf-8")
        assert_raises(RuntimeError, freezer.freeze, config_path, True)
    passed.append("stale validation status is rejected")

    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        config_path = prepare_workspace(root)
        rq1.run(config_path)
        output = root / "analysis_outputs"
        with (output / "rq1_baseline_summary.csv").open("a", encoding="utf-8") as handle:
            handle.write("tampered\n")
        assert_raises(RuntimeError, freezer.freeze, config_path, True)
    passed.append("tampered output is rejected before freeze")

    with tempfile.TemporaryDirectory() as temporary:
        output = Path(temporary)
        frames = {
            "rq1_baseline_week_results.csv": pd.DataFrame({"x": [1]}),
            "rq1_baseline_summary.csv": pd.DataFrame({"x": [2]}),
            "rq1_baseline_candidate_episodes.csv": pd.DataFrame({"x": [3]}),
            "rq1_baseline_detector_agreement.csv": pd.DataFrame({"x": [4]}),
            "rq1_baseline_support_diagnostics.csv": pd.DataFrame({"x": [5]}),
        }
        original_link = rq1.os.link
        count = {"value": 0}
        def fail_link(source, destination):
            destination = Path(destination)
            if destination.parent == output:
                count["value"] += 1
                if count["value"] == 3:
                    raise OSError("synthetic rollback")
            return original_link(source, destination)
        rq1.os.link = fail_link
        try:
            try:
                rq1.atomic_output_bundle(output, frames, {"status": "PASS"})
            except OSError:
                pass
            else:
                raise AssertionError("Expected rollback failure")
        finally:
            rq1.os.link = original_link
        assert not any((output / name).exists() for name in rq1.OUTPUT_NAMES)
        assert not list(output.glob("rq1_baseline_*"))
    passed.append("atomic output bundle rollback")

    with tempfile.TemporaryDirectory() as temporary:
        output = Path(temporary)
        frames = {
            "rq1_baseline_week_results.csv": pd.DataFrame({"x": [1]}),
            "rq1_baseline_summary.csv": pd.DataFrame({"x": [2]}),
            "rq1_baseline_candidate_episodes.csv": pd.DataFrame({"x": [3]}),
            "rq1_baseline_detector_agreement.csv": pd.DataFrame({"x": [4]}),
            "rq1_baseline_support_diagnostics.csv": pd.DataFrame({"x": [5]}),
        }
        original_link = rq1.os.link
        count = {"value": 0}
        collision = output / "rq1_baseline_summary.csv"
        def collide_during_commit(source, destination):
            destination = Path(destination)
            count["value"] += 1
            if count["value"] == 2:
                collision.write_text("appeared", encoding="utf-8")
            return original_link(source, destination)
        rq1.os.link = collide_during_commit
        try:
            assert_raises(
                (RuntimeError, FileExistsError),
                rq1.atomic_output_bundle,
                output,
                frames,
                {"status": "PASS"},
            )
        finally:
            rq1.os.link = original_link
        assert collision.read_text(encoding="utf-8") == "appeared"
        assert not (output / "rq1_baseline_week_results.csv").exists()
        collision.unlink()
        assert not list(output.glob("rq1_baseline_*"))
    passed.append("commit-time output collision preserves newcomer and rolls back")

    for manifest_name, records in frozen_before.items():
        current = json.loads((ROOT / manifest_name).read_text(encoding="utf-8"))
        assert set(current["files"]) == set(records)
        for relative, expected in records.items():
            assert (digest(ROOT / relative), (ROOT / relative).stat().st_size) == expected
    research_after = {
        path.name: (digest(path), path.stat().st_size)
        for path in (ROOT / "analysis_outputs").iterdir()
        if path.is_file()
    }
    assert research_before == research_after
    passed.append("no frozen file or research output changed")

    print("BASELINE-ONLY RQ1 VALIDATION TESTS")
    for item in passed:
        print(f"PASS: {item}")
    print(f"RESULT: PASS ({len(passed)}/{len(passed)})")


if __name__ == "__main__":
    main()
