from __future__ import annotations

import importlib.util
import hashlib
import json
import os
import sys
import tempfile
import types
from datetime import timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import yaml


ROOT = Path(__file__).resolve().parent


def load_module():
    common = types.ModuleType("ci_common")
    common.CORE_METRICS = ("latency_log", "failure_rate", "rerun_rate")
    common.load_config = lambda path: None
    common.output_dir = lambda cfg, path: None
    sys.modules["ci_common"] = common
    path = ROOT / "04_fit_hmm.py"
    spec = importlib.util.spec_from_file_location("hmm_stage", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Cannot load HMM module")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def assert_raises(expected, function, *args, **kwargs):
    try:
        function(*args, **kwargs)
    except expected:
        return
    raise AssertionError(f"Expected {expected.__name__}")


def digest(path):
    value = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def write_manifest(root, name, records):
    path = root / "analysis_outputs" / name
    payload = {"files": {}}
    for relative in records:
        candidate = root / relative
        payload["files"][relative] = {
            "bytes": candidate.stat().st_size,
            "sha256": digest(candidate),
        }
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def end_to_end_workspace(root):
    output = root / "analysis_outputs"
    output.mkdir()
    repositories = ["dev/a", "dev/b", "dev/c", "ext/a", "ext/b", "ext/c"]
    rows = []
    parameters = {}
    for repo_index, repository in enumerate(repositories):
        parameters[repository] = {
            "metrics": {
                metric: {"median": 0.0, "scale": 1.0}
                for metric in ("latency_log", "failure_rate", "rerun_rate")
            }
        }
        for position in range(30):
            development = repository.startswith("dev/")
            if development:
                split = "train" if position < 20 else (
                    "purge" if position < 24 else "holdout"
                )
            else:
                split = "external"
            state = -1.0 if position % 4 < 2 else 1.0
            rows.append({
                "repo_full": repository,
                "week": pd.Timestamp("2025-06-23") + timedelta(days=7 * int(position)),
                "split": split,
                "latency_log": state + 0.03 * repo_index,
                "failure_rate": state + 0.02 * repo_index,
                "rerun_rate": state + 0.01 * repo_index,
                "external_calibration": bool(not development and position < 13),
                "external_evaluation_eligible": bool(not development and position >= 13),
            })
    panel = pd.DataFrame(rows)
    panel.loc[
        panel["repo_full"].eq("ext/c") & panel["week"].eq(pd.Timestamp("2025-08-04")),
        ["latency_log", "failure_rate", "rerun_rate"],
    ] = np.nan
    baseline_results = output / "baseline_results.csv"
    panel.to_csv(baseline_results, index=False, date_format="%Y-%m-%d")
    baseline_metadata = output / "baseline_metadata.json"
    baseline_metadata.write_text(
        json.dumps({"mewma": {"standardization": parameters}}),
        encoding="utf-8",
    )
    analysis_config = root / "analysis_config.yaml"
    configuration = {
        "study": {
            "development_repositories": ["dev/a", "dev/b", "dev/c"],
            "external_repositories": ["ext/a", "ext/b", "ext/c"],
        },
        "paths": {"output": "analysis_outputs"},
        "hmm": {
            "states": 2,
            "random_starts": 2,
            "maximum_iterations": 100,
            "tolerance": 1e-5,
            "minimum_state_occupancy": 0.05,
            "random_seed": 20260816,
        },
    }
    analysis_config.write_text(
        yaml.safe_dump(configuration, sort_keys=False),
        encoding="utf-8",
    )
    stage_metadata = output / "stage_a_synthetic_metadata.json"
    stage_metadata.write_text(
        json.dumps({
            "input_verification": {
                "approved_inputs": {
                    "baseline_metadata": {
                        "path": "analysis_outputs/baseline_metadata.json",
                        "expected_sha256": digest(baseline_metadata),
                    },
                    "baseline_results": {
                        "path": "analysis_outputs/baseline_results.csv",
                        "expected_sha256": digest(baseline_results),
                    },
                }
            }
        }),
        encoding="utf-8",
    )
    measurement = write_manifest(
        root,
        "MEASUREMENT_FROZEN.json",
        ["analysis_config.yaml"],
    )
    stage_a = write_manifest(
        root,
        "STAGE_A_FROZEN.json",
        [
            "analysis_outputs/baseline_metadata.json",
            "analysis_outputs/baseline_results.csv",
            "analysis_outputs/stage_a_synthetic_metadata.json",
        ],
    )
    return configuration, analysis_config, measurement, stage_a


def synthetic_model():
    return {
        "pi": np.array([0.9, 0.1]),
        "trans": np.array([[0.94, 0.06], [0.12, 0.88]]),
        "means": np.array([[-0.5, -0.4, -0.3], [1.2, 1.0, 0.8]]),
        "variances": np.full((2, 3), 0.5),
    }


def panel_fixture():
    rows = []
    repositories = ["dev/a", "dev/b", "dev/c", "ext/a", "ext/b", "ext/c"]
    for repository in repositories:
        for week in range(8):
            rows.append({
                "repo_full": repository,
                "week": pd.Timestamp("2025-06-23") + timedelta(days=7 * int(week)),
                "split": "train" if repository.startswith("dev/") and week < 5 else (
                    "holdout" if repository.startswith("dev/") else "external"
                ),
                "latency_log": 1.0 + week / 10,
                "failure_rate": 0.1 + week / 100,
                "rerun_rate": 0.05 + week / 200,
                "external_calibration": repository.startswith("ext/") and week < 3,
                "external_evaluation_eligible": repository.startswith("ext/") and week >= 3,
            })
    return pd.DataFrame(rows)


def standardization_fixture(panel):
    result = {}
    for repository in panel["repo_full"].unique():
        result[repository] = {
            "metrics": {
                "latency_log": {"median": 1.2, "scale": 0.2},
                "failure_rate": {"median": 0.12, "scale": 0.02},
                "rerun_rate": {"median": 0.06, "scale": 0.01},
            }
        }
    return result


def main():
    stage = load_module()
    passed = []
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
    research_outputs_before = {
        path.name: (digest(path), path.stat().st_size)
        for path in (ROOT / "analysis_outputs").iterdir()
        if path.is_file()
    }

    model = synthetic_model()
    stage.validate_model(model)
    for key in ("pi", "trans", "means", "variances"):
        assert_raises(
            ValueError,
            stage.validate_model,
            {**model, key: np.asarray(model[key]) * np.nan},
        )
    invalid_transition = {**model, "trans": np.array([[0.8, 0.3], [0.2, 0.8]])}
    assert_raises(ValueError, stage.validate_model, invalid_transition)
    passed.append("model parameter validation")

    sequence = np.array([
        [-0.7, -0.4, -0.2],
        [-0.4, np.nan, -0.3],
        [np.nan, np.nan, np.nan],
        [1.1, 0.9, 0.7],
        [1.3, 1.2, 0.9],
    ])
    filtered = stage.filtered_probabilities(sequence, model)
    smoothed = stage.smoothed_probabilities(sequence, model)
    assert filtered.shape == smoothed.shape == (5, 2)
    np.testing.assert_allclose(filtered.sum(axis=1), 1.0)
    np.testing.assert_allclose(smoothed.sum(axis=1), 1.0)
    assert np.isfinite(filtered).all() and np.isfinite(smoothed).all()
    passed.append("partial and all-missing emissions")

    altered = sequence.copy()
    altered[3:] = np.array([[-2.0, -2.0, -2.0], [-2.0, -2.0, -2.0]])
    filtered_altered = stage.filtered_probabilities(altered, model)
    smoothed_altered = stage.smoothed_probabilities(altered, model)
    np.testing.assert_allclose(filtered[:3], filtered_altered[:3], atol=1e-12)
    assert not np.allclose(smoothed[:3], smoothed_altered[:3])
    passed.append("filtered future invariance and retrospective sensitivity")

    canonical = stage.canonicalize_model({
        **model,
        "means": model["means"][[1, 0]],
        "variances": model["variances"][[1, 0]],
        "pi": model["pi"][[1, 0]],
        "trans": model["trans"][np.ix_([1, 0], [1, 0])],
    })
    assert canonical["adverse_state"] == 1
    assert canonical["state_severity_scores"][0] < canonical["state_severity_scores"][1]
    ambiguous = {**model, "means": np.zeros((2, 3))}
    assert_raises(ValueError, stage.canonicalize_model, ambiguous)
    passed.append("canonical state labels and ambiguity rejection")

    rng = np.random.default_rng(881)
    training_sequences = {
        f"dev/{index}": np.vstack([
            rng.normal(-0.8, 0.35, size=(30, 3)),
            rng.normal(1.1, 0.40, size=(30, 3)),
        ])
        for index in range(3)
    }
    first = stage.fit(
        list(training_sequences.values()), 77, 150, 1e-5, states=2
    )
    second = stage.fit(
        list(training_sequences.values()), 77, 150, 1e-5, states=2
    )
    assert first["converged"] and first["likelihood_monotonic"]
    assert first["log_likelihood"] == second["log_likelihood"]
    np.testing.assert_allclose(first["means"], second["means"])
    np.testing.assert_allclose(
        first["log_likelihood"],
        stage.total_log_likelihood(list(training_sequences.values()), first),
    )
    trace = np.asarray(first["likelihood_trace"])
    assert np.all(np.diff(trace) >= -stage.LIKELIHOOD_MONOTONICITY_TOLERANCE * (1 + np.abs(trace[:-1])))
    passed.append("deterministic fit, convergence, monotonicity, and final likelihood")

    settings = {
        "random_starts": 3,
        "random_seed": 77,
        "maximum_iterations": 150,
        "tolerance": 1e-5,
        "states": 2,
        "minimum_state_occupancy": 0.05,
    }
    selected, diagnostics, selected_start = stage.fit_multistart(
        training_sequences, settings
    )
    assert selected["converged"]
    assert diagnostics["selected"].sum() == 1
    assert int(diagnostics.loc[diagnostics["selected"], "start_index"].iloc[0]) == selected_start
    assert diagnostics["likelihood_trace_json"].map(json.loads).map(len).gt(1).all()
    occupancy = stage.enforce_training_occupancy(training_sequences, selected, 0.05)
    assert len(occupancy["by_repository"]) == 3
    assert_raises(
        RuntimeError,
        stage.enforce_training_occupancy,
        training_sequences,
        selected,
        0.51,
    )
    passed.append("multi-start selection, diagnostics, and occupancy enforcement")

    original_fit = stage.fit
    calls = {"count": 0}
    def controlled_fit(sequences, seed, max_iter, tolerance, states=2):
        calls["count"] += 1
        if calls["count"] == 1:
            raise ValueError("synthetic failed start")
        candidate = dict(first)
        candidate["seed"] = seed
        candidate["converged"] = calls["count"] == 3
        candidate["likelihood_monotonic"] = True
        return candidate
    stage.fit = controlled_fit
    try:
        controlled_model, controlled_diagnostics, _ = stage.fit_multistart(
            training_sequences, settings
        )
    finally:
        stage.fit = original_fit
    assert controlled_model["converged"]
    assert set(controlled_diagnostics["status"]) == {
        "failed", "nonconverged", "admissible"
    }
    passed.append("failed and nonconverged start isolation")

    calls = {"count": 0}
    def nonfinite_fit(sequences, seed, max_iter, tolerance, states=2):
        calls["count"] += 1
        candidate = dict(first)
        candidate["seed"] = seed
        if calls["count"] == 1:
            candidate["log_likelihood"] = np.nan
        return candidate
    stage.fit = nonfinite_fit
    try:
        finite_model, finite_diagnostics, _ = stage.fit_multistart(
            training_sequences,
            settings,
        )
    finally:
        stage.fit = original_fit
    stage.validate_model(finite_model)
    assert (finite_diagnostics["status"] == "nonfinite").sum() == 1
    assert (finite_diagnostics["status"] == "admissible").sum() == 2
    passed.append("nonfinite random start isolation")

    original_occupancy = stage.training_occupancy
    calls = {"count": 0}
    def likelihood_ordered_fit(sequences, seed, max_iter, tolerance, states=2):
        calls["count"] += 1
        candidate = dict(first)
        candidate["seed"] = seed
        candidate["log_likelihood"] = 110.0 - 10.0 * calls["count"]
        return candidate
    def occupancy_by_seed(sequences, candidate):
        invalid = candidate["seed"] == settings["random_seed"]
        smoothed = [0.99, 0.01] if invalid else [0.55, 0.45]
        return {
            "pooled_smoothed": smoothed,
            "pooled_viterbi": smoothed,
            "by_repository": [],
        }
    stage.fit = likelihood_ordered_fit
    stage.training_occupancy = occupancy_by_seed
    try:
        admissible_model, admissible_diagnostics, admissible_start = (
            stage.fit_multistart(training_sequences, settings)
        )
    finally:
        stage.fit = original_fit
        stage.training_occupancy = original_occupancy
    assert admissible_start == 1
    assert admissible_model["log_likelihood"] == 90.0
    assert admissible_diagnostics.loc[0, "status"] == "occupancy_invalid"
    assert admissible_diagnostics.loc[1, "status"] == "admissible"
    assert admissible_diagnostics["minimum_occupancy_threshold"].eq(0.05).all()
    passed.append("higher-likelihood occupancy-invalid candidate is excluded")

    calls = {"count": 0}
    def ambiguous_first_fit(sequences, seed, max_iter, tolerance, states=2):
        calls["count"] += 1
        candidate = dict(first)
        candidate["seed"] = seed
        candidate["log_likelihood"] = 100.0 - calls["count"]
        if calls["count"] == 1:
            candidate["means"] = np.array([
                [-1.0, 0.0, 1.0],
                [0.0, 0.0, 0.0],
            ])
        return candidate
    stage.fit = ambiguous_first_fit
    try:
        _, ambiguous_diagnostics, ambiguous_start = stage.fit_multistart(
            training_sequences,
            settings,
        )
    finally:
        stage.fit = original_fit
    assert ambiguous_start == 1
    assert ambiguous_diagnostics.loc[0, "status"] == "ambiguous_states"
    assert not bool(ambiguous_diagnostics.loc[0, "canonicalization_valid"])
    passed.append("ambiguous candidate is excluded before selection")

    def all_invalid_occupancy(sequences, candidate):
        return {
            "pooled_smoothed": [0.99, 0.01],
            "pooled_viterbi": [1.0, 0.0],
            "by_repository": [],
        }
    stage.fit = likelihood_ordered_fit
    stage.training_occupancy = all_invalid_occupancy
    try:
        try:
            stage.fit_multistart(training_sequences, settings)
        except stage.CandidateSelectionError as exc:
            total_failure_diagnostics = exc.diagnostics
        else:
            raise AssertionError("Expected CandidateSelectionError")
    finally:
        stage.fit = original_fit
        stage.training_occupancy = original_occupancy
    assert len(total_failure_diagnostics) == settings["random_starts"]
    assert total_failure_diagnostics["status"].eq("occupancy_invalid").all()
    assert not total_failure_diagnostics["selected"].any()
    assert total_failure_diagnostics["minimum_occupancy_threshold"].eq(0.05).all()
    passed.append("all-invalid failure retains complete diagnostics")

    panel = panel_fixture()
    cfg = {
        "study": {
            "development_repositories": ["dev/a", "dev/b", "dev/c"],
            "external_repositories": ["ext/a", "ext/b", "ext/c"],
        }
    }
    validated = stage.validate_panel(panel.sample(frac=1, random_state=4), cfg)
    assert validated[["repo_full", "week"]].equals(
        validated[["repo_full", "week"]].sort_values(["repo_full", "week"]).reset_index(drop=True)
    )
    parameters = standardization_fixture(validated)
    standardized = stage.apply_frozen_standardization(validated, parameters)
    sequences = stage.build_training_sequences(standardized, cfg)
    assert list(sequences) == ["dev/a", "dev/b", "dev/c"]
    assert all(len(value) == 5 for value in sequences.values())
    passed.append("sorting, frozen scaling, and training isolation")

    changed = validated.copy()
    mask = ~(
        changed["repo_full"].isin(cfg["study"]["development_repositories"])
        & changed["split"].eq("train")
    )
    changed.loc[mask, ["latency_log", "failure_rate", "rerun_rate"]] += 100
    changed = stage.apply_frozen_standardization(changed, parameters)
    changed_sequences = stage.build_training_sequences(changed, cfg)
    for repository in sequences:
        np.testing.assert_allclose(sequences[repository], changed_sequences[repository])
    passed.append("holdout and external training invariance")

    scoring_model = stage.canonicalize_model(model)
    model_before = json.dumps(stage.serialise(scoring_model), sort_keys=True)
    scored = stage.score_panel(standardized, scoring_model)
    assert json.dumps(stage.serialise(scoring_model), sort_keys=True) == model_before
    assert scored["hmm_filtered_is_causal"].all()
    assert scored["hmm_smoothed_is_retrospective"].all()
    assert scored["hmm_viterbi_is_retrospective"].all()
    assert scored["hmm_observed_dimensions"].between(0, 3).all()
    missing_week = scored["repo_full"].eq("ext/a") & scored["week"].eq(
        scored.loc[scored["repo_full"].eq("ext/a"), "week"].iloc[2]
    )
    missing_input = standardized.copy()
    missing_input.loc[missing_week, [f"z_{m}" for m in stage.CORE_METRICS]] = np.nan
    missing_scored = stage.score_panel(missing_input, scoring_model)
    assert missing_scored.loc[missing_week, "hmm_filtered_adverse_probability"].notna().all()
    assert not missing_scored.loc[missing_week, "hmm_evidence_evaluable"].any()
    passed.append("separate causal and retrospective outputs")

    changed_boundary = standardized.copy()
    previous_repo = changed_boundary["repo_full"].eq("dev/a")
    changed_boundary.loc[
        previous_repo & changed_boundary["week"].eq(changed_boundary.loc[previous_repo, "week"].max()),
        [f"z_{m}" for m in stage.CORE_METRICS],
    ] = 50.0
    rescored_boundary = stage.score_panel(changed_boundary, scoring_model)
    first_next = standardized["repo_full"].eq("dev/b") & standardized["week"].eq(
        standardized.loc[standardized["repo_full"].eq("dev/b"), "week"].min()
    )
    np.testing.assert_allclose(
        scored.loc[first_next, "hmm_filtered_adverse_probability"],
        rescored_boundary.loc[first_next, "hmm_filtered_adverse_probability"],
    )
    passed.append("repository transition reset")

    split = stage.split_summary(scored)
    partitions = set(split["scoring_partition"])
    assert {"train", "holdout", "external_calibration", "external_evaluation"}.issubset(partitions)
    passed.append("external calibration and evaluation separation")

    future_changed = standardized.copy()
    repository = "dev/a"
    repo_mask = future_changed["repo_full"].eq(repository)
    later = repo_mask & future_changed["week"].ge(pd.Timestamp("2025-08-04"))
    future_changed.loc[later, [f"z_{m}" for m in stage.CORE_METRICS]] = -8.0
    original_scored = stage.score_panel(standardized, stage.canonicalize_model(model))
    changed_scored = stage.score_panel(future_changed, stage.canonicalize_model(model))
    earlier = repo_mask & standardized["week"].lt(pd.Timestamp("2025-08-04"))
    np.testing.assert_allclose(
        original_scored.loc[earlier, "hmm_filtered_adverse_probability"],
        changed_scored.loc[earlier, "hmm_filtered_adverse_probability"],
    )
    assert not np.allclose(
        original_scored.loc[earlier, "hmm_smoothed_adverse_probability"],
        changed_scored.loc[earlier, "hmm_smoothed_adverse_probability"],
    )
    assert not np.array_equal(
        original_scored.loc[repo_mask, "hmm_viterbi_state"].to_numpy(),
        changed_scored.loc[repo_mask, "hmm_viterbi_state"].to_numpy(),
    )
    passed.append("panel-level filtered invariance and retrospective sensitivity")

    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        output = root / "analysis_outputs"
        output.mkdir()
        protected = output / "protected.csv"
        protected.write_text("x\n1\n", encoding="utf-8")
        manifest_path = output / "freeze.json"
        manifest = {
            "files": {
                "analysis_outputs/protected.csv": {
                    "bytes": protected.stat().st_size,
                    "sha256": stage.file_sha256(protected),
                }
            }
        }
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        freeze = stage.validate_freeze_manifest(root, manifest_path)
        assert_raises(
            RuntimeError,
            stage.validate_output_paths,
            root,
            output,
            [freeze],
            ("protected.csv",),
        )
        protected.write_text("changed", encoding="utf-8")
        assert_raises(RuntimeError, stage.validate_freeze_manifest, root, manifest_path)
    passed.append("freeze and output-collision rejection")

    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        output = root / "analysis_outputs"
        output.mkdir()
        baseline_metadata = output / "baseline_metadata.json"
        baseline_results = output / "baseline_results.csv"
        baseline_metadata.write_text("{}", encoding="utf-8")
        baseline_results.write_text("x\n1\n", encoding="utf-8")
        metadata = output / "stage_a_synthetic_metadata.json"
        approved = {
            "baseline_metadata": {
                "path": "analysis_outputs/baseline_metadata.json",
                "expected_sha256": digest(baseline_metadata),
            },
            "baseline_results": {
                "path": "analysis_outputs/baseline_results.csv",
                "expected_sha256": digest(baseline_results),
            },
        }
        metadata.write_text(
            json.dumps({"input_verification": {"approved_inputs": approved}}),
            encoding="utf-8",
        )
        checks = stage.validate_stage_a_inputs(root, output)
        assert all(item["matches"] for item in checks.values())
        baseline_results.write_text("changed", encoding="utf-8")
        assert_raises(RuntimeError, stage.validate_stage_a_inputs, root, output)
        baseline_results.unlink()
        assert_raises(RuntimeError, stage.validate_stage_a_inputs, root, output)
    passed.append("Stage-A approved input matching, mismatch, and missing rejection")

    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        output = root / "analysis_outputs"
        output.mkdir()
        manifest = {"manifest": {"files": {}}}
        assert_raises(
            RuntimeError,
            stage.validate_output_paths,
            root,
            output,
            [manifest],
            ("x.csv", "x.csv"),
        )
        assert_raises(
            RuntimeError,
            stage.validate_output_paths,
            root,
            output,
            [manifest],
            ("../x.csv",),
        )
        assert_raises(
            RuntimeError,
            stage.validate_output_paths,
            root,
            output,
            [manifest],
            (str((root / "x.csv").resolve()),),
        )
        (output / "x.csv").write_text("existing", encoding="utf-8")
        assert_raises(
            RuntimeError,
            stage.validate_output_paths,
            root,
            output,
            [manifest],
            ("x.csv",),
        )
    passed.append("duplicate, traversal, absolute, and existing output rejection")

    with tempfile.TemporaryDirectory() as temporary:
        output = Path(temporary)
        frame = pd.DataFrame({"a": [1, 2]})
        stage.atomic_csv(frame, output / "result.csv")
        stage.atomic_json({"status": "PASS"}, output / "result.json")
        assert pd.read_csv(output / "result.csv").a.tolist() == [1, 2]
        assert json.loads((output / "result.json").read_text())["status"] == "PASS"
        assert not list(output.glob("*.tmp"))
    passed.append("atomic output creation")

    with tempfile.TemporaryDirectory() as temporary:
        output = Path(temporary)
        bundle_frames = {
            "hmm_results.csv": pd.DataFrame({"x": [1]}),
            "hmm_start_diagnostics.csv": pd.DataFrame({"x": [2]}),
            "hmm_split_summary.csv": pd.DataFrame({"x": [3]}),
        }
        stage.write_output_bundle(output, bundle_frames, {"status": "PASS"})
        assert {path.name for path in output.iterdir()} == set(stage.OUTPUT_NAMES)
        assert json.loads((output / "hmm_model.json").read_text())["status"] == "PASS"
    passed.append("complete staged output bundle")

    with tempfile.TemporaryDirectory() as temporary:
        output = Path(temporary)
        bundle_frames = {
            "hmm_results.csv": pd.DataFrame({"x": [1]}),
            "hmm_start_diagnostics.csv": pd.DataFrame({"x": [2]}),
            "hmm_split_summary.csv": pd.DataFrame({"x": [3]}),
        }
        original_replace = stage.os.replace
        committed = {"count": 0}
        def failing_replace(source, destination):
            destination = Path(destination)
            if destination.parent == output and destination.name in stage.OUTPUT_NAMES:
                committed["count"] += 1
                if committed["count"] == 2:
                    raise OSError("synthetic commit failure")
            return original_replace(source, destination)
        stage.os.replace = failing_replace
        try:
            assert_raises(
                OSError,
                stage.write_output_bundle,
                output,
                bundle_frames,
                {"status": "PASS"},
            )
        finally:
            stage.os.replace = original_replace
        assert not any((output / name).exists() for name in stage.OUTPUT_NAMES)
        assert not list(output.glob("hmm_output_*"))
    passed.append("bundle rollback removes partial outputs and staging")

    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        configuration, config_path, measurement, stage_a = end_to_end_workspace(root)
        original_load_config = stage.load_config
        original_output_dir = stage.output_dir
        original_argv = sys.argv
        stage.load_config = lambda path: yaml.safe_load(Path(path).read_text())
        stage.output_dir = lambda cfg, path: root / cfg["paths"]["output"]
        sys.argv = ["04_fit_hmm.py", "--config", str(config_path)]
        try:
            assert stage.main() == 0
        finally:
            stage.load_config = original_load_config
            stage.output_dir = original_output_dir
            sys.argv = original_argv
        output = root / "analysis_outputs"
        assert {path.name for path in output.iterdir() if path.name in stage.OUTPUT_NAMES} == set(stage.OUTPUT_NAMES)
        results = pd.read_csv(output / "hmm_results.csv")
        starts = pd.read_csv(output / "hmm_start_diagnostics.csv")
        split = pd.read_csv(output / "hmm_split_summary.csv")
        model_metadata = json.loads((output / "hmm_model.json").read_text())
        required_result = {
            "hmm_filtered_adverse_probability",
            "hmm_smoothed_adverse_probability",
            "hmm_viterbi_state",
            "hmm_observed_dimensions",
            "hmm_evidence_evaluable",
            "external_calibration",
            "external_evaluation_eligible",
        }
        assert required_result.issubset(results.columns)
        assert {
            "start_index", "seed", "status", "converged",
            "likelihood_trace_json", "selected",
        }.issubset(starts.columns)
        assert {
            "repo_full", "scoring_partition", "weeks", "evaluable_weeks",
            "mean_filtered_adverse_probability",
        }.issubset(split.columns)
        assert {
            "model", "training_occupancy", "standardization_source",
            "freeze_validation", "approved_inputs", "output_preflight",
            "causal_primary_output", "retrospective_outputs",
        }.issubset(model_metadata)
        assert model_metadata["standardization_source"]["recomputed"] is False
        assert {"external_calibration", "external_evaluation"}.issubset(
            set(split["scoring_partition"])
        )
        assert model_metadata["configuration"]["states"] == 2
        configuration["hmm"]["states"] = 3
        config_path.write_text(yaml.safe_dump(configuration), encoding="utf-8")
        for name in stage.OUTPUT_NAMES:
            (output / name).unlink()
        sys.argv = ["04_fit_hmm.py", "--config", str(config_path)]
        stage.load_config = lambda path: yaml.safe_load(Path(path).read_text())
        stage.output_dir = lambda cfg, path: root / cfg["paths"]["output"]
        try:
            assert_raises(RuntimeError, stage.main)
        finally:
            stage.load_config = original_load_config
            stage.output_dir = original_output_dir
            sys.argv = original_argv
    passed.append("temporary end-to-end main, schemas, partitions, and two-state guard")

    for manifest_name, records in frozen_before.items():
        current = json.loads((ROOT / manifest_name).read_text(encoding="utf-8"))
        assert set(current["files"]) == set(records)
        for relative, expected in records.items():
            assert (digest(ROOT / relative), (ROOT / relative).stat().st_size) == expected
    research_outputs_after = {
        path.name: (digest(path), path.stat().st_size)
        for path in (ROOT / "analysis_outputs").iterdir()
        if path.is_file()
    }
    assert research_outputs_before == research_outputs_after
    passed.append("no frozen file or research output changed")

    print("HMM INVARIANCE TESTS")
    for item in passed:
        print(f"PASS: {item}")
    print(f"RESULT: PASS ({len(passed)}/{len(passed)})")


if __name__ == "__main__":
    main()