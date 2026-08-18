from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys
import tempfile
from pathlib import Path

os.environ.setdefault("LOKY_MAX_CPU_COUNT", "1")

import numpy as np
import pandas as pd
from scipy.special import logsumexp
from sklearn.cluster import KMeans

from ci_common import CORE_METRICS, load_config, output_dir


OUTPUT_NAMES = (
    "hmm_results.csv",
    "hmm_model.json",
    "hmm_start_diagnostics.csv",
    "hmm_split_summary.csv",
)

LIKELIHOOD_MONOTONICITY_TOLERANCE = 1e-8
STATE_SEVERITY_TOLERANCE = 1e-6


class CandidateSelectionError(RuntimeError):
    def __init__(self, message, diagnostics):
        super().__init__(message)
        self.diagnostics = diagnostics


def file_sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def validate_freeze_manifest(root, manifest_path):
    path = Path(manifest_path)
    if not path.is_absolute():
        path = Path(root) / path
    if not path.is_file():
        raise RuntimeError(f"Freeze manifest is absent: {path}")
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Invalid freeze manifest: {path}: {exc}") from exc
    files = manifest.get("files")
    if not isinstance(files, dict) or not files:
        raise RuntimeError(f"Freeze manifest has no file records: {path}")
    failures = []
    for relative, record in files.items():
        candidate = Path(root) / relative
        if not candidate.is_file():
            failures.append(f"{relative}: missing")
        elif candidate.stat().st_size != int(record.get("bytes", -1)):
            failures.append(f"{relative}: byte size differs")
        elif file_sha256(candidate) != str(record.get("sha256", "")):
            failures.append(f"{relative}: SHA-256 differs")
    if failures:
        raise RuntimeError(
            f"Freeze validation failed for {path.name}: "
            + "; ".join(failures)
        )
    return {
        "path": str(path.relative_to(root).as_posix()),
        "sha256": file_sha256(path),
        "files_verified": len(files),
        "manifest": manifest,
    }


def validate_stage_a_inputs(root, output_directory):
    metadata_path = Path(output_directory) / "stage_a_synthetic_metadata.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    approved = metadata["input_verification"]["approved_inputs"]
    required = ("baseline_metadata", "baseline_results")
    checks = {}
    for label in required:
        record = approved[label]
        path = Path(root) / record["path"]
        actual = file_sha256(path) if path.is_file() else None
        checks[label] = {
            "path": record["path"],
            "expected_sha256": record["expected_sha256"],
            "actual_sha256": actual,
            "matches": actual == record["expected_sha256"],
        }
    failed = [label for label, record in checks.items() if not record["matches"]]
    if failed:
        raise RuntimeError("Frozen baseline inputs differ: " + ", ".join(failed))
    return checks


def validate_output_paths(root, output_directory, manifests, output_names=OUTPUT_NAMES):
    if len(output_names) != len(set(output_names)):
        raise RuntimeError("HMM output names contain duplicates")
    output_directory = Path(output_directory).resolve()
    frozen = {
        (Path(root) / relative).resolve()
        for item in manifests
        for relative in item["manifest"]["files"]
    }
    intended = []
    for name in output_names:
        value = Path(name)
        if value.is_absolute() or value.parent != Path(".") or value.name != name:
            raise RuntimeError(f"HMM output is not a plain filename: {name}")
        path = (output_directory / name).resolve()
        if path.parent != output_directory:
            raise RuntimeError(f"HMM output is outside output directory: {name}")
        if path in frozen:
            raise RuntimeError(f"HMM output collides with frozen path: {name}")
        if path.exists():
            raise RuntimeError(f"Refusing to overwrite HMM output: {name}")
        intended.append(path)
    return {
        "status": "PASS",
        "protected_frozen_path_count": len(frozen),
        "intended_output_count": len(intended),
        "output_directory": str(output_directory),
    }


def atomic_csv(frame, path):
    path = Path(path)
    handle = tempfile.NamedTemporaryFile(
        mode="w", dir=path.parent, suffix=".tmp", encoding="utf-8",
        newline="", delete=False,
    )
    temporary = Path(handle.name)
    handle.close()
    try:
        frame.to_csv(temporary, index=False, date_format="%Y-%m-%d")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def atomic_json(payload, path):
    path = Path(path)
    handle = tempfile.NamedTemporaryFile(
        mode="w", dir=path.parent, suffix=".tmp", encoding="utf-8",
        delete=False,
    )
    temporary = Path(handle.name)
    try:
        json.dump(payload, handle, indent=2, ensure_ascii=False, allow_nan=False)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
        handle.close()
        os.replace(temporary, path)
    finally:
        if not handle.closed:
            handle.close()
        temporary.unlink(missing_ok=True)


def emission_logprob(x, means, variances):
    values = np.asarray(x, dtype=float)
    means = np.asarray(means, dtype=float)
    variances = np.asarray(variances, dtype=float)
    out = np.zeros((len(values), len(means)), dtype=float)
    for index, row in enumerate(values):
        observed = np.isfinite(row)
        if not observed.any():
            continue
        difference = row[observed][None, :] - means[:, observed]
        out[index] = -0.5 * np.sum(
            np.log(2 * np.pi * variances[:, observed])
            + difference**2 / variances[:, observed],
            axis=1,
        )
    return out


def forward_backward(x, pi, trans, means, variances):
    emit = emission_logprob(x, means, variances)
    if not len(emit):
        raise ValueError("HMM sequence is empty")
    logpi = np.log(np.asarray(pi, dtype=float) + 1e-300)
    log_transition = np.log(np.asarray(trans, dtype=float) + 1e-300)
    alpha = np.empty_like(emit)
    alpha[0] = logpi + emit[0]
    for position in range(1, len(emit)):
        alpha[position] = emit[position] + logsumexp(
            alpha[position - 1][:, None] + log_transition,
            axis=0,
        )
    likelihood = float(logsumexp(alpha[-1]))
    beta = np.zeros_like(emit)
    for position in range(len(emit) - 2, -1, -1):
        beta[position] = logsumexp(
            log_transition
            + emit[position + 1][None, :]
            + beta[position + 1][None, :],
            axis=1,
        )
    smoothed = np.exp(alpha + beta - likelihood)
    smoothed /= smoothed.sum(axis=1, keepdims=True)
    transitions = []
    for position in range(len(emit) - 1):
        value = (
            alpha[position][:, None]
            + log_transition
            + emit[position + 1][None, :]
            + beta[position + 1][None, :]
            - likelihood
        )
        transitions.append(np.exp(value))
    xi = np.asarray(transitions)
    if not len(transitions):
        xi = np.empty((0, len(pi), len(pi)))
    filtered = np.exp(alpha - logsumexp(alpha, axis=1, keepdims=True))
    return likelihood, smoothed, xi, filtered


def validate_model(model, tolerance=1e-10):
    pi = np.asarray(model["pi"], dtype=float)
    transition = np.asarray(model["trans"], dtype=float)
    means = np.asarray(model["means"], dtype=float)
    variances = np.asarray(model["variances"], dtype=float)
    states = len(pi)
    if transition.shape != (states, states):
        raise ValueError("Transition matrix shape is invalid")
    if means.shape != variances.shape or means.shape[0] != states:
        raise ValueError("Emission parameter shapes are invalid")
    if not all(np.isfinite(value).all() for value in (pi, transition, means, variances)):
        raise ValueError("HMM parameters are not finite")
    if (pi < 0).any() or (transition < 0).any():
        raise ValueError("HMM probabilities are negative")
    if not np.isclose(pi.sum(), 1.0, atol=tolerance):
        raise ValueError("Initial probabilities do not sum to one")
    if not np.allclose(transition.sum(axis=1), 1.0, atol=tolerance):
        raise ValueError("Transition rows do not sum to one")
    if (variances <= 0).any():
        raise ValueError("Emission variances must be positive")


def initialise(sequences, seed, states):
    complete = np.vstack(sequences)
    medians = np.nanmedian(complete, axis=0)
    if not np.isfinite(medians).all():
        raise ValueError("A training metric has no finite observations")
    filled = np.where(np.isfinite(complete), complete, medians)
    labels = KMeans(
        states,
        n_init=10,
        random_state=int(seed),
    ).fit_predict(filled)
    if len(np.unique(labels)) != states:
        raise ValueError("Initialization did not populate every state")
    means = np.vstack([
        np.nanmean(complete[labels == state], axis=0)
        for state in range(states)
    ])
    variances = np.vstack([
        np.nanvar(complete[labels == state], axis=0) + 0.25
        for state in range(states)
    ])
    pi = np.full(states, 1.0 / states)
    transition = np.full((states, states), 0.1 / max(states - 1, 1))
    np.fill_diagonal(transition, 0.9 if states > 1 else 1.0)
    transition /= transition.sum(axis=1, keepdims=True)
    model = {
        "pi": pi,
        "trans": transition,
        "means": means,
        "variances": variances,
    }
    validate_model(model)
    return model


def total_log_likelihood(sequences, model):
    return float(sum(
        forward_backward(
            sequence,
            model["pi"],
            model["trans"],
            model["means"],
            model["variances"],
        )[0]
        for sequence in sequences
    ))


def fit(sequences, seed, max_iter, tolerance, states=2):
    model = initialise(sequences, seed, states)
    likelihood_trace = [total_log_likelihood(sequences, model)]
    converged = False
    monotonic = True
    for iteration in range(1, int(max_iter) + 1):
        pi_sum = np.zeros(states)
        transition_numerator = np.zeros((states, states))
        transition_denominator = np.zeros(states)
        mean_numerator = np.zeros_like(model["means"])
        second_numerator = np.zeros_like(model["means"])
        mean_denominator = np.zeros_like(model["means"])
        for sequence in sequences:
            _, smoothed, xi, _ = forward_backward(
                sequence,
                model["pi"], model["trans"],
                model["means"], model["variances"],
            )
            pi_sum += smoothed[0]
            if len(xi):
                transition_numerator += xi.sum(axis=0)
                transition_denominator += smoothed[:-1].sum(axis=0)
            for dimension in range(sequence.shape[1]):
                observed = np.isfinite(sequence[:, dimension])
                if observed.any():
                    weights = smoothed[observed]
                    values = sequence[observed, dimension, None]
                    mean_numerator[:, dimension] += (weights * values).sum(axis=0)
                    second_numerator[:, dimension] += (weights * values**2).sum(axis=0)
                    mean_denominator[:, dimension] += weights.sum(axis=0)
        updated = {
            "pi": (pi_sum + 0.01) / (pi_sum.sum() + 0.01 * states),
            "trans": (
                (transition_numerator + 0.5)
                / (transition_denominator[:, None] + 0.5 * states)
            ),
            "means": mean_numerator / np.maximum(mean_denominator, 1e-12),
            "variances": np.maximum(
                second_numerator / np.maximum(mean_denominator, 1e-12)
                - (mean_numerator / np.maximum(mean_denominator, 1e-12))**2,
                0.05,
            ),
        }
        validate_model(updated)
        likelihood = total_log_likelihood(sequences, updated)
        if not np.isfinite(likelihood):
            raise ValueError("HMM likelihood is not finite")
        allowed_drop = LIKELIHOOD_MONOTONICITY_TOLERANCE * (
            1 + abs(likelihood_trace[-1])
        )
        if likelihood < likelihood_trace[-1] - allowed_drop:
            monotonic = False
            raise ValueError("HMM likelihood decreased beyond numerical tolerance")
        likelihood_trace.append(likelihood)
        model = updated
        if abs(likelihood_trace[-1] - likelihood_trace[-2]) <= float(tolerance) * (
            1 + abs(likelihood_trace[-2])
        ):
            converged = True
            break
    model.update({
        "log_likelihood": total_log_likelihood(sequences, model),
        "iterations": iteration,
        "converged": converged,
        "likelihood_trace": likelihood_trace,
        "likelihood_monotonic": monotonic,
        "seed": int(seed),
    })
    return model


def filtered_probabilities(x, model):
    return forward_backward(
        x, model["pi"], model["trans"],
        model["means"], model["variances"],
    )[3]


def smoothed_probabilities(x, model):
    return forward_backward(
        x, model["pi"], model["trans"],
        model["means"], model["variances"],
    )[1]


def viterbi(x, model):
    emit = emission_logprob(x, model["means"], model["variances"])
    transition = np.log(model["trans"] + 1e-300)
    delta = np.empty_like(emit)
    previous = np.zeros_like(emit, dtype=int)
    delta[0] = np.log(model["pi"] + 1e-300) + emit[0]
    for position in range(1, len(x)):
        scores = delta[position - 1][:, None] + transition
        previous[position] = np.argmax(scores, axis=0)
        delta[position] = emit[position] + np.max(scores, axis=0)
    states = np.zeros(len(x), dtype=int)
    states[-1] = int(np.argmax(delta[-1]))
    for position in range(len(x) - 2, -1, -1):
        states[position] = previous[position + 1, states[position + 1]]
    return states


def canonicalize_model(model, severity_tolerance=STATE_SEVERITY_TOLERANCE):
    severity = np.asarray(model["means"], dtype=float).sum(axis=1)
    order = np.argsort(severity, kind="stable")
    ordered_severity = severity[order]
    if len(ordered_severity) > 1 and np.min(np.diff(ordered_severity)) <= severity_tolerance:
        raise ValueError("Adverse-state severity ordering is ambiguous")
    result = dict(model)
    result["pi"] = np.asarray(model["pi"])[order]
    result["trans"] = np.asarray(model["trans"])[np.ix_(order, order)]
    result["means"] = np.asarray(model["means"])[order]
    result["variances"] = np.asarray(model["variances"])[order]
    result["state_severity_scores"] = ordered_severity
    result["canonical_order_from_original"] = order
    result["adverse_state"] = len(order) - 1
    validate_model(result)
    return result


def training_occupancy(sequences, model):
    state_count = len(model["pi"])
    smoothed_totals = np.zeros(state_count)
    viterbi_totals = np.zeros(state_count)
    rows = []
    for repo, sequence in sequences.items():
        smoothed = smoothed_probabilities(sequence, model)
        states = viterbi(sequence, model)
        smoothed_occupancy = smoothed.mean(axis=0)
        viterbi_occupancy = np.bincount(states, minlength=state_count) / len(states)
        smoothed_totals += smoothed.sum(axis=0)
        viterbi_totals += np.bincount(states, minlength=state_count)
        rows.append({
            "repo_full": repo,
            "weeks": len(sequence),
            "smoothed_occupancy": smoothed_occupancy.tolist(),
            "viterbi_occupancy": viterbi_occupancy.tolist(),
        })
    total_weeks = sum(len(sequence) for sequence in sequences.values())
    return {
        "pooled_smoothed": (smoothed_totals / total_weeks).tolist(),
        "pooled_viterbi": (viterbi_totals / total_weeks).tolist(),
        "by_repository": rows,
    }


def serialise(value):
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, dict):
        return {key: serialise(item) for key, item in value.items()}
    if isinstance(value, list):
        return [serialise(item) for item in value]
    return value


def load_json(path):
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Cannot read JSON file {path}: {exc}") from exc


def frozen_standardization(metadata):
    candidates = [
        metadata.get("mewma", {}).get("standardization")
        if isinstance(metadata.get("mewma"), dict) else None,
        metadata.get("standardization"),
        metadata.get("standardization_parameters"),
        metadata.get("parameters"),
    ]
    for candidate in candidates:
        if isinstance(candidate, dict) and candidate:
            return candidate
    raise RuntimeError("Frozen baseline metadata has no standardization parameters")


def metric_parameters(repository_parameters, metric):
    metrics = repository_parameters.get("metrics", repository_parameters)
    item = metrics.get(metric)
    if not isinstance(item, dict):
        raise RuntimeError(f"Frozen parameters are missing metric {metric}")
    median = float(item.get("median", np.nan))
    scale = float(item.get("scale", np.nan))
    if not np.isfinite(median) or not np.isfinite(scale) or scale <= 0:
        raise RuntimeError(f"Invalid frozen parameters for {metric}")
    return median, scale


def apply_frozen_standardization(panel, parameters):
    result = panel.copy()
    for repository, indexes in result.groupby("repo_full", sort=False).groups.items():
        if repository not in parameters:
            raise RuntimeError(f"No frozen standardization for {repository}")
        for metric in CORE_METRICS:
            median, scale = metric_parameters(parameters[repository], metric)
            values = pd.to_numeric(result.loc[indexes, metric], errors="coerce")
            result.loc[indexes, f"z_{metric}"] = (values - median) / scale
    return result


def validate_panel(panel, cfg):
    required = {
        "repo_full", "week", "split", *CORE_METRICS,
        "external_calibration", "external_evaluation_eligible",
    }
    missing = sorted(required - set(panel.columns))
    if missing:
        raise RuntimeError("Baseline panel is missing: " + ", ".join(missing))
    result = panel.copy()
    result["week"] = pd.to_datetime(result["week"], errors="coerce")
    if result["week"].isna().any():
        raise RuntimeError("Baseline panel contains invalid weeks")
    result = result.sort_values(["repo_full", "week"], kind="stable").reset_index(drop=True)
    if result.duplicated(["repo_full", "week"]).any():
        raise RuntimeError("Baseline panel contains duplicate repository-week keys")
    if not result["week"].dt.weekday.eq(0).all():
        raise RuntimeError("Baseline panel weeks are not Monday anchored")
    expected = set(cfg["study"]["development_repositories"]) | set(
        cfg["study"]["external_repositories"]
    )
    if set(result["repo_full"]) != expected:
        raise RuntimeError("Baseline panel repository set differs from configuration")
    for repository, group in result.groupby("repo_full", sort=False):
        differences = group["week"].diff().dropna().dt.days
        if not differences.eq(7).all():
            raise RuntimeError(f"Repository sequence is not weekly-contiguous: {repository}")
    return result


def build_training_sequences(panel, cfg):
    development = set(cfg["study"]["development_repositories"])
    sequences = {}
    for repository in cfg["study"]["development_repositories"]:
        group = panel[
            panel["repo_full"].eq(repository) & panel["split"].eq("train")
        ].sort_values("week")
        if group.empty:
            raise RuntimeError(f"No training observations for {repository}")
        if repository not in development:
            raise RuntimeError("External repository entered HMM training")
        sequences[repository] = group[[f"z_{m}" for m in CORE_METRICS]].to_numpy(float)
    return sequences


def fit_multistart(sequences, settings):
    records = []
    candidates = []
    sequence_values = list(sequences.values())
    minimum_occupancy = float(settings["minimum_state_occupancy"])
    for start in range(int(settings["random_starts"])):
        seed = int(settings["random_seed"]) + start
        record = {
            "start_index": start,
            "seed": seed,
            "status": "failed",
            "rejection_reason": "",
            "iterations": np.nan,
            "converged": False,
            "likelihood_monotonic": False,
            "log_likelihood": np.nan,
            "likelihood_trace_json": "[]",
            "parameter_valid": False,
            "canonicalization_valid": False,
            "state_severity_scores_json": "[]",
            "minimum_severity_gap": np.nan,
            "pooled_smoothed_occupancy_state_0": np.nan,
            "pooled_smoothed_occupancy_state_1": np.nan,
            "pooled_viterbi_occupancy_state_0": np.nan,
            "pooled_viterbi_occupancy_state_1": np.nan,
            "minimum_occupancy_threshold": minimum_occupancy,
            "occupancy_valid": False,
            "selected": False,
            "error": "",
        }
        try:
            candidate = fit(
                sequence_values,
                seed,
                int(settings["maximum_iterations"]),
                float(settings["tolerance"]),
                int(settings["states"]),
            )
            record.update({
                "iterations": int(candidate["iterations"]),
                "converged": bool(candidate["converged"]),
                "likelihood_monotonic": bool(candidate["likelihood_monotonic"]),
                "log_likelihood": float(candidate["log_likelihood"]),
                "likelihood_trace_json": json.dumps(
                    [float(value) for value in candidate["likelihood_trace"]]
                ),
            })
            if not np.isfinite(record["log_likelihood"]):
                record.update({
                    "status": "nonfinite",
                    "rejection_reason": "final log likelihood is not finite",
                })
                records.append(record)
                continue
            try:
                validate_model(candidate)
                record["parameter_valid"] = True
            except Exception as exc:
                record.update({
                    "status": "invalid_parameters",
                    "rejection_reason": f"{type(exc).__name__}: {exc}",
                })
                records.append(record)
                continue
            if not candidate["converged"]:
                record.update({
                    "status": "nonconverged",
                    "rejection_reason": "maximum iterations reached without convergence",
                })
                records.append(record)
                continue
            if not candidate["likelihood_monotonic"]:
                record.update({
                    "status": "nonmonotonic",
                    "rejection_reason": "likelihood trace is not monotonic",
                })
                records.append(record)
                continue
            try:
                canonical = canonicalize_model(candidate)
                record["canonicalization_valid"] = True
                severity = np.asarray(
                    canonical["state_severity_scores"], dtype=float
                )
                record["state_severity_scores_json"] = json.dumps(
                    severity.tolist()
                )
                record["minimum_severity_gap"] = float(
                    np.min(np.diff(severity))
                )
            except Exception as exc:
                record.update({
                    "status": "ambiguous_states",
                    "rejection_reason": f"{type(exc).__name__}: {exc}",
                })
                records.append(record)
                continue
            occupancy = training_occupancy(sequences, canonical)
            smoothed = np.asarray(occupancy["pooled_smoothed"], dtype=float)
            retrospective = np.asarray(occupancy["pooled_viterbi"], dtype=float)
            record.update({
                "pooled_smoothed_occupancy_state_0": float(smoothed[0]),
                "pooled_smoothed_occupancy_state_1": float(smoothed[1]),
                "pooled_viterbi_occupancy_state_0": float(retrospective[0]),
                "pooled_viterbi_occupancy_state_1": float(retrospective[1]),
            })
            occupancy_valid = bool(
                np.isfinite(smoothed).all()
                and (smoothed >= minimum_occupancy).all()
                and (smoothed <= 1 - minimum_occupancy).all()
            )
            record["occupancy_valid"] = occupancy_valid
            if not occupancy_valid:
                record.update({
                    "status": "occupancy_invalid",
                    "rejection_reason": (
                        "pooled smoothed occupancy violates frozen minimum "
                        f"{minimum_occupancy}"
                    ),
                })
                records.append(record)
                continue
            record["status"] = "admissible"
            candidates.append((start, canonical, occupancy))
        except Exception as exc:
            record.update({
                "status": "failed",
                "rejection_reason": f"{type(exc).__name__}: {exc}",
                "error": f"{type(exc).__name__}: {exc}",
            })
        records.append(record)
    diagnostics = pd.DataFrame(records)
    if not candidates:
        raise CandidateSelectionError(
            "No HMM random start satisfied all frozen admissibility criteria",
            diagnostics,
        )
    selected_start, selected, selected_occupancy = max(
        candidates,
        key=lambda item: item[1]["log_likelihood"],
    )
    diagnostics.loc[
        diagnostics["start_index"].eq(selected_start), "selected"
    ] = True
    selected["training_occupancy"] = selected_occupancy
    return selected, diagnostics, selected_start


def enforce_training_occupancy(sequences, model, minimum):
    occupancy = training_occupancy(sequences, model)
    values = np.asarray(occupancy["pooled_smoothed"], dtype=float)
    if (values < float(minimum)).any() or (values > 1 - float(minimum)).any():
        raise RuntimeError("HMM violates minimum training-state occupancy")
    return occupancy


def score_panel(panel, model):
    result = panel.copy()
    adverse = int(model["adverse_state"])
    probability_columns = [
        "hmm_filtered_adverse_probability",
        "hmm_smoothed_adverse_probability",
    ]
    for column in probability_columns:
        result[column] = np.nan
    result["hmm_viterbi_state"] = pd.Series(pd.NA, index=result.index, dtype="Int64")
    result["hmm_observed_dimensions"] = 0
    result["hmm_evidence_evaluable"] = False
    for repository, indexes in result.groupby("repo_full", sort=False).groups.items():
        ordered = result.loc[indexes].sort_values("week")
        ordered_indexes = ordered.index
        values = ordered[[f"z_{m}" for m in CORE_METRICS]].to_numpy(float)
        observed = np.isfinite(values).sum(axis=1)
        filtered = filtered_probabilities(values, model)
        smoothed = smoothed_probabilities(values, model)
        states = viterbi(values, model)
        result.loc[ordered_indexes, probability_columns[0]] = filtered[:, adverse]
        result.loc[ordered_indexes, probability_columns[1]] = smoothed[:, adverse]
        result.loc[ordered_indexes, "hmm_viterbi_state"] = states
        result.loc[ordered_indexes, "hmm_observed_dimensions"] = observed
        result.loc[ordered_indexes, "hmm_evidence_evaluable"] = observed > 0
    result["hmm_viterbi_adverse"] = result["hmm_viterbi_state"].eq(adverse)
    result["hmm_filtered_is_causal"] = True
    result["hmm_smoothed_is_retrospective"] = True
    result["hmm_viterbi_is_retrospective"] = True
    return result


def split_summary(panel):
    values = panel.copy()
    values["scoring_partition"] = values["split"].astype("string")
    external = values["split"].eq("external")
    values.loc[
        external & values["external_calibration"].fillna(False).astype(bool),
        "scoring_partition",
    ] = "external_calibration"
    values.loc[
        external
        & values["external_evaluation_eligible"].fillna(False).astype(bool),
        "scoring_partition",
    ] = "external_evaluation"
    values.loc[
        external
        & ~values["external_calibration"].fillna(False).astype(bool)
        & ~values["external_evaluation_eligible"].fillna(False).astype(bool),
        "scoring_partition",
    ] = "external_other"
    rows = []
    for (repository, partition), group in values.groupby(
        ["repo_full", "scoring_partition"], dropna=False
    ):
        evaluable = group["hmm_evidence_evaluable"].fillna(False)
        rows.append({
            "repo_full": repository,
            "scoring_partition": partition,
            "weeks": len(group),
            "evaluable_weeks": int(evaluable.sum()),
            "unevaluable_weeks": int((~evaluable).sum()),
            "mean_filtered_adverse_probability": group.loc[
                evaluable, "hmm_filtered_adverse_probability"
            ].mean(),
            "mean_smoothed_adverse_probability": group.loc[
                evaluable, "hmm_smoothed_adverse_probability"
            ].mean(),
            "viterbi_adverse_fraction": group.loc[
                evaluable, "hmm_viterbi_adverse"
            ].mean(),
        })
    return pd.DataFrame(rows)


def write_output_bundle(output_directory, frames, metadata):
    output_directory = Path(output_directory)
    staging = Path(tempfile.mkdtemp(prefix="hmm_output_", dir=output_directory))
    committed = []
    try:
        atomic_csv(frames["hmm_results.csv"], staging / "hmm_results.csv")
        atomic_csv(
            frames["hmm_start_diagnostics.csv"],
            staging / "hmm_start_diagnostics.csv",
        )
        atomic_csv(
            frames["hmm_split_summary.csv"], staging / "hmm_split_summary.csv"
        )
        atomic_json(metadata, staging / "hmm_model.json")
        for name in OUTPUT_NAMES:
            path = staging / name
            if not path.is_file() or path.stat().st_size == 0:
                raise RuntimeError(f"Staged HMM output is invalid: {name}")
        json.loads((staging / "hmm_model.json").read_text(encoding="utf-8"))
        for name in OUTPUT_NAMES:
            destination = output_directory / name
            if destination.exists():
                raise RuntimeError(f"Refusing to overwrite HMM output: {name}")
            os.replace(staging / name, destination)
            committed.append(destination)
    except Exception:
        for path in committed:
            path.unlink(missing_ok=True)
        raise
    finally:
        shutil.rmtree(staging, ignore_errors=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="analysis_config.yaml")
    args = parser.parse_args()
    config_path = Path(args.config).resolve()
    root = config_path.parent
    cfg = load_config(config_path)
    out = output_dir(cfg, config_path)

    if int(cfg["hmm"]["states"]) != 2:
        raise RuntimeError("The frozen research design requires exactly two HMM states")

    measurement = validate_freeze_manifest(
        root, root / "analysis_outputs" / "MEASUREMENT_FROZEN.json"
    )
    stage_a = validate_freeze_manifest(
        root, root / "analysis_outputs" / "STAGE_A_FROZEN.json"
    )
    approved_inputs = validate_stage_a_inputs(root, out)
    output_preflight = validate_output_paths(root, out, [measurement, stage_a])

    baseline_path = Path(out) / "baseline_results.csv"
    metadata_path = Path(out) / "baseline_metadata.json"
    baseline_metadata = load_json(metadata_path)
    parameters = frozen_standardization(baseline_metadata)
    panel = validate_panel(pd.read_csv(baseline_path), cfg)
    panel = apply_frozen_standardization(panel, parameters)
    sequences = build_training_sequences(panel, cfg)

    try:
        model, starts, selected_start = fit_multistart(sequences, cfg["hmm"])
    except CandidateSelectionError as exc:
        print("HMM CANDIDATE DIAGNOSTICS", file=sys.stderr)
        print(exc.diagnostics.to_string(index=False), file=sys.stderr)
        raise
    occupancy = model.pop("training_occupancy")
    enforce_training_occupancy(
        sequences, model, cfg["hmm"]["minimum_state_occupancy"]
    )
    results = score_panel(panel, model)
    summary = split_summary(results)

    metadata = {
        "status": "PASS",
        "model": serialise(model),
        "selected_start_index": int(selected_start),
        "training_occupancy": serialise(occupancy),
        "standardization_source": {
            "path": Path(os.path.relpath(metadata_path, root)).as_posix(),
            "sha256": file_sha256(metadata_path),
            "parameters": serialise(parameters),
            "recomputed": False,
        },
        "freeze_validation": {
            "measurement": {k: v for k, v in measurement.items() if k != "manifest"},
            "stage_a": {k: v for k, v in stage_a.items() if k != "manifest"},
        },
        "approved_inputs": approved_inputs,
        "output_preflight": output_preflight,
        "configuration": serialise(cfg["hmm"]),
        "training_repositories": list(sequences),
        "core_metrics": list(CORE_METRICS),
        "causal_primary_output": "hmm_filtered_adverse_probability",
        "retrospective_outputs": [
            "hmm_smoothed_adverse_probability", "hmm_viterbi_state"
        ],
        "input_hashes": {
            "baseline_results": file_sha256(baseline_path),
            "baseline_metadata": file_sha256(metadata_path),
            "analysis_config": file_sha256(config_path),
        },
    }

    write_output_bundle(
        out,
        {
            "hmm_results.csv": results,
            "hmm_start_diagnostics.csv": starts,
            "hmm_split_summary.csv": summary,
        },
        serialise(metadata),
    )
    print(json.dumps({
        "status": "PASS",
        "selected_start_index": selected_start,
        "training_repositories": list(sequences),
        "result_rows": len(results),
        "split_summary_rows": len(summary),
    }, indent=2))
    return 0


if __name__=="__main__": raise SystemExit(main())