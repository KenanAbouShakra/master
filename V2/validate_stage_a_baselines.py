from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import importlib.util
import io
import json
import os
import tempfile
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd
import yaml


CORE_METRICS = ("latency_log", "failure_rate", "rerun_rate")
OUTPUT_NAMES = (
    "stage_a_synthetic_scenarios.csv",
    "stage_a_synthetic_week_results.csv.gz",
    "stage_a_synthetic_detector_results.csv.gz",
    "stage_a_synthetic_summary.csv",
    "stage_a_synthetic_metadata.json",
)
WEEK_RESULT_FIELDS = (
    "scenario_id", "repo_full", "week", "truth", "affected_metrics",
    "volume_condition", "missingness_condition", "attempts_total_before",
    "attempts_total_after", "outcome_n_before", "outcome_n_after",
    "latency_n_before", "latency_n_after",
    "failure_count_before", "failure_count_after", "logical_run_n_before",
    "logical_run_n_after", "rerun_count_before", "rerun_count_after",
    "latency_log_before", "latency_log_after", "failure_rate_before",
    "failure_rate_injection_base", "failure_rate_preclip",
    "failure_rate_postclip", "failure_rate_after", "rerun_rate_before",
    "rerun_rate_injection_base", "rerun_rate_preclip",
    "rerun_rate_postclip", "rerun_rate_after",
    "probability_clipping_count", "missing_metrics",
)
DETECTOR_RESULT_FIELDS = (
    "scenario_id", "repo_full", "detector_family", "detector_variant",
    "metric", "window_weeks", "window_role", "threshold",
    "threshold_role", "persistence_weeks", "persistence_role",
    "scenario_type", "affected_metrics", "magnitude_level",
    "magnitude", "duration_weeks", "volume_condition",
    "missingness_condition", "repetition", "tp", "fp", "tn", "fn",
    "precision", "recall", "false_alarm_rate", "episode_detected",
    "episode_detection_rate", "detection_delay_weeks", "boundary_overlap",
    "alarm_duration_weeks", "evaluable_weeks", "unevaluable_weeks",
    "unevaluable_fraction", "truth_evaluable_weeks",
    "truth_unevaluable_weeks", "predicted_alarm_weeks",
)
SUMMARY_KEYS = (
    "repo_full", "detector_family", "detector_variant", "metric",
    "window_weeks", "window_role", "threshold", "threshold_role",
    "persistence_weeks", "persistence_role", "scenario_type",
    "affected_metrics", "magnitude_level", "magnitude",
    "duration_weeks", "volume_condition", "missingness_condition",
)
SUMMARY_METRICS = (
    "precision", "recall", "false_alarm_rate", "episode_detection_rate",
    "detection_delay_weeks", "boundary_overlap", "alarm_duration_weeks",
    "unevaluable_fraction",
)


def sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def stable_seed(base_seed: int, *parts: object) -> int:
    text = "|".join([str(base_seed), *(str(part) for part in parts)])
    return int.from_bytes(
        hashlib.sha256(text.encode("utf-8")).digest()[:8],
        "big",
    )


def load_yaml(path: Path) -> dict:
    value = yaml.safe_load(path.read_text(encoding="utf-8-sig"))
    if not isinstance(value, dict):
        raise ValueError(f"Configuration root must be a mapping: {path}")
    return value


def import_baseline_module(path: Path):
    spec = importlib.util.spec_from_file_location("stage_a_baselines", path)
    if spec is None or spec.loader is None:
        raise ImportError(path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def verify_inputs(root: Path, stage_cfg: dict) -> dict:
    approved = stage_cfg["approved_inputs"]
    checks: dict[str, dict[str, Any]] = {}
    for label, item in approved.items():
        path = root / item["path"]
        actual = sha256(path) if path.is_file() else None
        checks[label] = {
            "path": item["path"],
            "exists": path.is_file(),
            "expected_sha256": str(item["sha256"]).lower(),
            "actual_sha256": actual,
            "matches": actual == str(item["sha256"]).lower(),
        }
    failures = [label for label, item in checks.items() if not item["matches"]]
    if failures:
        raise RuntimeError("Approved Stage-A inputs differ: " + ", ".join(failures))

    manifest_path = root / approved["measurement_manifest"]["path"]
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    frozen_failures: list[str] = []
    for relative, record in manifest["files"].items():
        path = root / relative
        if not path.is_file():
            frozen_failures.append(f"{relative}: missing")
        elif path.stat().st_size != int(record["bytes"]):
            frozen_failures.append(f"{relative}: size")
        elif sha256(path) != str(record["sha256"]):
            frozen_failures.append(f"{relative}: hash")
    if frozen_failures:
        raise RuntimeError("Measurement freeze differs: " + "; ".join(frozen_failures))
    return {
        "approved_inputs": checks,
        "measurement_files_verified": len(manifest["files"]),
        "measurement_manifest_sha256": sha256(manifest_path),
    }


def validate_frozen_detector(
    analysis_cfg: dict,
    stage_cfg: dict,
    baseline_metadata: dict,
) -> None:
    design = stage_cfg["scenario_design"]
    synthetic = analysis_cfg["synthetic_validation"]
    comparisons = {
        "random_seed": (design["random_seed"], synthetic["random_seed"]),
        "repetitions": (design["repetitions"], synthetic["repetitions"]),
        "latency_relative_shifts": (
            design["latency_relative_shifts"],
            synthetic["latency_relative_shifts"],
        ),
        "failure_probability_shifts": (
            design["failure_probability_shifts"],
            synthetic["probability_absolute_shifts"],
        ),
        "rerun_probability_shifts": (
            design["rerun_probability_shifts"],
            synthetic["probability_absolute_shifts"],
        ),
        "durations_weeks": (
            design["durations_weeks"],
            synthetic["durations_weeks"],
        ),
    }
    mismatches = [name for name, values in comparisons.items() if values[0] != values[1]]
    if mismatches:
        raise RuntimeError("Stage-A design differs from frozen analysis config: " + ", ".join(mismatches))

    expected_mad = {
        "primary_window_weeks": 13,
        "sensitivity_windows_weeks": [26],
        "thresholds": [2.5, 3.0, 3.5],
        "primary_threshold": 3.0,
        "persistence_weeks": [1, 2, 3, 4],
        "primary_persistence_weeks": 2,
    }
    for key, expected in expected_mad.items():
        actual = analysis_cfg["mad"][key]
        if actual != expected or baseline_metadata["mad"][key] != expected:
            raise RuntimeError(f"Frozen MAD setting differs: {key}")

    mewma = baseline_metadata["mewma"]
    if float(mewma["lambda"]) != 0.20:
        raise RuntimeError("Frozen MEWMA lambda differs")
    if mewma["control_limit_reference"] != "development_training_rows_only":
        raise RuntimeError("MEWMA control limit is not training-only")
    if not np.isfinite(float(mewma["empirical_limit"])):
        raise RuntimeError("Frozen MEWMA limit is not finite")


def magnitude_mapping(design: dict, level: str) -> dict[str, float]:
    index = {"low": 0, "medium": 1, "high": 2}[level]
    return {
        "latency_log": float(design["latency_relative_shifts"][index]),
        "failure_rate": float(design["failure_probability_shifts"][index]),
        "rerun_rate": float(design["rerun_probability_shifts"][index]),
    }


def build_scenario_grid(panel: pd.DataFrame, stage_cfg: dict) -> pd.DataFrame:
    design = stage_cfg["scenario_design"]
    base_seed = int(design["random_seed"])
    rows: list[dict[str, Any]] = []
    scenario_number = 0

    for repo, sequence in panel.groupby("repo_full", sort=True):
        sequence = sequence.sort_values("week").reset_index(drop=True)
        sequence_weeks = len(sequence)
        for repetition in range(int(design["repetitions"])):
            scenario_number += 1
            rows.append({
                "scenario_id": f"S{scenario_number:09d}",
                "repo_full": repo,
                "scenario_type": "no_injection",
                "affected_metrics": "none",
                "magnitude_level": "none",
                "magnitude": "{}",
                "duration_weeks": 0,
                "volume_condition": "observed",
                "missingness_condition": "none",
                "repetition": repetition,
                "seed": stable_seed(base_seed, repo, "control", repetition),
                "start_index": None,
                "end_index": None,
                "injected_weeks": "[]",
                "status": "evaluable",
                "unsupported_reason": "",
            })

        for combination in design["signal_combinations"]:
            affected = "+".join(combination)
            for duration_value in design["durations_weeks"]:
                duration = int(duration_value)
                valid_starts = list(range(
                    int(design["earliest_injection_index"]),
                    sequence_weeks - duration + 1,
                ))
                if not valid_starts:
                    raise ValueError(f"No valid injection start for {repo}, duration={duration}")
                for profile in design["condition_profiles"]:
                    for level in profile["magnitude_levels"]:
                        magnitudes = magnitude_mapping(design, level)
                        selected = {metric: magnitudes[metric] for metric in combination}
                        for repetition in range(int(design["repetitions"])):
                            seed = stable_seed(
                                base_seed, repo, affected, duration,
                                profile["name"], level, repetition,
                            )
                            start = valid_starts[seed % len(valid_starts)]
                            weeks = sequence.loc[
                                start:start + duration - 1,
                                "week",
                            ].dt.strftime("%Y-%m-%d").tolist()
                            scenario_number += 1
                            rows.append({
                                "scenario_id": f"S{scenario_number:09d}",
                                "repo_full": repo,
                                "scenario_type": "metric_shift",
                                "affected_metrics": affected,
                                "magnitude_level": level,
                                "magnitude": json.dumps(selected, sort_keys=True),
                                "duration_weeks": duration,
                                "volume_condition": profile["volume_condition"],
                                "missingness_condition": profile["missingness_condition"],
                                "repetition": repetition,
                                "seed": seed,
                                "start_index": start,
                                "end_index": start + duration - 1,
                                "injected_weeks": json.dumps(weeks),
                                "status": "evaluable",
                                "unsupported_reason": "",
                            })

        workflow = design["workflow_composition"]
        scenario_number += 1
        rows.append({
            "scenario_id": f"S{scenario_number:09d}",
            "repo_full": repo,
            "scenario_type": "workflow_composition",
            "affected_metrics": "workflow_composition",
            "magnitude_level": "not_applicable",
            "magnitude": "{}",
            "duration_weeks": 0,
            "volume_condition": "observed",
            "missingness_condition": "none",
            "repetition": 0,
            "seed": stable_seed(base_seed, repo, "workflow_composition"),
            "start_index": None,
            "end_index": None,
            "injected_weeks": "[]",
            "status": workflow["status"],
            "unsupported_reason": workflow["reason"].strip(),
        })

    return pd.DataFrame(rows)


def _subsample_count(successes: int, total: int, sample: int, rng: np.random.Generator) -> int:
    if total <= 0 or sample <= 0:
        return 0
    successes = min(max(int(successes), 0), int(total))
    return int(rng.hypergeometric(successes, total - successes, sample))


def apply_scenario(
    reference: pd.DataFrame,
    scenario: pd.Series,
    stage_cfg: dict,
) -> tuple[pd.DataFrame, pd.Series, list[dict], dict]:
    result = reference.copy(deep=True).reset_index(drop=True)
    truth = pd.Series(False, index=result.index, dtype=bool)
    diagnostics = {
        "probability_preclip_min": None,
        "probability_preclip_max": None,
        "probability_postclip_min": None,
        "probability_postclip_max": None,
        "clipping_count": 0,
        "missing_cells": 0,
    }
    if scenario["scenario_type"] == "no_injection":
        return result, truth, [], diagnostics
    if scenario["status"] != "evaluable":
        return result, truth, [], diagnostics

    start = int(scenario["start_index"])
    end = int(scenario["end_index"])
    indexes = list(range(start, end + 1))
    truth.loc[indexes] = True
    affected = str(scenario["affected_metrics"]).split("+")
    magnitudes = json.loads(scenario["magnitude"])
    rng = np.random.default_rng(int(scenario["seed"]))
    before = result.loc[indexes].copy(deep=True)

    if scenario["volume_condition"] == "low_volume":
        settings = stage_cfg["scenario_design"]["low_volume"]
        fraction = float(settings["sampling_fraction"])
        minimum = int(settings["minimum_denominator"])
        for index in indexes:
            outcome = int(result.at[index, "outcome_n"])
            logical = int(result.at[index, "logical_run_n"])
            sampled_outcome = min(outcome, max(minimum, int(round(outcome * fraction))))
            sampled_logical = min(logical, max(minimum, int(round(logical * fraction))))
            sampled_latency = _subsample_count(
                int(result.at[index, "latency_n"]),
                outcome,
                sampled_outcome,
                rng,
            )
            result.at[index, "failure_count"] = _subsample_count(
                int(result.at[index, "failure_count"]), outcome,
                sampled_outcome, rng,
            )
            result.at[index, "rerun_count"] = _subsample_count(
                int(result.at[index, "rerun_count"]), logical,
                sampled_logical, rng,
            )
            result.at[index, "outcome_n"] = sampled_outcome
            result.at[index, "latency_n"] = sampled_latency
            result.at[index, "logical_run_n"] = sampled_logical
            result.at[index, "failure_rate"] = (
                result.at[index, "failure_count"] / sampled_outcome
                if sampled_outcome else np.nan
            )
            result.at[index, "rerun_rate"] = (
                result.at[index, "rerun_count"] / sampled_logical
                if sampled_logical else np.nan
            )
            result.at[index, "attempts_total"] = max(
                sampled_logical,
                min(
                    int(result.at[index, "attempts_total"]),
                    int(round(result.at[index, "attempts_total"] * fraction)),
                ),
            )

    preclip_values: list[float] = []
    postclip_values: list[float] = []
    probability_values: dict[tuple[int, str], tuple[float, float]] = {}
    probability_clipping_by_index = {index: 0 for index in indexes}
    injection_base = result.loc[indexes].copy(deep=True)
    if "latency_log" in affected:
        result.loc[indexes, "latency_log"] = (
            pd.to_numeric(result.loc[indexes, "latency_log"])
            + np.log1p(float(magnitudes["latency_log"]))
        )

    for metric, count_column, denominator_column in (
        ("failure_rate", "failure_count", "outcome_n"),
        ("rerun_rate", "rerun_count", "logical_run_n"),
    ):
        if metric not in affected:
            continue
        shift = float(magnitudes[metric])
        for index in indexes:
            current = float(result.at[index, metric])
            preclip = current + shift
            target = float(np.clip(preclip, 0, 1))
            denominator = int(result.at[index, denominator_column])
            count = int(rng.binomial(denominator, target)) if denominator else 0
            realized = count / denominator if denominator else np.nan
            result.at[index, count_column] = count
            result.at[index, metric] = realized
            preclip_values.append(preclip)
            postclip_values.append(target)
            probability_values[(index, metric)] = (preclip, target)
            clipped = int(preclip != target)
            probability_clipping_by_index[index] += clipped
            diagnostics["clipping_count"] += clipped

    if scenario["missingness_condition"] == "controlled":
        settings = stage_cfg["scenario_design"]["controlled_missingness"]
        candidates = [(index, metric) for index in indexes for metric in affected]
        count = max(
            int(settings["minimum_cells"]),
            int(np.ceil(len(candidates) * float(settings["fraction_of_affected_cells"]))),
        )
        chosen = rng.choice(len(candidates), size=min(count, len(candidates)), replace=False)
        for position in np.atleast_1d(chosen):
            index, metric = candidates[int(position)]
            result.at[index, metric] = np.nan
        diagnostics["missing_cells"] = int(len(np.atleast_1d(chosen)))

    if preclip_values:
        diagnostics.update({
            "probability_preclip_min": min(preclip_values),
            "probability_preclip_max": max(preclip_values),
            "probability_postclip_min": min(postclip_values),
            "probability_postclip_max": max(postclip_values),
        })

    week_rows: list[dict] = []
    for index in indexes:
        missing = [metric for metric in affected if pd.isna(result.at[index, metric])]
        failure_values = probability_values.get((index, "failure_rate"))
        rerun_values = probability_values.get((index, "rerun_rate"))
        week_rows.append({
            "scenario_id": scenario["scenario_id"],
            "repo_full": scenario["repo_full"],
            "week": result.at[index, "week"].strftime("%Y-%m-%d"),
            "truth": True,
            "affected_metrics": scenario["affected_metrics"],
            "volume_condition": scenario["volume_condition"],
            "missingness_condition": scenario["missingness_condition"],
            "attempts_total_before": before.at[index, "attempts_total"],
            "attempts_total_after": result.at[index, "attempts_total"],
            "outcome_n_before": before.at[index, "outcome_n"],
            "outcome_n_after": result.at[index, "outcome_n"],
            "latency_n_before": before.at[index, "latency_n"],
            "latency_n_after": result.at[index, "latency_n"],
            "failure_count_before": before.at[index, "failure_count"],
            "failure_count_after": result.at[index, "failure_count"],
            "logical_run_n_before": before.at[index, "logical_run_n"],
            "logical_run_n_after": result.at[index, "logical_run_n"],
            "rerun_count_before": before.at[index, "rerun_count"],
            "rerun_count_after": result.at[index, "rerun_count"],
            "latency_log_before": before.at[index, "latency_log"],
            "latency_log_after": result.at[index, "latency_log"],
            "failure_rate_before": before.at[index, "failure_rate"],
            "failure_rate_injection_base": (
                injection_base.at[index, "failure_rate"]
            ),
            "failure_rate_preclip": (
                failure_values[0] if failure_values else None
            ),
            "failure_rate_postclip": (
                failure_values[1] if failure_values else None
            ),
            "failure_rate_after": result.at[index, "failure_rate"],
            "rerun_rate_before": before.at[index, "rerun_rate"],
            "rerun_rate_injection_base": (
                injection_base.at[index, "rerun_rate"]
            ),
            "rerun_rate_preclip": (
                rerun_values[0] if rerun_values else None
            ),
            "rerun_rate_postclip": (
                rerun_values[1] if rerun_values else None
            ),
            "rerun_rate_after": result.at[index, "rerun_rate"],
            "probability_clipping_count": (
                probability_clipping_by_index[index]
            ),
            "missing_metrics": "+".join(missing),
        })
    return result, truth, week_rows, diagnostics


def apply_frozen_mewma(
    frame: pd.DataFrame,
    repo: str,
    baseline_metadata: dict,
) -> tuple[pd.Series, pd.Series]:
    mewma = baseline_metadata["mewma"]
    parameters = mewma["standardization"][repo]["metrics"]
    z = np.column_stack([
        (
            pd.to_numeric(frame[metric], errors="coerce").to_numpy(float)
            - float(parameters[metric]["median"])
        ) / float(parameters[metric]["scale"])
        for metric in CORE_METRICS
    ])
    z = np.clip(z, -8, 8)
    previous = np.zeros(len(CORE_METRICS), dtype=float)
    statistic = np.full(len(frame), np.nan)
    lam = float(mewma["lambda"])
    for index, row in enumerate(z):
        observed = np.isfinite(row)
        if not observed.any():
            continue
        previous[observed] = lam * row[observed] + (1 - lam) * previous[observed]
        statistic[index] = float(np.sum(np.square(np.maximum(previous, 0))))
    stat = pd.Series(statistic, index=frame.index)
    alarm = stat.gt(float(mewma["empirical_limit"])).where(stat.notna()).astype("boolean")
    return stat, alarm


def detector_series(frame: pd.DataFrame, analysis_cfg: dict, baseline_module) -> Iterable[dict]:
    for window in [
        int(analysis_cfg["mad"]["primary_window_weeks"]),
        *map(int, analysis_cfg["mad"]["sensitivity_windows_weeks"]),
    ]:
        for threshold_value in analysis_cfg["mad"]["thresholds"]:
            threshold = float(threshold_value)
            components: dict[str, pd.Series] = {}
            for metric in CORE_METRICS:
                _, alarm = baseline_module.causal_mad(frame[metric], window, threshold)
                components[metric] = alarm
            component_frame = pd.DataFrame(components)
            complete = component_frame.notna().all(axis=1)
            hits = component_frame.fillna(False).astype(int).sum(axis=1)
            raw_variants = {
                **components,
                "union": hits.ge(1).where(complete).astype("boolean"),
                "two_of_three": hits.ge(2).where(complete).astype("boolean"),
            }
            for variant, raw in raw_variants.items():
                for persistence_value in analysis_cfg["mad"]["persistence_weeks"]:
                    persistence = int(persistence_value)
                    yield {
                        "detector_family": "causal_rolling_mad",
                        "detector_variant": variant,
                        "metric": variant if variant in CORE_METRICS else "composite",
                        "window_weeks": window,
                        "window_role": "primary" if window == 13 else "sensitivity",
                        "threshold": threshold,
                        "threshold_role": "primary" if threshold == 3.0 else "sensitivity",
                        "persistence_weeks": persistence,
                        "persistence_role": "primary" if persistence == 2 else "sensitivity",
                        "alarm": baseline_module.persistence(raw, persistence),
                    }


def evaluate_predictions(truth: pd.Series, predicted: pd.Series) -> dict[str, Any]:
    truth_values = truth.astype(bool).to_numpy()
    prediction = predicted.astype("boolean")
    evaluable = prediction.notna().to_numpy()
    predicted_values = prediction.fillna(False).to_numpy(bool)
    tp = int(np.sum(evaluable & truth_values & predicted_values))
    fp = int(np.sum(evaluable & ~truth_values & predicted_values))
    tn = int(np.sum(evaluable & ~truth_values & ~predicted_values))
    fn = int(np.sum(evaluable & truth_values & ~predicted_values))
    precision = tp / (tp + fp) if tp + fp else None
    recall = tp / (tp + fn) if tp + fn else None
    false_alarm = fp / (fp + tn) if fp + tn else None
    truth_indexes = np.flatnonzero(truth_values)
    hit_indexes = np.flatnonzero(evaluable & truth_values & predicted_values)
    predicted_indexes = np.flatnonzero(evaluable & predicted_values)
    episode_detected = bool(len(hit_indexes)) if len(truth_indexes) else None
    delay = int(hit_indexes[0] - truth_indexes[0]) if len(hit_indexes) else None
    union = set(truth_indexes[evaluable[truth_indexes]]) | set(predicted_indexes)
    overlap = len(set(hit_indexes)) / len(union) if truth_indexes.size and union else None
    return {
        "tp": tp,
        "fp": fp,
        "tn": tn,
        "fn": fn,
        "precision": precision,
        "recall": recall,
        "false_alarm_rate": false_alarm,
        "episode_detected": episode_detected,
        "episode_detection_rate": int(episode_detected) if episode_detected is not None else None,
        "detection_delay_weeks": delay,
        "boundary_overlap": overlap,
        "alarm_duration_weeks": int(len(predicted_indexes)),
        "evaluable_weeks": int(evaluable.sum()),
        "unevaluable_weeks": int((~evaluable).sum()),
        "unevaluable_fraction": float((~evaluable).mean()),
        "truth_evaluable_weeks": int(np.sum(evaluable & truth_values)),
        "truth_unevaluable_weeks": int(np.sum(~evaluable & truth_values)),
        "predicted_alarm_weeks": json.dumps(predicted_indexes.tolist()),
    }


class GzipCsvWriter:
    def __init__(self, path: Path, fields: tuple[str, ...]):
        self.path = path
        self.fields = fields
        self.temporary: Path | None = None
        self.raw = None
        self.gzip_file = None
        self.text = None
        self.writer = None

    def __enter__(self):
        handle = tempfile.NamedTemporaryFile(dir=self.path.parent, delete=False)
        self.temporary = Path(handle.name)
        handle.close()
        self.raw = self.temporary.open("wb")
        self.gzip_file = gzip.GzipFile(filename="", mode="wb", fileobj=self.raw, mtime=0)
        self.text = io.TextIOWrapper(self.gzip_file, encoding="utf-8", newline="")
        self.writer = csv.DictWriter(self.text, fieldnames=self.fields, extrasaction="ignore", lineterminator="\n")
        self.writer.writeheader()
        return self

    def writerow(self, row: dict) -> None:
        assert self.writer is not None
        self.writer.writerow(row)

    def __exit__(self, exc_type, exc, traceback):
        assert (
            self.text is not None
            and self.raw is not None
            and self.temporary is not None
        )
        self.text.flush()
        self.text.close()
        self.raw.close()
        if exc_type is None:
            os.replace(self.temporary, self.path)
        else:
            self.temporary.unlink(missing_ok=True)


def atomic_csv(frame: pd.DataFrame, path: Path) -> None:
    handle = tempfile.NamedTemporaryFile(mode="w", dir=path.parent, delete=False, encoding="utf-8", newline="")
    temporary = Path(handle.name)
    handle.close()
    try:
        frame.to_csv(temporary, index=False, lineterminator="\n")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def atomic_json(value: dict, path: Path) -> None:
    handle = tempfile.NamedTemporaryFile(mode="w", dir=path.parent, delete=False, encoding="utf-8")
    temporary = Path(handle.name)
    try:
        json.dump(value, handle, indent=2, sort_keys=True, allow_nan=False)
        handle.write("\n")
        handle.close()
        os.replace(temporary, path)
    finally:
        if not handle.closed:
            handle.close()
        temporary.unlink(missing_ok=True)


def update_summary(summary: dict, row: dict) -> None:
    key = tuple(row[field] for field in SUMMARY_KEYS)
    item = summary.setdefault(key, {
        "scenarios": 0,
        **{f"{metric}_sum": 0.0 for metric in SUMMARY_METRICS},
        **{f"{metric}_n": 0 for metric in SUMMARY_METRICS},
    })
    item["scenarios"] += 1
    for metric in SUMMARY_METRICS:
        value = row[metric]
        if value is not None and not pd.isna(value):
            item[f"{metric}_sum"] += float(value)
            item[f"{metric}_n"] += 1


def summary_frame(summary: dict) -> pd.DataFrame:
    rows = []
    for key, values in summary.items():
        row = dict(zip(SUMMARY_KEYS, key))
        row["scenarios"] = values["scenarios"]
        for metric in SUMMARY_METRICS:
            count = values[f"{metric}_n"]
            row[f"mean_{metric}"] = values[f"{metric}_sum"] / count if count else None
            row[f"{metric}_observations"] = count
        rows.append(row)
    return pd.DataFrame(rows)


def run(stage_config_path: str | Path) -> dict:
    stage_path = Path(stage_config_path).resolve()
    root = stage_path.parent
    stage_cfg = load_yaml(stage_path)
    verification = verify_inputs(root, stage_cfg)
    approved = stage_cfg["approved_inputs"]
    analysis_cfg = load_yaml(root / approved["analysis_config"]["path"])
    metadata = json.loads((root / approved["baseline_metadata"]["path"]).read_text(encoding="utf-8"))
    validate_frozen_detector(analysis_cfg, stage_cfg, metadata)
    baseline_module = import_baseline_module(root / approved["baseline_source"]["path"])
    panel = pd.read_csv(root / approved["baseline_results"]["path"], parse_dates=["week"])
    development = set(analysis_cfg["study"]["development_repositories"])
    training = panel[panel["repo_full"].isin(development) & panel["split"].eq("train")].copy()
    if set(training["repo_full"]) != development:
        raise RuntimeError("Development-training sequences are incomplete")

    scenario_grid = build_scenario_grid(training, stage_cfg)
    grid_bytes = scenario_grid.to_csv(index=False, lineterminator="\n").encode("utf-8")
    grid_sha256 = hashlib.sha256(grid_bytes).hexdigest()
    output = root / stage_cfg["outputs"]["directory"]
    output.mkdir(parents=True, exist_ok=True)
    intended = tuple(stage_cfg["outputs"]["files"])
    if intended != OUTPUT_NAMES:
        raise RuntimeError("Stage-A output names differ from the implementation")
    existing = [name for name in intended if (output / name).exists()]
    if existing:
        raise RuntimeError("Refusing to overwrite Stage-A outputs: " + ", ".join(existing))

    runtime_diagnostics: dict[str, dict] = {}
    summary: dict = {}
    sequence_map = {
        repo: group.sort_values("week").reset_index(drop=True)
        for repo, group in training.groupby("repo_full", sort=True)
    }

    atomic_csv(scenario_grid, output / OUTPUT_NAMES[0])
    try:
        with GzipCsvWriter(output / OUTPUT_NAMES[1], WEEK_RESULT_FIELDS) as week_writer, GzipCsvWriter(output / OUTPUT_NAMES[2], DETECTOR_RESULT_FIELDS) as detector_writer:
            for _, scenario in scenario_grid.iterrows():
                if scenario["status"] != "evaluable":
                    continue
                reference = sequence_map[str(scenario["repo_full"])]
                altered, truth, week_rows, diagnostics = apply_scenario(reference, scenario, stage_cfg)
                runtime_diagnostics[str(scenario["scenario_id"])] = diagnostics
                for week_row in week_rows:
                    week_writer.writerow(week_row)

                detectors = list(detector_series(altered, analysis_cfg, baseline_module))
                _, mewma_alarm = apply_frozen_mewma(
                    altered,
                    str(scenario["repo_full"]),
                    metadata,
                )
                detectors.append({
                    "detector_family": "mewma",
                    "detector_variant": "mewma",
                    "metric": "composite",
                    "window_weeks": None,
                    "window_role": "not_applicable",
                    "threshold": float(metadata["mewma"]["empirical_limit"]),
                    "threshold_role": "frozen_control_limit",
                    "persistence_weeks": None,
                    "persistence_role": "not_applicable",
                    "alarm": mewma_alarm,
                })
                for detector in detectors:
                    result = {
                        **{field: scenario[field] for field in (
                            "scenario_id", "repo_full", "scenario_type",
                            "affected_metrics", "magnitude_level", "magnitude",
                            "duration_weeks", "volume_condition",
                            "missingness_condition", "repetition",
                        )},
                        **{field: detector[field] for field in (
                            "detector_family", "detector_variant", "metric",
                            "window_weeks", "window_role", "threshold",
                            "threshold_role", "persistence_weeks",
                            "persistence_role",
                        )},
                        **evaluate_predictions(truth, detector["alarm"]),
                    }
                    detector_writer.writerow(result)
                    update_summary(summary, result)
    except Exception:
        for name in OUTPUT_NAMES[1:]:
            (output / name).unlink(missing_ok=True)
        (output / OUTPUT_NAMES[0]).unlink(missing_ok=True)
        raise

    summary = summary_frame(summary)
    atomic_csv(summary, output / OUTPUT_NAMES[3])
    clipping_total = sum(item["clipping_count"] for item in runtime_diagnostics.values())
    missing_total = sum(item["missing_cells"] for item in runtime_diagnostics.values())
    metadata_output = {
        "status": "PASS",
        "input_verification": verification,
        "stage_config_sha256": sha256(stage_path),
        "scenario_grid_sha256": grid_sha256,
        "scenario_count": int(len(scenario_grid)),
        "evaluable_scenario_count": int(scenario_grid["status"].eq("evaluable").sum()),
        "unsupported_scenario_count": int(scenario_grid["status"].eq("unsupported").sum()),
        "development_training_only": True,
        "development_repositories": sorted(development),
        "frozen_detectors": {
            "mad": metadata["mad"],
            "mewma_lambda": metadata["mewma"]["lambda"],
            "mewma_empirical_limit": metadata["mewma"]["empirical_limit"],
            "mewma_standardization": metadata["mewma"]["standardization"],
            "mewma_refitted": False,
        },
        "injection_semantics": {
            "latency": "latency_log + log1p(relative_shift)",
            "failure_and_rerun": "absolute probability shift, clipped to [0,1], then deterministic seeded binomial realization",
            "low_volume": "deterministic seeded hypergeometric count subsampling",
            "missingness": "affected cells remain missing and reduce detector evaluability",
            "workflow_composition": stage_cfg["scenario_design"]["workflow_composition"],
        },
        "probability_clipping_count": int(clipping_total),
        "injected_missing_cells": int(missing_total),
        "random_seed": int(stage_cfg["scenario_design"]["random_seed"]),
        "outputs": list(OUTPUT_NAMES),
    }
    atomic_json(metadata_output, output / OUTPUT_NAMES[4])
    return metadata_output


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="stage_a_synthetic_config.yaml")
    args = parser.parse_args()
    result = run(args.config)
    print(json.dumps({
        "status": result["status"],
        "scenario_count": result["scenario_count"],
        "outputs": result["outputs"],
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
