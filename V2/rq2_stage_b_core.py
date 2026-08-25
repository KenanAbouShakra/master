from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Iterable

import numpy as np
import pandas as pd


CORE_METRICS = ("latency_log", "failure_rate", "rerun_rate")
PRIMARY_MAD_VARIANTS = (
    "latency_log", "failure_rate", "rerun_rate", "union", "two_of_three",
)
RESPONSE_HORIZONS = (0, 1, 2, 4)


def stable_seed(base_seed: int, *parts: object) -> int:
    text = "|".join([str(base_seed), *(str(part) for part in parts)])
    return int.from_bytes(hashlib.sha256(text.encode("utf-8")).digest()[:8], "big")


def valid_injection_starts(
    sequence: pd.DataFrame,
    duration_weeks: int,
    minimum_prior_observed_weeks: int,
) -> list[int]:
    if duration_weeks <= 0:
        raise ValueError("duration_weeks must be positive")
    eligible = sequence["external_evaluation_eligible"].fillna(False).astype(bool).to_numpy()
    observed = (
        pd.to_numeric(sequence["attempts_total"], errors="coerce")
        .fillna(0).gt(0).to_numpy()
    )
    starts: list[int] = []
    for start in range(len(sequence) - duration_weeks + 1):
        if int(observed[:start].sum()) < minimum_prior_observed_weeks:
            continue
        if bool(eligible[start:start + duration_weeks].all()):
            starts.append(start)
    return starts


def magnitude_mapping(design: dict, level: str) -> dict[str, float]:
    index = {"low": 0, "medium": 1, "high": 2}[level]
    return {
        "latency_log": float(design["latency_relative_shifts"][index]),
        "failure_rate": float(design["failure_probability_shifts"][index]),
        "rerun_rate": float(design["rerun_probability_shifts"][index]),
    }


def build_stage_b_scenario_grid(panel: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    evidence = cfg["evidence_domain"]
    design = cfg["scenario_design"]
    base_seed = int(design["base_seed"])
    repetitions = int(design["repetitions_per_injected_cell"])
    minimum_prior = int(evidence["calibration_weeks"])
    rows: list[dict] = []
    scenario_number = 0

    for repo in evidence["repositories"]:
        sequence = (
            panel.loc[panel["repo_full"].eq(repo)]
            .sort_values("week").reset_index(drop=True)
        )
        if sequence.empty:
            raise RuntimeError(f"External repository absent: {repo}")

        for repetition in range(int(design["no_injection_repetitions_per_repository"])):
            scenario_number += 1
            rows.append({
                "scenario_id": f"B{scenario_number:09d}",
                "repo_full": repo,
                "scenario_type": "no_injection",
                "affected_metrics": "none",
                "dimensionality": 0,
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
            })

        for combination in design["signal_combinations"]:
            affected = "+".join(combination)
            dimensionality = len(combination)
            for duration_value in design["durations_weeks"]:
                duration = int(duration_value)
                starts = valid_injection_starts(sequence, duration, minimum_prior)
                if not starts:
                    raise RuntimeError(f"No valid Stage-B start: {repo}, duration={duration}")
                for profile in design["condition_profiles"]:
                    for level in profile["magnitude_levels"]:
                        all_magnitudes = magnitude_mapping(design, level)
                        selected = {metric: all_magnitudes[metric] for metric in combination}
                        for repetition in range(repetitions):
                            seed = stable_seed(
                                base_seed, repo, affected, duration,
                                profile["name"], level, repetition,
                            )
                            start = starts[seed % len(starts)]
                            weeks = sequence.loc[
                                start:start + duration - 1, "week"
                            ].dt.strftime("%Y-%m-%d").tolist()
                            scenario_number += 1
                            rows.append({
                                "scenario_id": f"B{scenario_number:09d}",
                                "repo_full": repo,
                                "scenario_type": "metric_shift",
                                "affected_metrics": affected,
                                "dimensionality": dimensionality,
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
                            })
    grid = pd.DataFrame(rows)
    if grid["scenario_id"].duplicated().any():
        raise AssertionError("Duplicate Stage-B scenario ID")
    if grid["seed"].duplicated().any():
        raise AssertionError("Duplicate Stage-B seed")
    return grid


def _as_boolean(values: pd.Series) -> pd.Series:
    return values.astype("boolean")


def primary_detector_alarms(
    frame: pd.DataFrame,
    baseline_module,
    stage_a_module,
    baseline_metadata: dict,
    *,
    repo: str,
    window_weeks: int = 13,
    threshold: float = 3.0,
    persistence_weeks: int = 2,
) -> dict[str, pd.Series]:
    components: dict[str, pd.Series] = {}
    for metric in CORE_METRICS:
        _, alarm = baseline_module.causal_mad(frame[metric], window_weeks, threshold)
        components[metric] = _as_boolean(alarm)
    component_frame = pd.DataFrame(components)
    complete = component_frame.notna().all(axis=1)
    hits = component_frame.fillna(False).astype(int).sum(axis=1)
    raw = {
        **components,
        "union": hits.ge(1).where(complete).astype("boolean"),
        "two_of_three": hits.ge(2).where(complete).astype("boolean"),
    }
    alarms = {
        f"mad:{variant}": baseline_module.persistence(raw[variant], persistence_weeks)
        for variant in PRIMARY_MAD_VARIANTS
    }
    _, mewma_alarm = stage_a_module.apply_frozen_mewma(frame, repo, baseline_metadata)
    alarms["mewma:mewma"] = _as_boolean(mewma_alarm)
    return alarms


@dataclass(frozen=True)
class PairedEvaluation:
    result: dict
    week_rows: list[dict]


def evaluate_paired_alarms(
    *,
    scenario_id: str,
    repo_full: str,
    detector_id: str,
    weeks: pd.Series,
    truth: pd.Series,
    eligible: pd.Series,
    reference_alarm: pd.Series,
    injected_alarm: pd.Series,
    response_horizons: Iterable[int] = RESPONSE_HORIZONS,
) -> PairedEvaluation:
    truth_array = truth.astype(bool).to_numpy()
    eligible_array = eligible.fillna(False).astype(bool).to_numpy()
    reference = _as_boolean(reference_alarm)
    injected = _as_boolean(injected_alarm)
    pair_evaluable = reference.notna().to_numpy() & injected.notna().to_numpy() & eligible_array
    reference_values = reference.fillna(False).to_numpy(bool)
    injected_values = injected.fillna(False).to_numpy(bool)
    incremental = pair_evaluable & injected_values & ~reference_values
    strict_hits = np.flatnonzero(incremental & truth_array)
    truth_indexes = np.flatnonzero(truth_array & eligible_array)
    reference_truth = pair_evaluable & truth_array & reference_values
    injected_truth = pair_evaluable & truth_array & injected_values
    incremental_indexes = np.flatnonzero(incremental)

    if truth_indexes.size:
        start, end = int(truth_indexes[0]), int(truth_indexes[-1])
        strict_detected = bool(strict_hits.size)
        strict_delay = int(strict_hits[0] - start) if strict_hits.size else None
        spillover = incremental & (np.arange(len(truth_array)) > end)
        reference_overlap = float(reference_truth.sum() / max(pair_evaluable[truth_array].sum(), 1))
        operational_detected = bool(injected_truth.any())
        union = set(truth_indexes[pair_evaluable[truth_indexes]]) | set(incremental_indexes)
        boundary_overlap = len(set(strict_hits)) / len(union) if union else None
        horizon_values = {}
        for horizon in response_horizons:
            horizon = int(horizon)
            upper = min(end + horizon, len(truth_array) - 1)
            horizon_values[f"incremental_detected_within_{horizon}w_post"] = bool(
                incremental[start:upper + 1].any()
            )
    else:
        start = end = None
        strict_detected = None
        strict_delay = None
        spillover = np.zeros(len(truth_array), dtype=bool)
        reference_overlap = None
        operational_detected = None
        boundary_overlap = None
        horizon_values = {
            f"incremental_detected_within_{int(horizon)}w_post": None
            for horizon in response_horizons
        }

    eligible_evaluable_count = int(pair_evaluable.sum())
    reference_alarm_count = int((pair_evaluable & reference_values).sum())
    injected_alarm_count = int((pair_evaluable & injected_values).sum())
    incremental_alarm_count = int(incremental.sum())
    off_truth_injected = pair_evaluable & ~truth_array
    injection_tp = int((pair_evaluable & truth_array & injected_values).sum())
    injection_fp = int((off_truth_injected & injected_values).sum())
    injection_tn = int((off_truth_injected & ~injected_values).sum())
    injection_fn = int((pair_evaluable & truth_array & ~injected_values).sum())

    result = {
        "scenario_id": scenario_id,
        "repo_full": repo_full,
        "detector_id": detector_id,
        "strict_incremental_episode_detected": strict_detected,
        "strict_incremental_detection_delay_weeks": strict_delay,
        "reference_alarm_weeks": reference_alarm_count,
        "reference_alarm_burden": (
            reference_alarm_count / eligible_evaluable_count
            if eligible_evaluable_count else None
        ),
        "injected_alarm_weeks": injected_alarm_count,
        "incremental_alarm_weeks": incremental_alarm_count,
        "incremental_alarm_duration_weeks": incremental_alarm_count,
        "incremental_spillover_weeks": int(spillover.sum()),
        "total_operational_episode_detected": operational_detected,
        "reference_alarm_overlap_fraction": reference_overlap,
        "incremental_boundary_overlap": boundary_overlap,
        "pair_evaluable_weeks": eligible_evaluable_count,
        "pair_unevaluable_weeks": int(eligible_array.sum() - pair_evaluable.sum()),
        "pair_unevaluable_fraction": (
            float((eligible_array.sum() - pair_evaluable.sum()) / eligible_array.sum())
            if eligible_array.sum() else None
        ),
        "injection_relative_tp": injection_tp,
        "injection_relative_fp": injection_fp,
        "injection_relative_tn": injection_tn,
        "injection_relative_fn": injection_fn,
        "injection_relative_precision": (
            injection_tp / (injection_tp + injection_fp)
            if injection_tp + injection_fp else None
        ),
        "injection_relative_false_alarm_rate": (
            injection_fp / (injection_fp + injection_tn)
            if injection_fp + injection_tn else None
        ),
        **horizon_values,
    }

    week_rows = []
    for index in range(len(truth_array)):
        if not eligible_array[index]:
            continue
        week_rows.append({
            "scenario_id": scenario_id,
            "repo_full": repo_full,
            "detector_id": detector_id,
            "week": pd.Timestamp(weeks.iloc[index]).strftime("%Y-%m-%d"),
            "truth": bool(truth_array[index]),
            "pair_evaluable": bool(pair_evaluable[index]),
            "reference_alarm": bool(reference_values[index]) if pair_evaluable[index] else None,
            "injected_alarm": bool(injected_values[index]) if pair_evaluable[index] else None,
            "incremental_alarm": bool(incremental[index]) if pair_evaluable[index] else None,
            "spillover_alarm": bool(spillover[index]) if pair_evaluable[index] else None,
        })
    return PairedEvaluation(result=result, week_rows=week_rows)