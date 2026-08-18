from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yaml


OUTPUT_NAMES = (
    "rq1_baseline_week_results.csv",
    "rq1_baseline_summary.csv",
    "rq1_baseline_candidate_episodes.csv",
    "rq1_baseline_detector_agreement.csv",
    "rq1_baseline_support_diagnostics.csv",
    "rq1_baseline_validation_status.json",
)
CORE_METRICS = ("latency_log", "failure_rate", "rerun_rate")
SYNTHETIC_METRICS = (
    "precision",
    "recall",
    "false_alarm_rate",
    "episode_detection_rate",
    "detection_delay_weeks",
    "boundary_overlap",
    "alarm_duration_weeks",
    "unevaluable_fraction",
)
EXPECTED_MAD_VARIANTS = (
    "latency_log", "failure_rate", "rerun_rate", "union", "two_of_three",
)
EXPECTED_MAD_WINDOW = 13
EXPECTED_MAD_THRESHOLD = 3.0
EXPECTED_MAD_PERSISTENCE = 2
EXPECTED_MEWMA_LAMBDA = 0.20
EXPECTED_MEWMA_LIMIT = 7.150034791553729
WEEK_COLUMNS = (
    "evidence_domain", "repo_full", "week", "evaluation_population",
    "source_split", "detector_family", "detector_variant", "metric",
    "specification_role", "window_weeks", "threshold_scaled_mad",
    "persistence_weeks", "evaluable", "alarm", "magnitude",
    "magnitude_definition", "raw_mewma_stat", "frozen_control_limit",
    "attempts_total", "outcome_n", "failure_count", "latency_n",
    "logical_run_n", "rerun_count", "low_outcome_support",
    "low_latency_support", "missing_core_metric_count", "workflow_count",
    "workflow_concentration_hhi", "external_calibration",
    "external_evaluation_eligible", "causal_history_policy",
)
SUMMARY_COLUMNS = (
    "evidence_domain", "repo_full", "evaluation_population",
    "detector_family", "detector_variant", "metric", "specification_role",
    "total_weeks", "evaluable_weeks", "unevaluable_weeks", "alarm_weeks",
    "alarm_prevalence", "episode_count", "complete_episode_count",
    "left_censored_episode_count", "right_censored_episode_count",
    "total_observed_episode_weeks", "median_observed_duration_weeks",
    "maximum_observed_duration_weeks", "median_alarm_magnitude",
    "maximum_alarm_magnitude", "attempts_total_sum", "outcome_n_sum",
    "latency_n_sum", "logical_run_n_sum", "synthetic_scenarios",
    "precision", "precision_valid_observations", "recall",
    "recall_valid_observations", "false_alarm_rate",
    "false_alarm_rate_valid_observations", "episode_detection_rate",
    "episode_detection_rate_valid_observations", "detection_delay_weeks",
    "detection_delay_weeks_valid_observations", "boundary_overlap",
    "boundary_overlap_valid_observations", "alarm_duration_weeks",
    "alarm_duration_weeks_valid_observations", "unevaluable_fraction",
    "unevaluable_fraction_valid_observations",
)
EPISODE_COLUMNS = (
    "repo_full", "evaluation_population", "detector_family",
    "detector_variant", "metric", "start_week", "end_week",
    "left_censored", "right_censored", "observed_duration_weeks",
    "median_magnitude", "maximum_magnitude", "attempts_total_sum",
    "outcome_n_sum", "latency_n_sum", "logical_run_n_sum",
    "minimum_attempts_total", "minimum_outcome_n", "minimum_latency_n",
    "minimum_logical_run_n", "median_workflow_count",
    "maximum_workflow_concentration_hhi", "low_support_week_count",
    "missing_core_metric_week_count", "interpretation",
)
AGREEMENT_COLUMNS = (
    "repo_full", "evaluation_population", "mad_variant", "mewma_variant",
    "both_evaluable_weeks", "both_alarm_weeks", "neither_alarm_weeks",
    "mad_only_alarm_weeks", "mewma_only_alarm_weeks",
    "observed_agreement", "jaccard_alarm_overlap", "interpretation",
)
SUPPORT_COLUMNS = (
    "repo_full", "evaluation_population", "total_weeks",
    "attempts_total_sum", "outcome_n_sum", "latency_n_sum",
    "logical_run_n_sum", "low_outcome_support_weeks",
    "low_latency_support_weeks", "missing_core_metric_weeks",
    "median_workflow_count", "median_workflow_concentration_hhi",
    "maximum_workflow_concentration_hhi", "release_context_available",
)


def sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_json(path: str | Path) -> dict:
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Cannot read JSON {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise RuntimeError(f"JSON root must be an object: {path}")
    return value


def load_yaml(path: str | Path) -> dict:
    value = yaml.safe_load(Path(path).read_text(encoding="utf-8-sig"))
    if not isinstance(value, dict):
        raise RuntimeError(f"YAML root must be a mapping: {path}")
    return value


def validate_manifest(root: Path, relative: str) -> dict:
    path = root / relative
    manifest = load_json(path)
    records = manifest.get("files")
    if not isinstance(records, dict) or not records:
        raise RuntimeError(f"Manifest has no files: {relative}")
    failures = []
    for item, record in records.items():
        candidate = root / item
        if not candidate.is_file():
            failures.append(f"{item}: missing")
        elif candidate.stat().st_size != int(record.get("bytes", -1)):
            failures.append(f"{item}: size")
        elif sha256(candidate) != str(record.get("sha256", "")):
            failures.append(f"{item}: hash")
    if failures:
        raise RuntimeError(f"Manifest validation failed: {relative}: " + "; ".join(failures))
    return {
        "path": relative,
        "sha256": sha256(path),
        "files_verified": len(records),
        "manifest": manifest,
    }


def validate_hmm_rejection(root: Path) -> dict:
    path = root / "analysis_outputs" / "HMM_REJECTION_REPORT.json"
    report = load_json(path)
    provenance = report.get("provenance", {})
    failures = []
    for label, record in provenance.items():
        candidate = root / record["path"]
        if not candidate.is_file() or candidate.stat().st_size != record["bytes"] or sha256(candidate) != record["sha256"]:
            failures.append(label)
    required = (
        report.get("status") == "HMM_REJECTED"
        and report["decision"]["confirmatory_rq1_inclusion"] is False
        and report["model_use"]["model_selected"] is False
        and report["model_use"]["model_used_for_repository_scoring"] is False
        and report["decision"]["supported_primary_detectors"]
        == ["causal rolling MAD", "MEWMA"]
    )
    if failures or not required:
        raise RuntimeError("HMM rejection provenance is invalid")
    return {"path": path.relative_to(root).as_posix(), "sha256": sha256(path)}


def load_detector_contract(stage_manifest: dict, panel_columns: set[str]) -> dict:
    primary = stage_manifest.get("primary_mad_specification")
    mewma = stage_manifest.get("frozen_mewma_specification")
    if not isinstance(primary, dict) or not isinstance(mewma, dict):
        raise RuntimeError("Stage-A manifest lacks detector contracts")
    required = {
        "window_weeks", "threshold_scaled_mad", "persistence_weeks",
        "variants", "current_week_excluded", "interpretation",
    }
    if not required.issubset(primary):
        raise RuntimeError("Primary MAD contract is incomplete")
    window = int(primary["window_weeks"])
    threshold = float(primary["threshold_scaled_mad"])
    persistence = int(primary["persistence_weeks"])
    exact_mad = (
        window == EXPECTED_MAD_WINDOW
        and threshold == EXPECTED_MAD_THRESHOLD
        and persistence == EXPECTED_MAD_PERSISTENCE
        and primary["current_week_excluded"] is True
        and tuple(primary["variants"]) == EXPECTED_MAD_VARIANTS
        and primary["interpretation"] == "detector signal, not ground truth"
    )
    if not exact_mad:
        raise RuntimeError("Frozen primary MAD contract differs from the approved specification")
    exact_mewma = (
        float(mewma.get("lambda", np.nan)) == EXPECTED_MEWMA_LAMBDA
        and float(mewma.get("empirical_control_limit", np.nan))
        == EXPECTED_MEWMA_LIMIT
        and mewma.get("refitted_during_synthetic_evaluation") is False
    )
    if not exact_mewma:
        raise RuntimeError("Frozen MEWMA contract differs from the approved specification")
    prefix = f"mad_w{window}_t{threshold:g}"
    component_scores = {
        metric: f"{prefix}_{metric}_score" for metric in CORE_METRICS
    }
    variants = []
    for variant in primary["variants"]:
        alarm_column = f"{prefix}_{variant}_k{persistence}"
        if alarm_column not in panel_columns:
            raise RuntimeError(f"Frozen primary alarm column is absent: {alarm_column}")
        metric = variant if component_scores.get(variant) in panel_columns else "composite"
        variants.append({
            "variant": variant,
            "metric": metric,
            "alarm_column": alarm_column,
        })
    if not all(column in panel_columns for column in component_scores.values()):
        raise RuntimeError("Frozen MAD component score columns are incomplete")
    if "mewma_stat" not in panel_columns or "mewma_alarm" not in panel_columns:
        raise RuntimeError("Frozen MEWMA outputs are absent")
    return {
        "specification_role": "primary",
        "mad": {
            **primary,
            "window_weeks": window,
            "threshold_scaled_mad": threshold,
            "persistence_weeks": persistence,
            "prefix": prefix,
            "component_scores": component_scores,
            "variants_resolved": variants,
        },
        "mewma": {
            "lambda": float(mewma["lambda"]),
            "frozen_control_limit": float(mewma["empirical_control_limit"]),
            "refitted": bool(mewma["refitted_during_synthetic_evaluation"]),
        },
    }


def validate_frozen_mewma_panel(panel: pd.DataFrame, contract: dict) -> dict:
    required = {"mewma_stat", "mewma_limit", "mewma_alarm"}
    missing = sorted(required - set(panel.columns))
    if missing:
        raise RuntimeError("Frozen MEWMA panel columns are missing: " + ", ".join(missing))
    stat = pd.to_numeric(panel["mewma_stat"], errors="coerce")
    limit = pd.to_numeric(panel["mewma_limit"], errors="coerce")
    alarm = nullable_boolean(panel["mewma_alarm"])
    finite_stat = np.isfinite(stat)
    finite_limit = np.isfinite(limit)
    frozen_limit = float(contract["mewma"]["frozen_control_limit"])
    if not finite_limit.all() or not np.allclose(
        limit.to_numpy(float), frozen_limit, rtol=0, atol=1e-12
    ):
        raise RuntimeError("Panel MEWMA limits differ from the frozen control limit")
    expected_alarm = stat.gt(frozen_limit).where(finite_stat).astype("boolean")
    mismatch = ~(
        (alarm.isna() & expected_alarm.isna())
        | (alarm.notna() & expected_alarm.notna() & alarm.eq(expected_alarm))
    )
    if mismatch.any():
        raise RuntimeError("Panel MEWMA alarms are inconsistent with statistic and limit")
    if alarm.notna().to_numpy().tolist() != finite_stat.to_numpy().tolist():
        raise RuntimeError("Panel MEWMA evaluability is inconsistent with finite statistics")
    return {
        "rows": int(len(panel)),
        "finite_statistics": int(finite_stat.sum()),
        "unevaluable_statistics": int((~finite_stat).sum()),
        "frozen_control_limit": frozen_limit,
        "alarm_consistency": "PASS",
    }


def nullable_boolean(values: pd.Series) -> pd.Series:
    if str(values.dtype) == "boolean":
        return values
    result = pd.Series(pd.NA, index=values.index, dtype="boolean")
    text = values.astype("string").str.lower()
    result.loc[text.eq("true") | text.eq("1")] = True
    result.loc[text.eq("false") | text.eq("0")] = False
    return result


def evaluation_population(panel: pd.DataFrame, analysis_cfg: dict) -> pd.Series:
    development = panel["repo_full"].isin(analysis_cfg["study"]["development_repositories"])
    external = panel["repo_full"].isin(analysis_cfg["study"]["external_repositories"])
    result = pd.Series(pd.NA, index=panel.index, dtype="string")
    result.loc[development & panel["split"].eq("holdout")] = "holdout"
    eligible = nullable_boolean(panel["external_evaluation_eligible"]).fillna(False)
    calibration = nullable_boolean(panel["external_calibration"]).fillna(False)
    result.loc[external & panel["split"].eq("external") & eligible & ~calibration] = "external_evaluation"
    return result


def validate_evaluation_denominators(
    panel: pd.DataFrame,
    populations: pd.Series,
    analysis_cfg: dict,
) -> tuple[dict, dict]:
    totals = populations.value_counts().to_dict()
    if totals != {"external_evaluation": 138, "holdout": 57}:
        raise RuntimeError(f"Unexpected evaluation denominators: {totals}")
    repository_denominators = (
        panel.loc[populations.notna(), ["repo_full"]]
        .assign(evaluation_population=populations.loc[populations.notna()].values)
        .groupby(["repo_full", "evaluation_population"])
        .size()
        .to_dict()
    )
    development = set(analysis_cfg["study"]["development_repositories"])
    external = set(analysis_cfg["study"]["external_repositories"])
    for (repository, population), weeks in repository_denominators.items():
        required_weeks = 19 if repository in development else 46
        required_population = (
            "holdout" if repository in development else "external_evaluation"
        )
        if population != required_population or int(weeks) != required_weeks:
            raise RuntimeError(
                f"Unexpected repository denominator: {repository}, "
                f"{population}, weeks={weeks}"
            )
    if set(repository for repository, _ in repository_denominators) != development | external:
        raise RuntimeError("Evaluation denominator omits a frozen repository")
    return totals, repository_denominators


def mad_magnitude(row: pd.Series, variant: str, contract: dict) -> float:
    scores = [
        float(row[column])
        for column in contract["component_scores"].values()
        if pd.notna(row[column])
    ]
    if variant in contract["component_scores"]:
        value = row[contract["component_scores"][variant]]
        return float(value) if pd.notna(value) else np.nan
    if len(scores) != len(CORE_METRICS):
        return np.nan
    ordered = sorted(scores, reverse=True)
    if variant == "union":
        return float(ordered[0])
    if variant == "two_of_three":
        return float(ordered[1])
    raise ValueError(f"Unknown MAD variant: {variant}")


def magnitude_definition(variant: str) -> str:
    if variant in CORE_METRICS:
        return "frozen component scaled-MAD score"
    if variant == "union":
        return "maximum of three frozen component scaled-MAD scores"
    if variant == "two_of_three":
        return "second-highest of three frozen component scaled-MAD scores"
    if variant == "mewma":
        return "raw mewma_stat divided by frozen control limit"
    raise ValueError(variant)


def build_week_results(panel: pd.DataFrame, populations: pd.Series, contract: dict) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    scored = panel.loc[populations.notna()].copy()
    scored["evaluation_population"] = populations.loc[scored.index]
    common = (
        "attempts_total", "outcome_n", "failure_count", "latency_n",
        "logical_run_n", "rerun_count", "low_outcome_support",
        "low_latency_support", "missing_core_metric_count", "workflow_count",
        "workflow_concentration_hhi", "external_calibration",
        "external_evaluation_eligible",
    )
    for index, row in scored.iterrows():
        population = str(row["evaluation_population"])
        history = (
            "frozen full development sequence; preceding purge observations retained"
            if population == "holdout"
            else "frozen external sequence; 13 calibration weeks retained for causal initialization only"
        )
        base = {
            "evidence_domain": "observed_research",
            "repo_full": row["repo_full"],
            "week": row["week"],
            "evaluation_population": population,
            "source_split": row["split"],
            "specification_role": "primary",
            "attempts_total": row["attempts_total"],
            "outcome_n": row["outcome_n"],
            "failure_count": row["failure_count"],
            "latency_n": row["latency_n"],
            "logical_run_n": row["logical_run_n"],
            "rerun_count": row["rerun_count"],
            "low_outcome_support": row["low_outcome_support"],
            "low_latency_support": row["low_latency_support"],
            "missing_core_metric_count": row["missing_core_metric_count"],
            "workflow_count": row["workflow_count"],
            "workflow_concentration_hhi": row["workflow_concentration_hhi"],
            "external_calibration": row["external_calibration"],
            "external_evaluation_eligible": row["external_evaluation_eligible"],
            "causal_history_policy": history,
        }
        for resolved in contract["mad"]["variants_resolved"]:
            alarm = nullable_boolean(pd.Series([row[resolved["alarm_column"]]])).iloc[0]
            rows.append({
                **base,
                "detector_family": "causal_rolling_mad",
                "detector_variant": resolved["variant"],
                "metric": resolved["metric"],
                "window_weeks": contract["mad"]["window_weeks"],
                "threshold_scaled_mad": contract["mad"]["threshold_scaled_mad"],
                "persistence_weeks": contract["mad"]["persistence_weeks"],
                "evaluable": bool(pd.notna(alarm)),
                "alarm": alarm,
                "magnitude": mad_magnitude(row, resolved["variant"], contract["mad"]),
                "magnitude_definition": magnitude_definition(resolved["variant"]),
                "raw_mewma_stat": np.nan,
                "frozen_control_limit": np.nan,
            })
        mewma_alarm = nullable_boolean(pd.Series([row["mewma_alarm"]])).iloc[0]
        limit = contract["mewma"]["frozen_control_limit"]
        stat = float(row["mewma_stat"]) if pd.notna(row["mewma_stat"]) else np.nan
        rows.append({
            **base,
            "detector_family": "mewma",
            "detector_variant": "mewma",
            "metric": "composite",
            "window_weeks": np.nan,
            "threshold_scaled_mad": np.nan,
            "persistence_weeks": np.nan,
            "evaluable": bool(pd.notna(mewma_alarm) and np.isfinite(stat)),
            "alarm": mewma_alarm,
            "magnitude": stat / limit if np.isfinite(stat) else np.nan,
            "magnitude_definition": magnitude_definition("mewma"),
            "raw_mewma_stat": stat,
            "frozen_control_limit": limit,
        })
    return pd.DataFrame(rows, columns=WEEK_COLUMNS)


def validate_week_detector_rows(week_results: pd.DataFrame) -> dict:
    if len(week_results) != 1170:
        raise RuntimeError(f"Expected 1170 week-detector rows, found {len(week_results)}")
    detector_counts = week_results.groupby(["repo_full", "week"]).size()
    if not detector_counts.eq(6).all() or len(detector_counts) != 195:
        raise RuntimeError("Each eligible repository-week must have exactly six detectors")
    return {
        "eligible_repository_weeks": 195,
        "detectors_per_week": 6,
        "expected_rows": 1170,
        "actual_rows": int(len(week_results)),
    }


def construct_episodes(detector_rows: pd.DataFrame, full_alarm: pd.Series) -> pd.DataFrame:
    rows = []
    ordered = detector_rows.sort_values("week").copy()
    if ordered.empty:
        return pd.DataFrame(columns=EPISODE_COLUMNS)
    full = nullable_boolean(full_alarm)
    active: list[int] = []

    def close_episode() -> None:
        nonlocal active
        if not active:
            return
        block = ordered.loc[active].sort_values("week")
        first = block.iloc[0]
        last = block.iloc[-1]
        first_position = ordered.index.get_loc(block.index[0])
        last_position = ordered.index.get_loc(block.index[-1])
        left_censored = False
        if first_position == 0:
            prior_week = first["week"] - pd.Timedelta(days=7)
            prior = full[full.index == prior_week]
            left_censored = bool(
                len(prior) and pd.notna(prior.iloc[0]) and bool(prior.iloc[0])
            )
        right_censored = bool(last_position == len(ordered) - 1)
        rows.append({
            "repo_full": first["repo_full"],
            "evaluation_population": first["evaluation_population"],
            "detector_family": first["detector_family"],
            "detector_variant": first["detector_variant"],
            "metric": first["metric"],
            "start_week": first["week"],
            "end_week": last["week"],
            "left_censored": left_censored,
            "right_censored": right_censored,
            "observed_duration_weeks": len(block),
            "median_magnitude": block["magnitude"].median(),
            "maximum_magnitude": block["magnitude"].max(),
            "attempts_total_sum": block["attempts_total"].sum(),
            "outcome_n_sum": block["outcome_n"].sum(),
            "latency_n_sum": block["latency_n"].sum(),
            "logical_run_n_sum": block["logical_run_n"].sum(),
            "minimum_attempts_total": block["attempts_total"].min(),
            "minimum_outcome_n": block["outcome_n"].min(),
            "minimum_latency_n": block["latency_n"].min(),
            "minimum_logical_run_n": block["logical_run_n"].min(),
            "median_workflow_count": block["workflow_count"].median(),
            "maximum_workflow_concentration_hhi": block["workflow_concentration_hhi"].max(),
            "low_support_week_count": int(
                (nullable_boolean(block["low_outcome_support"]).fillna(False)
                 | nullable_boolean(block["low_latency_support"]).fillna(False)).sum()
            ),
            "missing_core_metric_week_count": int(block["missing_core_metric_count"].gt(0).sum()),
            "interpretation": "candidate detector episode, not ground truth",
        })
        active = []

    previous_week = None
    for index, row in ordered.iterrows():
        alarm = row["alarm"]
        consecutive = previous_week is not None and (row["week"] - previous_week).days == 7
        if pd.notna(alarm) and bool(alarm):
            if active and not consecutive:
                close_episode()
            active.append(index)
        else:
            close_episode()
        previous_week = row["week"]
    close_episode()
    return pd.DataFrame(rows, columns=EPISODE_COLUMNS)


def summarize_observed(week_results: pd.DataFrame, episodes: pd.DataFrame) -> pd.DataFrame:
    rows = []
    keys = ["repo_full", "evaluation_population", "detector_family", "detector_variant", "metric"]
    for key, group in week_results.groupby(keys, sort=True):
        evaluable = group["evaluable"].fillna(False).astype(bool)
        alarms = nullable_boolean(group["alarm"]).fillna(False)
        episode_group = episodes
        for column, value in zip(keys, key):
            episode_group = episode_group[episode_group[column].eq(value)]
        magnitudes = pd.to_numeric(group.loc[evaluable & alarms, "magnitude"], errors="coerce").dropna()
        durations = pd.to_numeric(episode_group["observed_duration_weeks"], errors="coerce").dropna()
        rows.append({
            "evidence_domain": "observed_research",
            **dict(zip(keys, key)),
            "specification_role": "primary",
            "total_weeks": len(group),
            "evaluable_weeks": int(evaluable.sum()),
            "unevaluable_weeks": int((~evaluable).sum()),
            "alarm_weeks": int((evaluable & alarms).sum()),
            "alarm_prevalence": float(alarms[evaluable].mean()) if evaluable.any() else np.nan,
            "episode_count": len(episode_group),
            "complete_episode_count": int((~episode_group["left_censored"] & ~episode_group["right_censored"]).sum()),
            "left_censored_episode_count": int(episode_group["left_censored"].sum()),
            "right_censored_episode_count": int(episode_group["right_censored"].sum()),
            "total_observed_episode_weeks": int(durations.sum()) if len(durations) else 0,
            "median_observed_duration_weeks": durations.median() if len(durations) else np.nan,
            "maximum_observed_duration_weeks": durations.max() if len(durations) else np.nan,
            "median_alarm_magnitude": magnitudes.median() if len(magnitudes) else np.nan,
            "maximum_alarm_magnitude": magnitudes.max() if len(magnitudes) else np.nan,
            "attempts_total_sum": group["attempts_total"].sum(),
            "outcome_n_sum": group["outcome_n"].sum(),
            "latency_n_sum": group["latency_n"].sum(),
            "logical_run_n_sum": group["logical_run_n"].sum(),
            "synthetic_scenarios": np.nan,
            **{metric: np.nan for metric in SYNTHETIC_METRICS},
            **{f"{metric}_valid_observations": 0 for metric in SYNTHETIC_METRICS},
        })
    return pd.DataFrame(rows, columns=SUMMARY_COLUMNS)


def synthetic_summary(stage_table: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, source in stage_table.iterrows():
        variant = source["detector_variant"]
        rows.append({
            "evidence_domain": "synthetic_stage_a",
            "repo_full": source["repo_full"],
            "evaluation_population": "development_training_synthetic",
            "detector_family": source["detector_family"],
            "detector_variant": variant,
            "metric": variant if variant in CORE_METRICS else "composite",
            "specification_role": "primary",
            "total_weeks": np.nan,
            "evaluable_weeks": np.nan,
            "unevaluable_weeks": np.nan,
            "alarm_weeks": np.nan,
            "alarm_prevalence": np.nan,
            "episode_count": np.nan,
            "complete_episode_count": np.nan,
            "left_censored_episode_count": np.nan,
            "right_censored_episode_count": np.nan,
            "total_observed_episode_weeks": np.nan,
            "median_observed_duration_weeks": np.nan,
            "maximum_observed_duration_weeks": np.nan,
            "median_alarm_magnitude": np.nan,
            "maximum_alarm_magnitude": np.nan,
            "attempts_total_sum": np.nan,
            "outcome_n_sum": np.nan,
            "latency_n_sum": np.nan,
            "logical_run_n_sum": np.nan,
            "synthetic_scenarios": source["scenario_count"],
            **{metric: source[f"mean_{metric}"] for metric in SYNTHETIC_METRICS},
            **{
                f"{metric}_valid_observations": source[f"{metric}_valid_observations"]
                for metric in SYNTHETIC_METRICS
            },
        })
    return pd.DataFrame(rows, columns=SUMMARY_COLUMNS)


def detector_agreement(week_results: pd.DataFrame) -> pd.DataFrame:
    rows = []
    mad = week_results[week_results["detector_family"].eq("causal_rolling_mad")]
    mewma = week_results[week_results["detector_family"].eq("mewma")]
    for (repo, population), group in mad.groupby(["repo_full", "evaluation_population"], sort=True):
        reference = mewma[
            mewma["repo_full"].eq(repo) & mewma["evaluation_population"].eq(population)
        ][["week", "evaluable", "alarm"]].rename(
            columns={"evaluable": "mewma_evaluable", "alarm": "mewma_alarm"}
        )
        for variant, variant_group in group.groupby("detector_variant", sort=True):
            joined = variant_group[["week", "evaluable", "alarm"]].merge(reference, on="week")
            eligible = joined["evaluable"].astype(bool) & joined["mewma_evaluable"].astype(bool)
            scored = joined.loc[eligible]
            mad_alarm = nullable_boolean(scored["alarm"]).fillna(False)
            mewma_alarm = nullable_boolean(scored["mewma_alarm"]).fillna(False)
            both = int((mad_alarm & mewma_alarm).sum())
            neither = int((~mad_alarm & ~mewma_alarm).sum())
            mad_only = int((mad_alarm & ~mewma_alarm).sum())
            mewma_only = int((~mad_alarm & mewma_alarm).sum())
            union = both + mad_only + mewma_only
            rows.append({
                "repo_full": repo,
                "evaluation_population": population,
                "mad_variant": variant,
                "mewma_variant": "mewma",
                "both_evaluable_weeks": len(scored),
                "both_alarm_weeks": both,
                "neither_alarm_weeks": neither,
                "mad_only_alarm_weeks": mad_only,
                "mewma_only_alarm_weeks": mewma_only,
                "observed_agreement": (both + neither) / len(scored) if len(scored) else np.nan,
                "jaccard_alarm_overlap": both / union if union else np.nan,
                "interpretation": "detector agreement, not ground truth",
            })
    return pd.DataFrame(rows, columns=AGREEMENT_COLUMNS)


def support_diagnostics(panel: pd.DataFrame, populations: pd.Series) -> pd.DataFrame:
    rows = []
    scored = panel.loc[populations.notna()].copy()
    scored["evaluation_population"] = populations.loc[scored.index]
    for (repo, population), group in scored.groupby(["repo_full", "evaluation_population"], sort=True):
        rows.append({
            "repo_full": repo,
            "evaluation_population": population,
            "total_weeks": len(group),
            "attempts_total_sum": group["attempts_total"].sum(),
            "outcome_n_sum": group["outcome_n"].sum(),
            "latency_n_sum": group["latency_n"].sum(),
            "logical_run_n_sum": group["logical_run_n"].sum(),
            "low_outcome_support_weeks": int(nullable_boolean(group["low_outcome_support"]).fillna(False).sum()),
            "low_latency_support_weeks": int(nullable_boolean(group["low_latency_support"]).fillna(False).sum()),
            "missing_core_metric_weeks": int(group["missing_core_metric_count"].gt(0).sum()),
            "median_workflow_count": group["workflow_count"].median(),
            "median_workflow_concentration_hhi": group["workflow_concentration_hhi"].median(),
            "maximum_workflow_concentration_hhi": group["workflow_concentration_hhi"].max(),
            "release_context_available": bool(group["release_count"].notna().any()),
        })
    return pd.DataFrame(rows, columns=SUPPORT_COLUMNS)


def artifact_record(path: Path, frame: pd.DataFrame, role: str) -> dict:
    return {
        "path": f"analysis_outputs/{path.name}",
        "sha256": sha256(path),
        "bytes": path.stat().st_size,
        "row_count": int(len(frame)),
        "schema": list(frame.columns),
        "role": role,
    }


def commit_without_overwrite(source: Path, destination: Path) -> None:
    if destination.exists():
        raise RuntimeError(f"Refusing to overwrite RQ1 output: {destination.name}")
    try:
        os.link(source, destination)
    except FileExistsError as exc:
        raise RuntimeError(
            f"RQ1 output appeared during commit: {destination.name}"
        ) from exc
    source.unlink()


def atomic_output_bundle(output_directory: Path, frames: dict[str, pd.DataFrame], status: dict) -> None:
    staging = Path(tempfile.mkdtemp(prefix="rq1_baseline_", dir=output_directory))
    committed: list[Path] = []
    try:
        for name, frame in frames.items():
            frame.to_csv(staging / name, index=False, date_format="%Y-%m-%d")
        status = dict(status)
        status["output_artifacts"] = {
            name: artifact_record(
                staging / name,
                frame,
                "baseline_only_confirmatory_rq1_output",
            )
            for name, frame in frames.items()
        }
        status["status_artifact"] = {
            "path": "analysis_outputs/rq1_baseline_validation_status.json",
            "schema": sorted([*status.keys(), "status_artifact"]),
            "row_count": 1,
            "role": "self_describing_validation_status",
            "hash_policy": (
                "The status file cannot contain its own SHA-256 without recursion; "
                "freeze_rq1_baselines.py validates its current hash separately."
            ),
        }
        (staging / "rq1_baseline_validation_status.json").write_text(
            json.dumps(status, indent=2, sort_keys=True, allow_nan=False) + "\n",
            encoding="utf-8",
        )
        for name in OUTPUT_NAMES:
            candidate = staging / name
            if not candidate.is_file() or candidate.stat().st_size == 0:
                raise RuntimeError(f"Invalid staged RQ1 output: {name}")
        for name in OUTPUT_NAMES:
            destination = output_directory / name
            if destination.exists():
                raise RuntimeError(f"Refusing to overwrite RQ1 output: {name}")
            commit_without_overwrite(staging / name, destination)
            committed.append(destination)
    except Exception:
        for path in committed:
            path.unlink(missing_ok=True)
        raise
    finally:
        shutil.rmtree(staging, ignore_errors=True)


def run(config_path: str | Path) -> dict:
    config_path = Path(config_path).resolve()
    root = config_path.parent
    analysis_cfg = load_yaml(config_path)
    output_directory = root / analysis_cfg["paths"]["output"]
    measurement = validate_manifest(root, "analysis_outputs/MEASUREMENT_FROZEN.json")
    stage_a = validate_manifest(root, "analysis_outputs/STAGE_A_FROZEN.json")
    hmm = validate_hmm_rejection(root)
    protected = {
        (root / relative).resolve()
        for manifest in (measurement, stage_a)
        for relative in manifest["manifest"]["files"]
    }
    for name in OUTPUT_NAMES:
        destination = (output_directory / name).resolve()
        if destination.parent != output_directory.resolve() or destination in protected:
            raise RuntimeError(f"Invalid RQ1 output path: {name}")
        if destination.exists():
            raise RuntimeError(f"RQ1 output already exists: {name}")
    baseline_path = output_directory / "baseline_results.csv"
    panel = pd.read_csv(baseline_path, parse_dates=["week"])
    panel = panel.sort_values(["repo_full", "week"]).reset_index(drop=True)
    if panel.duplicated(["repo_full", "week"]).any():
        raise RuntimeError("Baseline panel has duplicate keys")
    contract = load_detector_contract(stage_a["manifest"], set(panel.columns))
    mewma_validation = validate_frozen_mewma_panel(panel, contract)
    populations = evaluation_population(panel, analysis_cfg)
    expected, repository_denominators = validate_evaluation_denominators(
        panel, populations, analysis_cfg
    )
    week_results = build_week_results(panel, populations, contract)
    week_detector_contract = validate_week_detector_rows(week_results)
    episodes = []
    for (repo, population, family, variant), group in week_results.groupby(
        ["repo_full", "evaluation_population", "detector_family", "detector_variant"],
        sort=True,
    ):
        full_repo = panel[panel["repo_full"].eq(repo)].sort_values("week")
        if family == "causal_rolling_mad":
            resolved = next(
                item for item in contract["mad"]["variants_resolved"]
                if item["variant"] == variant
            )
            alarm = nullable_boolean(full_repo[resolved["alarm_column"]])
        else:
            alarm = nullable_boolean(full_repo["mewma_alarm"])
        alarm.index = full_repo["week"]
        episodes.append(construct_episodes(group, alarm))
    episode_table = pd.concat(episodes, ignore_index=True) if episodes else pd.DataFrame(columns=EPISODE_COLUMNS)
    observed_summary = summarize_observed(week_results, episode_table)
    stage_table = pd.read_csv(output_directory / "stage_a_table_primary_by_repository.csv")
    synthetic = synthetic_summary(stage_table)
    summary = pd.concat([observed_summary, synthetic], ignore_index=True)
    agreement = detector_agreement(week_results)
    support = support_diagnostics(panel, populations)
    status = {
        "status": "PASS",
        "provenance": {
            "measurement_freeze": {k: v for k, v in measurement.items() if k != "manifest"},
            "stage_a_freeze": {k: v for k, v in stage_a.items() if k != "manifest"},
            "hmm_rejection": hmm,
            "analysis_config_sha256": sha256(config_path),
            "baseline_results_sha256": sha256(baseline_path),
        },
        "contracts": contract,
        "mewma_panel_validation": mewma_validation,
        "observed_denominators": expected,
        "repository_denominators": {
            f"{repository}|{population}": int(weeks)
            for (repository, population), weeks in repository_denominators.items()
        },
        "causal_history": {
            "holdout": "frozen histories include preceding purge observations",
            "external_evaluation": "frozen histories include 13 calibration weeks for initialization only",
        },
        "episode_contract": {
            "adjacent_alarm_weeks_merged": True,
            "adjacency_days": 7,
            "false_or_unevaluable_week_breaks_episode": True,
            "left_censoring_uses_prior_frozen_alarm": True,
            "right_censoring_applies_at_evaluation_boundary": True,
            "duration_field": "observed_duration_weeks",
        },
        "declarations": {
            "observed_ground_truth_available": False,
            "observed_confusion_metrics_reported": False,
            "synthetic_and_observed_denominators_combined": False,
            "agreement_is_ground_truth": False,
            "detectors_refitted": False,
            "parameters_tuned": False,
            "hmm_excluded_before_confirmatory_scoring": True,
        },
        "row_counts": {
            "week_results": len(week_results),
            "summary": len(summary),
            "candidate_episodes": len(episode_table),
            "agreement": len(agreement),
            "support": len(support),
        },
        "week_detector_contract": week_detector_contract,
        "warnings": [
            "Release context is unavailable in the frozen measurement panel.",
            "Observed alarm prevalence and agreement are not ground-truth performance metrics.",
            "Right-boundary episodes are censored when still active in the final evaluation week.",
        ],
    }
    atomic_output_bundle(
        output_directory,
        {
            "rq1_baseline_week_results.csv": week_results,
            "rq1_baseline_summary.csv": summary,
            "rq1_baseline_candidate_episodes.csv": episode_table,
            "rq1_baseline_detector_agreement.csv": agreement,
            "rq1_baseline_support_diagnostics.csv": support,
        },
        status,
    )
    return status


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="analysis_config.yaml")
    args = parser.parse_args()
    result = run(args.config)
    print(json.dumps({"status": result["status"], "row_counts": result["row_counts"]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
