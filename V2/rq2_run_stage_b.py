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
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from rq2_stage_b_core import (
    RESPONSE_HORIZONS,
    build_stage_b_scenario_grid,
    evaluate_paired_alarms,
    primary_detector_alarms,
)


SCENARIO_FIELDS = (
    "scenario_id", "repo_full", "scenario_type", "affected_metrics",
    "dimensionality", "magnitude_level", "magnitude", "duration_weeks",
    "volume_condition", "missingness_condition", "repetition", "seed",
    "start_index", "end_index", "injected_weeks", "status",
)

DETECTOR_RESULT_FIELDS = (
    *SCENARIO_FIELDS,
    "detector_id", "detector_family", "detector_variant",
    "strict_incremental_episode_detected",
    "strict_incremental_detection_delay_weeks",
    "reference_alarm_weeks", "reference_alarm_burden",
    "injected_alarm_weeks", "incremental_alarm_weeks",
    "incremental_alarm_duration_weeks", "incremental_spillover_weeks",
    "total_operational_episode_detected", "reference_alarm_overlap_fraction",
    "incremental_boundary_overlap", "pair_evaluable_weeks",
    "pair_unevaluable_weeks", "pair_unevaluable_fraction",
    "injection_relative_tp", "injection_relative_fp",
    "injection_relative_tn", "injection_relative_fn",
    "injection_relative_precision", "injection_relative_false_alarm_rate",
    "incremental_detected_within_0w_post",
    "incremental_detected_within_1w_post",
    "incremental_detected_within_2w_post",
    "incremental_detected_within_4w_post",
    "probability_clipping_count", "injected_missing_cells",
)

WEEK_RESULT_FIELDS = (
    "scenario_id", "repo_full", "detector_id", "week", "truth",
    "pair_evaluable", "reference_alarm", "injected_alarm",
    "incremental_alarm", "spillover_alarm",
)

SUMMARY_KEYS = (
    "repo_full", "detector_family", "detector_variant", "scenario_type",
    "affected_metrics", "dimensionality", "magnitude_level",
    "duration_weeks", "volume_condition", "missingness_condition",
)

SUMMARY_METRICS = (
    "strict_incremental_episode_detected",
    "strict_incremental_detection_delay_weeks",
    "reference_alarm_burden",
    "incremental_alarm_duration_weeks",
    "incremental_spillover_weeks",
    "total_operational_episode_detected",
    "reference_alarm_overlap_fraction",
    "incremental_boundary_overlap",
    "pair_unevaluable_fraction",
    "injection_relative_precision",
    "injection_relative_false_alarm_rate",
    "incremental_detected_within_0w_post",
    "incremental_detected_within_1w_post",
    "incremental_detected_within_2w_post",
    "incremental_detected_within_4w_post",
)

EXPECTED_OUTPUTS = (
    "rq2_stage_b_scenarios.csv",
    "rq2_stage_b_paired_week_results.csv.gz",
    "rq2_stage_b_detector_results.csv.gz",
    "rq2_stage_b_primary_summary.csv",
    "rq2_stage_b_metadata.json",
    "rq2_stage_b_validation_status.json",
)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def load_yaml(path: Path) -> dict:
    value = yaml.safe_load(path.read_text(encoding="utf-8-sig"))
    if not isinstance(value, dict):
        raise RuntimeError(f"YAML root is not a mapping: {path}")
    return value


def import_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ImportError(path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def verify_protocol_freeze(root: Path, cfg: dict) -> dict:
    manifest_path = root / "analysis_outputs" / "RQ2_STAGE_B_PROTOCOL_FROZEN.json"
    if not manifest_path.is_file():
        raise RuntimeError(
            "Stage-B protocol is not frozen. Only --plan-only is permitted."
        )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("status") != "FROZEN":
        raise RuntimeError("Stage-B protocol manifest status is not FROZEN")
    failures = {}
    for relative, record in manifest.get("files", {}).items():
        path = root / relative
        if not path.is_file():
            failures[relative] = "missing"
        elif path.stat().st_size != int(record["bytes"]):
            failures[relative] = "size mismatch"
        elif sha256(path) != str(record["sha256"]):
            failures[relative] = "hash mismatch"
    if failures:
        raise RuntimeError("Stage-B protocol freeze mismatch: " + json.dumps(failures))
    if manifest.get("declarations", {}).get("confirmatory_run_authorized") is not True:
        raise RuntimeError("Stage-B confirmatory run is not authorised by the freeze")
    if manifest.get("config_sha256") != sha256(root / "stage_b_confirmatory_config.yaml"):
        raise RuntimeError("Stage-B configuration differs from frozen protocol")
    return {"path": str(manifest_path.relative_to(root)), "sha256": sha256(manifest_path)}


class AtomicGzipCsvWriter:
    def __init__(self, path: Path, fields: tuple[str, ...]):
        self.path = path
        self.fields = fields
        self.temporary: Path | None = None
        self.raw = self.compressed = self.text = self.writer = None

    def __enter__(self):
        handle = tempfile.NamedTemporaryFile(dir=self.path.parent, delete=False)
        self.temporary = Path(handle.name)
        handle.close()
        self.raw = self.temporary.open("wb")
        self.compressed = gzip.GzipFile(filename="", mode="wb", fileobj=self.raw, mtime=0)
        self.text = io.TextIOWrapper(self.compressed, encoding="utf-8", newline="")
        self.writer = csv.DictWriter(
            self.text, fieldnames=self.fields, extrasaction="ignore", lineterminator="\n"
        )
        self.writer.writeheader()
        return self

    def writerow(self, row: dict) -> None:
        self.writer.writerow(row)

    def __exit__(self, exc_type, exc, traceback):
        self.text.flush()
        self.text.close()
        self.raw.close()
        if exc_type is None:
            os.replace(self.temporary, self.path)
        else:
            self.temporary.unlink(missing_ok=True)


def atomic_csv(frame: pd.DataFrame, path: Path) -> None:
    handle = tempfile.NamedTemporaryFile(
        mode="w", dir=path.parent, delete=False, encoding="utf-8", newline=""
    )
    temporary = Path(handle.name)
    handle.close()
    try:
        frame.to_csv(temporary, index=False, lineterminator="\n")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def atomic_json(value: dict, path: Path) -> None:
    handle = tempfile.NamedTemporaryFile(
        mode="w", dir=path.parent, delete=False, encoding="utf-8"
    )
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


def detector_identity(detector_id: str) -> tuple[str, str]:
    family, variant = detector_id.split(":", 1)
    return ("causal_rolling_mad" if family == "mad" else family, variant)


def update_summary(summary: dict, row: dict) -> None:
    key = tuple(row[field] for field in SUMMARY_KEYS)
    item = summary.setdefault(key, {
        "scenario_detector_rows": 0,
        **{f"{metric}_sum": 0.0 for metric in SUMMARY_METRICS},
        **{f"{metric}_n": 0 for metric in SUMMARY_METRICS},
    })
    item["scenario_detector_rows"] += 1
    for metric in SUMMARY_METRICS:
        value = row.get(metric)
        if value is not None and not pd.isna(value):
            item[f"{metric}_sum"] += float(value)
            item[f"{metric}_n"] += 1


def summary_frame(summary: dict) -> pd.DataFrame:
    rows = []
    for key, values in summary.items():
        row = dict(zip(SUMMARY_KEYS, key))
        row["scenario_detector_rows"] = values["scenario_detector_rows"]
        for metric in SUMMARY_METRICS:
            count = values[f"{metric}_n"]
            row[f"mean_{metric}"] = values[f"{metric}_sum"] / count if count else None
            row[f"{metric}_valid_observations"] = count
        rows.append(row)
    return pd.DataFrame(rows)


def build_plan(root: Path, cfg: dict) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    approved = cfg["approved_frozen_inputs"]
    panel = pd.read_csv(
        root / approved["baseline_results"]["path"], parse_dates=["week"], low_memory=False
    )
    grid = build_stage_b_scenario_grid(panel, cfg)
    grid_bytes = grid.to_csv(index=False, lineterminator="\n").encode("utf-8")
    grid_hash = hashlib.sha256(grid_bytes).hexdigest()
    per_repo = (
        grid.groupby(["repo_full", "scenario_type"], dropna=False)
        .size().rename("scenarios").reset_index()
    )
    plan = {
        "status": "PASS",
        "protocol_status": cfg["protocol_status"],
        "scenario_rows": int(len(grid)),
        "scenario_grid_sha256": grid_hash,
        "primary_detector_count": 6,
        "expected_detector_rows": int(len(grid) * 6),
        "expected_evaluation_week_rows": int(
            sum(
                grid["repo_full"].eq(repo).sum()
                * panel.loc[
                    panel["repo_full"].eq(repo), "external_evaluation_eligible"
                ].fillna(False).sum()
                * 6
                for repo in cfg["evidence_domain"]["repositories"]
            )
        ),
        "unique_seeds": int(grid["seed"].nunique()),
        "output_directory_exists": (root / cfg["outputs"]["directory"]).exists(),
        "research_outputs_written": False,
        "confirmatory_run_authorized": False,
    }
    return panel, grid, {"plan": plan, "per_repository": per_repo.to_dict("records")}


def run_confirmatory(root: Path, cfg: dict, panel: pd.DataFrame, grid: pd.DataFrame) -> dict:
    protocol = verify_protocol_freeze(root, cfg)
    output = root / cfg["outputs"]["directory"]
    if output.exists():
        raise RuntimeError(f"Refusing to overwrite Stage-B directory: {output}")
    output.mkdir(parents=True, exist_ok=False)
    expected = tuple(cfg["outputs"]["required_files"])
    if expected != EXPECTED_OUTPUTS:
        output.rmdir()
        raise RuntimeError("Stage-B output contract differs from implementation")

    approved = cfg["approved_frozen_inputs"]
    baseline_module = import_module(root / approved["baseline_source"]["path"], "rq2_baseline")
    stage_a_module = import_module(
        root / approved["stage_a_evaluator_source"]["path"], "rq2_stage_a"
    )
    metadata = json.loads(
        (root / approved["baseline_metadata"]["path"]).read_text(encoding="utf-8")
    )
    summary: dict = {}
    detector_rows = week_rows = 0

    atomic_csv(grid, output / EXPECTED_OUTPUTS[0])
    try:
        with AtomicGzipCsvWriter(output / EXPECTED_OUTPUTS[1], WEEK_RESULT_FIELDS) as week_writer, AtomicGzipCsvWriter(output / EXPECTED_OUTPUTS[2], DETECTOR_RESULT_FIELDS) as detector_writer:
            for repo in cfg["evidence_domain"]["repositories"]:
                reference = (
                    panel.loc[panel["repo_full"].eq(repo)]
                    .sort_values("week").reset_index(drop=True)
                )
                eligible = reference["external_evaluation_eligible"]
                reference_alarms = primary_detector_alarms(
                    reference, baseline_module, stage_a_module, metadata, repo=repo
                )
                for _, scenario in grid.loc[grid["repo_full"].eq(repo)].iterrows():
                    altered, truth, _, diagnostics = stage_a_module.apply_scenario(
                        reference, scenario, cfg
                    )
                    injected_alarms = primary_detector_alarms(
                        altered, baseline_module, stage_a_module, metadata, repo=repo
                    )
                    if set(reference_alarms) != set(injected_alarms):
                        raise AssertionError("Reference/injected detector sets differ")
                    for detector_id in reference_alarms:
                        evaluated = evaluate_paired_alarms(
                            scenario_id=str(scenario["scenario_id"]),
                            repo_full=repo,
                            detector_id=detector_id,
                            weeks=reference["week"], truth=truth, eligible=eligible,
                            reference_alarm=reference_alarms[detector_id],
                            injected_alarm=injected_alarms[detector_id],
                            response_horizons=RESPONSE_HORIZONS,
                        )
                        family, variant = detector_identity(detector_id)
                        row = {
                            **{field: scenario[field] for field in SCENARIO_FIELDS},
                            **evaluated.result,
                            "detector_family": family,
                            "detector_variant": variant,
                            "probability_clipping_count": diagnostics["clipping_count"],
                            "injected_missing_cells": diagnostics["missing_cells"],
                        }
                        detector_writer.writerow(row)
                        detector_rows += 1
                        update_summary(summary, row)
                        for week_row in evaluated.week_rows:
                            week_writer.writerow(week_row)
                            week_rows += 1
    except Exception:
        for path in output.iterdir():
            path.unlink(missing_ok=True)
        output.rmdir()
        raise

    summary_table = summary_frame(summary)
    atomic_csv(summary_table, output / EXPECTED_OUTPUTS[3])
    payload = {
        "status": "PASS",
        "claim_scope": "controlled perturbation responsiveness; not real-world ground truth",
        "protocol_freeze": protocol,
        "configuration_sha256": sha256(root / "stage_b_confirmatory_config.yaml"),
        "contract_sha256": sha256(root / "RQ2_Stage_B_Confirmatory_Contract.md"),
        "scenario_grid_sha256": sha256(output / EXPECTED_OUTPUTS[0]),
        "scenario_rows": int(len(grid)),
        "detector_result_rows": detector_rows,
        "paired_week_rows": week_rows,
        "summary_rows": int(len(summary_table)),
        "response_horizons_weeks": list(RESPONSE_HORIZONS),
        "detector_refitted": False,
        "outputs": list(EXPECTED_OUTPUTS),
    }
    atomic_json(payload, output / EXPECTED_OUTPUTS[4])
    status = {
        "status": "PASS",
        "expected_scenario_rows": int(len(grid)),
        "expected_detector_rows": int(len(grid) * 6),
        "actual_detector_rows": detector_rows,
        "detector_row_count_matches": detector_rows == int(len(grid) * 6),
        "outputs_present": [name for name in EXPECTED_OUTPUTS[:-1] if (output / name).is_file()],
    }
    atomic_json(status, output / EXPECTED_OUTPUTS[5])
    return payload


def main() -> int:
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--plan-only", action="store_true")
    mode.add_argument("--confirm-run", action="store_true")
    parser.add_argument("--config", default="stage_b_confirmatory_config.yaml")
    args = parser.parse_args()
    config_path = Path(args.config).resolve()
    root = config_path.parent
    cfg = load_yaml(config_path)
    panel, grid, plan = build_plan(root, cfg)
    print("RQ2 STAGE-B PLAN")
    print(json.dumps(plan, indent=2))
    if args.plan_only:
        return 0
    if cfg.get("protocol_status") != "frozen_confirmatory":
        raise RuntimeError(
            "Configuration is not frozen_confirmatory; confirmatory execution is blocked."
        )
    result = run_confirmatory(root, cfg, panel, grid)
    print("RQ2 STAGE-B CONFIRMATORY RUN")
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())