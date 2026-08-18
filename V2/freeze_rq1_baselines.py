from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

import validate_rq1_baselines as rq1


RQ1_DATA_OUTPUTS = tuple(
    name for name in rq1.OUTPUT_NAMES
    if name != "rq1_baseline_validation_status.json"
)
HMM_OUTPUTS = (
    "hmm_results.csv",
    "hmm_model.json",
    "hmm_start_diagnostics.csv",
    "hmm_split_summary.csv",
)
EXPECTED_SCHEMAS = {
    "rq1_baseline_week_results.csv": tuple(rq1.WEEK_COLUMNS),
    "rq1_baseline_summary.csv": tuple(rq1.SUMMARY_COLUMNS),
    "rq1_baseline_candidate_episodes.csv": tuple(rq1.EPISODE_COLUMNS),
    "rq1_baseline_detector_agreement.csv": tuple(rq1.AGREEMENT_COLUMNS),
    "rq1_baseline_support_diagnostics.csv": tuple(rq1.SUPPORT_COLUMNS),
}


def sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_json(path: Path) -> dict:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Cannot read JSON {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise RuntimeError(f"JSON root must be an object: {path}")
    return value


def validate_manifest(root: Path, relative: str) -> dict:
    path = root / relative
    manifest = load_json(path)
    records = manifest.get("files")
    if not isinstance(records, dict) or not records:
        raise RuntimeError(f"Manifest has no file records: {relative}")
    failures = []
    for item, record in records.items():
        candidate = root / item
        if not candidate.is_file():
            failures.append(f"{item}: missing")
        elif candidate.stat().st_size != int(record.get("bytes", -1)):
            failures.append(f"{item}: byte size differs")
        elif sha256(candidate) != str(record.get("sha256", "")):
            failures.append(f"{item}: SHA-256 differs")
    if failures:
        raise RuntimeError(
            f"Manifest validation failed: {relative}: " + "; ".join(failures)
        )
    return {
        "path": relative,
        "sha256": sha256(path),
        "bytes": path.stat().st_size,
        "files_verified": len(records),
    }


def validate_rejection(root: Path) -> dict:
    path = root / "analysis_outputs" / "HMM_REJECTION_REPORT.json"
    report = load_json(path)
    valid = (
        report.get("status") == "HMM_REJECTED"
        and report.get("decision", {}).get("confirmatory_rq1_inclusion") is False
        and report.get("model_use", {}).get("model_selected") is False
        and report.get("model_use", {}).get(
            "model_used_for_repository_scoring"
        ) is False
    )
    if not valid:
        raise RuntimeError("HMM rejection report is incompatible with baseline-only RQ1")
    return {
        "path": "analysis_outputs/HMM_REJECTION_REPORT.json",
        "sha256": sha256(path),
        "bytes": path.stat().st_size,
    }


def validate_status_provenance(
    root: Path,
    config_path: Path,
    status: dict,
    measurement: dict,
    stage_a: dict,
    rejection: dict,
) -> None:
    provenance = status.get("provenance")
    if not isinstance(provenance, dict):
        raise RuntimeError("Validation status has no provenance")
    expected_records = {
        "measurement_freeze": measurement,
        "stage_a_freeze": stage_a,
        "hmm_rejection": rejection,
    }
    for label, expected in expected_records.items():
        record = provenance.get(label)
        if not isinstance(record, dict):
            raise RuntimeError(f"Validation status has no provenance for {label}")
        if record.get("path") != expected["path"]:
            raise RuntimeError(f"Validation status path is stale: {label}")
        if record.get("sha256") != expected["sha256"]:
            raise RuntimeError(f"Validation status hash is stale: {label}")
    baseline = root / "analysis_outputs" / "baseline_results.csv"
    if provenance.get("analysis_config_sha256") != sha256(config_path):
        raise RuntimeError("Validation status has stale analysis configuration")
    if provenance.get("baseline_results_sha256") != sha256(baseline):
        raise RuntimeError("Validation status has stale baseline results")


def validate_week_contract(status: dict) -> None:
    contract = status.get("week_detector_contract")
    expected = {
        "eligible_repository_weeks": 195,
        "detectors_per_week": 6,
        "expected_rows": 1170,
        "actual_rows": 1170,
    }
    if contract != expected:
        raise RuntimeError("Validation status has an invalid week-detector contract")
    totals = status.get("observed_denominators")
    if totals != {"external_evaluation": 138, "holdout": 57}:
        raise RuntimeError("Validation status has invalid evaluation denominators")
    repository = status.get("repository_denominators")
    if not isinstance(repository, dict) or len(repository) != 6:
        raise RuntimeError("Validation status has incomplete repository denominators")
    holdout = [value for key, value in repository.items() if key.endswith("|holdout")]
    external = [
        value for key, value in repository.items()
        if key.endswith("|external_evaluation")
    ]
    if sorted(holdout) != [19, 19, 19] or sorted(external) != [46, 46, 46]:
        raise RuntimeError("Repository-specific evaluation denominators are invalid")


def validate_declarations(status: dict) -> None:
    declarations = status.get("declarations", {})
    required_false = (
        "observed_ground_truth_available",
        "observed_confusion_metrics_reported",
        "synthetic_and_observed_denominators_combined",
        "agreement_is_ground_truth",
        "detectors_refitted",
        "parameters_tuned",
    )
    if any(declarations.get(name) is not False for name in required_false):
        raise RuntimeError("Validation status violates a baseline-only declaration")
    if declarations.get("hmm_excluded_before_confirmatory_scoring") is not True:
        raise RuntimeError("Validation status does not exclude HMM")


def validate_output_records(output: Path, status: dict) -> dict:
    records = status.get("output_artifacts")
    if not isinstance(records, dict) or set(records) != set(RQ1_DATA_OUTPUTS):
        raise RuntimeError("Validation status output artifact set is incomplete")
    validated = {}
    for name in RQ1_DATA_OUTPUTS:
        path = output / name
        if not path.is_file():
            raise FileNotFoundError(path)
        record = records[name]
        frame = pd.read_csv(path)
        schema = list(frame.columns)
        if tuple(schema) != EXPECTED_SCHEMAS[name]:
            raise RuntimeError(f"Unexpected output schema: {name}")
        current = {
            "path": f"analysis_outputs/{name}",
            "sha256": sha256(path),
            "bytes": path.stat().st_size,
            "row_count": len(frame),
            "schema": schema,
        }
        for key, value in current.items():
            if record.get(key) != value:
                raise RuntimeError(f"Validation status has stale output record: {name}")
        validated[name] = current
    self_record = status.get("status_artifact")
    expected_path = "analysis_outputs/rq1_baseline_validation_status.json"
    if not isinstance(self_record, dict) or self_record.get("path") != expected_path:
        raise RuntimeError("Validation status self-record is invalid")
    expected_schema = sorted(status.keys())
    if self_record.get("schema") != expected_schema:
        raise RuntimeError("Validation status self-schema is stale")
    if int(self_record.get("row_count", -1)) != 1:
        raise RuntimeError("Validation status self-row count is invalid")
    return validated


def source_record(root: Path, relative: str) -> dict:
    path = root / relative
    if not path.is_file():
        raise FileNotFoundError(path)
    return {
        "path": relative,
        "sha256": sha256(path),
        "bytes": path.stat().st_size,
    }


def atomic_json(payload: dict, target: Path) -> None:
    handle = tempfile.NamedTemporaryFile(
        mode="w",
        dir=target.parent,
        suffix=".tmp",
        encoding="utf-8",
        delete=False,
    )
    temporary = Path(handle.name)
    try:
        json.dump(payload, handle, indent=2, sort_keys=True, allow_nan=False)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
        handle.close()
        if target.exists():
            raise RuntimeError(f"Refusing to overwrite freeze manifest: {target}")
        try:
            os.link(temporary, target)
        except FileExistsError as exc:
            raise RuntimeError(f"Freeze manifest appeared during commit: {target}") from exc
    finally:
        if not handle.closed:
            handle.close()
        temporary.unlink(missing_ok=True)


def freeze(config_path: str | Path, confirm: bool) -> dict:
    if not confirm:
        raise RuntimeError("Explicit --confirm is required after methodological review")
    config_path = Path(config_path).resolve()
    root = config_path.parent
    output = root / "analysis_outputs"
    target = output / "RQ1_BASELINES_FROZEN.json"
    if target.exists():
        raise RuntimeError(f"Refusing to overwrite freeze manifest: {target}")
    present_hmm = [name for name in HMM_OUTPUTS if (output / name).exists()]
    if present_hmm:
        raise RuntimeError("HMM outputs must not enter baseline-only RQ1 freeze")
    measurement = validate_manifest(
        root, "analysis_outputs/MEASUREMENT_FROZEN.json"
    )
    stage_a = validate_manifest(root, "analysis_outputs/STAGE_A_FROZEN.json")
    rejection = validate_rejection(root)
    status_path = output / "rq1_baseline_validation_status.json"
    status = load_json(status_path)
    if status.get("status") != "PASS":
        raise RuntimeError("Baseline-only RQ1 validation status is not PASS")
    validate_status_provenance(
        root, config_path, status, measurement, stage_a, rejection
    )
    validate_week_contract(status)
    validate_declarations(status)
    validated_outputs = validate_output_records(output, status)
    records = {}
    for name, record in validated_outputs.items():
        records[f"analysis_outputs/{name}"] = {
            **record,
            "role": "baseline_only_confirmatory_rq1_output",
        }
    records["analysis_outputs/rq1_baseline_validation_status.json"] = {
        "path": "analysis_outputs/rq1_baseline_validation_status.json",
        "sha256": sha256(status_path),
        "bytes": status_path.stat().st_size,
        "row_count": 1,
        "schema": sorted(status.keys()),
        "role": "baseline_only_confirmatory_rq1_validation_status",
    }
    payload = {
        "manifest_version": 1,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "status": "FROZEN",
        "files": records,
        "provenance": {
            "measurement_freeze": measurement,
            "stage_a_freeze": stage_a,
            "hmm_rejection": rejection,
            "analysis_config": source_record(root, config_path.name),
            "validation_source": source_record(root, "validate_rq1_baselines.py"),
            "validation_test": source_record(
                root, "test_rq1_baseline_validation.py"
            ),
            "freeze_source": source_record(root, "freeze_rq1_baselines.py"),
        },
        "detectors": ["causal rolling MAD", "MEWMA"],
        "declarations": {
            "hmm_included": False,
            "observed_ground_truth_available": False,
            "detector_selected_by_observed_results": False,
            "parameter_tuning_authorized": False,
            "rq2_authorized": False,
        },
        "rule": (
            "No detector setting, threshold, repository, evaluation denominator, "
            "or reporting rule may be changed to maximize downstream associations."
        ),
    }
    atomic_json(payload, target)
    return {
        "status": "PASS",
        "path": str(target),
        "sha256": sha256(target),
        "bytes": target.stat().st_size,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="analysis_config.yaml")
    parser.add_argument("--confirm", action="store_true")
    args = parser.parse_args()
    print(json.dumps(freeze(args.config, args.confirm), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
