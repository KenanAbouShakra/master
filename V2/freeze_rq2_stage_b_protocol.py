from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import yaml

from rq2_run_stage_b import build_plan


EXPECTED_GRID_SHA256 = "b17550141accf12d8b01729baa7dc48e8fb7a63d226d747639bbbce6e2f17937"
EXPECTED_SCENARIOS = 42003
EXPECTED_DETECTOR_ROWS = 252018
RESPONSE_HORIZONS_WEEKS = [0, 1, 2, 4]

PROTOCOL_FILES = (
    "RQ2_Stage_B_Confirmatory_Contract.md",
    "stage_b_confirmatory_config.yaml",
    "preflight_rq2_stage_b.py",
    "rq2_stage_b_core.py",
    "test_rq2_stage_b_core.py",
    "rq2_run_stage_b.py",
    "test_rq2_stage_b_runner.py",
    "freeze_rq2_stage_b_protocol.py",
)


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_yaml(path: Path) -> dict:
    value = yaml.safe_load(path.read_text(encoding="utf-8-sig"))
    if not isinstance(value, dict):
        raise RuntimeError(f"YAML root is not a mapping: {path}")
    return value


def frozen_config_bytes(cfg: dict) -> bytes:
    frozen = dict(cfg)
    frozen["protocol_status"] = "frozen_confirmatory"
    paired = dict(frozen["paired_evaluation"])
    paired["response_horizons_weeks"] = RESPONSE_HORIZONS_WEEKS
    frozen["paired_evaluation"] = paired
    return yaml.safe_dump(
        frozen,
        sort_keys=False,
        allow_unicode=True,
        width=88,
    ).encode("utf-8")


def atomic_bytes(path: Path, value: bytes) -> None:
    handle = tempfile.NamedTemporaryFile(dir=path.parent, delete=False)
    temporary = Path(handle.name)
    try:
        handle.write(value)
        handle.flush()
        os.fsync(handle.fileno())
        handle.close()
        os.replace(temporary, path)
    finally:
        if not handle.closed:
            handle.close()
        temporary.unlink(missing_ok=True)


def validate(root: Path, config_path: Path) -> tuple[dict, bytes, dict]:
    cfg = load_yaml(config_path)
    if cfg.get("protocol_status") != "draft_not_authorized_for_execution":
        raise RuntimeError("Protocol must be draft_not_authorized_for_execution before freezing")
    output = root / cfg["outputs"]["directory"]
    if output.exists():
        raise RuntimeError(f"Stage-B output directory already exists: {output}")
    manifest_path = root / "analysis_outputs" / "RQ2_STAGE_B_PROTOCOL_FROZEN.json"
    if manifest_path.exists():
        raise RuntimeError(f"Protocol freeze manifest already exists: {manifest_path}")

    missing = [relative for relative in PROTOCOL_FILES if not (root / relative).is_file()]
    if missing:
        raise RuntimeError("Protocol files are missing: " + ", ".join(missing))

    _, grid, plan_payload = build_plan(root, cfg)
    plan = plan_payload["plan"]
    checks = {
        "scenario_rows": plan["scenario_rows"] == EXPECTED_SCENARIOS,
        "scenario_grid_sha256": plan["scenario_grid_sha256"] == EXPECTED_GRID_SHA256,
        "expected_detector_rows": plan["expected_detector_rows"] == EXPECTED_DETECTOR_ROWS,
        "unique_seeds": plan["unique_seeds"] == EXPECTED_SCENARIOS,
        "output_directory_absent": not plan["output_directory_exists"],
    }
    failures = [name for name, passed in checks.items() if not passed]
    if failures:
        raise RuntimeError("Freeze checks failed: " + ", ".join(failures))
    if len(grid) != EXPECTED_SCENARIOS:
        raise AssertionError("Scenario grid size changed during validation")

    return cfg, frozen_config_bytes(cfg), {"checks": checks, "plan": plan}


def main() -> int:
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--preflight-only", action="store_true")
    mode.add_argument("--confirm-freeze", action="store_true")
    parser.add_argument("--config", default="stage_b_confirmatory_config.yaml")
    args = parser.parse_args()

    config_path = Path(args.config).resolve()
    root = config_path.parent
    cfg, new_config, validation = validate(root, config_path)
    preview = {
        "status": "PASS",
        "action": "preflight_only" if args.preflight_only else "freeze_protocol",
        "current_protocol_status": cfg["protocol_status"],
        "frozen_protocol_status": "frozen_confirmatory",
        "response_horizons_weeks": RESPONSE_HORIZONS_WEEKS,
        "scenario_grid_sha256": validation["plan"]["scenario_grid_sha256"],
        "scenario_rows": validation["plan"]["scenario_rows"],
        "expected_detector_rows": validation["plan"]["expected_detector_rows"],
        "planned_config_sha256": sha256_bytes(new_config),
        "research_outputs_written": False,
        "confirmatory_run_executed": False,
    }
    if args.preflight_only:
        print("RQ2 STAGE-B PROTOCOL FREEZE PREFLIGHT")
        print(json.dumps(preview, indent=2))
        return 0

    original_config = config_path.read_bytes()
    manifest_path = root / "analysis_outputs" / "RQ2_STAGE_B_PROTOCOL_FROZEN.json"
    try:
        atomic_bytes(config_path, new_config)
        records = {}
        for relative in PROTOCOL_FILES:
            path = root / relative
            records[relative] = {"sha256": sha256(path), "bytes": path.stat().st_size}
        manifest = {
            "status": "FROZEN",
            "manifest_version": 1,
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "config_sha256": sha256(config_path),
            "scenario_grid_sha256": EXPECTED_GRID_SHA256,
            "scenario_rows": EXPECTED_SCENARIOS,
            "expected_detector_rows": EXPECTED_DETECTOR_ROWS,
            "response_horizons_weeks": RESPONSE_HORIZONS_WEEKS,
            "files": records,
            "declarations": {
                "confirmatory_run_authorized": True,
                "detector_refitting_authorized": False,
                "research_outputs_written_by_freeze": False,
                "protocol_changes_after_freeze_authorized": False,
            },
        }
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        atomic_bytes(
            manifest_path,
            (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode("utf-8"),
        )
    except Exception:
        atomic_bytes(config_path, original_config)
        manifest_path.unlink(missing_ok=True)
        raise

    print("RQ2 STAGE-B PROTOCOL FREEZE")
    print(json.dumps({
        "status": "FROZEN",
        "manifest": str(manifest_path.relative_to(root)),
        "manifest_sha256": sha256(manifest_path),
        "config_sha256": sha256(config_path),
        "files_frozen": len(PROTOCOL_FILES),
        "response_horizons_weeks": RESPONSE_HORIZONS_WEEKS,
        "confirmatory_run_authorized": True,
        "research_outputs_written": False,
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())