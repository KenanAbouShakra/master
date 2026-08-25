from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path


EXPECTED = {
    "rq2_stage_b_scenarios.csv": "b17550141accf12d8b01729baa7dc48e8fb7a63d226d747639bbbce6e2f17937",
    "rq2_stage_b_paired_week_results.csv.gz": "16591a5e23aac555bf588cc7980ff5b6d2dbf9bdae584655270ad43e9a197786",
    "rq2_stage_b_detector_results.csv.gz": "dafa4b8355f3173707c4234c2e768301b13f8ece0c70a67230e0132448cc36e0",
    "rq2_stage_b_primary_summary.csv": "6bb10cc5af7a7213b31f70915de31617e7b648678209b0884f10ef92a66e2496",
    "rq2_stage_b_metadata.json": "7fe9a65f1053a98671da7d400693b4992c0617b7c8c13d9a64990792970b0932",
    "rq2_stage_b_validation_status.json": "d5255b27404acd8113ae8b810e2cfa22b4a1c3507efc6dcf1519455df54e7c4d",
}
EXPECTED_PROTOCOL_MANIFEST_SHA256 = "f497e208726361488e981078a842b9dc0147cc6f0335034a82a610aeb60e9ba6"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def atomic_json(value: dict, path: Path) -> None:
    handle = tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=path.parent, delete=False
    )
    temporary = Path(handle.name)
    try:
        json.dump(value, handle, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
        handle.close()
        os.replace(temporary, path)
    finally:
        if not handle.closed:
            handle.close()
        temporary.unlink(missing_ok=True)


def audit(root: Path) -> tuple[dict, dict]:
    output = root / "analysis_outputs" / "rq2_stage_b"
    checks = {}
    records = {}
    for name, expected_hash in EXPECTED.items():
        path = output / name
        actual = sha256(path) if path.is_file() else None
        checks[name] = {
            "exists": path.is_file(),
            "expected_sha256": expected_hash,
            "actual_sha256": actual,
            "matches": actual == expected_hash,
        }
        if path.is_file():
            records[str(path.relative_to(root))] = {
                "sha256": actual,
                "bytes": path.stat().st_size,
            }

    protocol = root / "analysis_outputs" / "RQ2_STAGE_B_PROTOCOL_FROZEN.json"
    protocol_actual = sha256(protocol) if protocol.is_file() else None
    checks["protocol_manifest"] = {
        "exists": protocol.is_file(),
        "expected_sha256": EXPECTED_PROTOCOL_MANIFEST_SHA256,
        "actual_sha256": protocol_actual,
        "matches": protocol_actual == EXPECTED_PROTOCOL_MANIFEST_SHA256,
    }
    if protocol.is_file():
        records[str(protocol.relative_to(root))] = {
            "sha256": protocol_actual,
            "bytes": protocol.stat().st_size,
        }

    validator = root / "validate_rq2_stage_b.py"
    if not validator.is_file():
        checks["validator_source"] = {"exists": False, "matches": False}
    else:
        records[str(validator.relative_to(root))] = {
            "sha256": sha256(validator),
            "bytes": validator.stat().st_size,
        }
        checks["validator_source"] = {
            "exists": True,
            "actual_sha256": sha256(validator),
            "matches": True,
        }

    failures = {
        name: item for name, item in checks.items() if not item.get("matches", False)
    }
    return checks, {"records": records, "failures": failures}


def main() -> int:
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--preflight-only", action="store_true")
    mode.add_argument("--confirm-freeze", action="store_true")
    args = parser.parse_args()
    root = Path(__file__).resolve().parent
    manifest = root / "analysis_outputs" / "RQ2_STAGE_B_RESULTS_FROZEN.json"
    if manifest.exists():
        raise RuntimeError(f"Results freeze already exists: {manifest}")

    checks, result = audit(root)
    payload = {
        "status": "PASS" if not result["failures"] else "FAIL",
        "action": "preflight_only" if args.preflight_only else "freeze_results",
        "failures": result["failures"],
        "files_checked": len(EXPECTED),
        "all_expected_output_hashes_match": not result["failures"],
        "protocol_manifest_matches": checks["protocol_manifest"]["matches"],
        "research_results_modified": False,
    }
    if result["failures"]:
        print("RQ2 STAGE-B RESULTS FREEZE PREFLIGHT")
        print(json.dumps(payload, indent=2))
        return 1
    if args.preflight_only:
        print("RQ2 STAGE-B RESULTS FREEZE PREFLIGHT")
        print(json.dumps(payload, indent=2))
        return 0

    freeze = {
        "status": "FROZEN",
        "manifest_version": 1,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_validation_status": "PASS",
        "protocol_manifest_sha256": EXPECTED_PROTOCOL_MANIFEST_SHA256,
        "files": result["records"],
        "declarations": {
            "stage_b_execution_complete": True,
            "independent_validation_passed": True,
            "results_are_read_only_inputs_for_reporting": True,
            "rerun_or_overwrite_authorized": False,
            "detector_refitted": False,
            "claim_scope": "controlled perturbation responsiveness; not real-world ground truth",
        },
    }
    atomic_json(freeze, manifest)
    print("RQ2 STAGE-B RESULTS FREEZE")
    print(json.dumps({
        "status": "FROZEN",
        "manifest": str(manifest.relative_to(root)),
        "manifest_sha256": sha256(manifest),
        "files_frozen": len(result["records"]),
        "stage_b_output_files_frozen": len(EXPECTED),
        "rerun_or_overwrite_authorized": False,
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())