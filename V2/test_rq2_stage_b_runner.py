from __future__ import annotations

import gzip
import tempfile
from pathlib import Path

import pandas as pd

from rq2_run_stage_b import (
    AtomicGzipCsvWriter,
    detector_identity,
    summary_frame,
    update_summary,
)


def main() -> None:
    passed = []

    assert detector_identity("mad:union") == ("causal_rolling_mad", "union")
    assert detector_identity("mewma:mewma") == ("mewma", "mewma")
    passed.append("detector identities map to frozen family names")

    with tempfile.TemporaryDirectory() as temporary:
        path = Path(temporary) / "result.csv.gz"
        with AtomicGzipCsvWriter(path, ("a", "b")) as writer:
            writer.writerow({"a": 1, "b": "x"})
            writer.writerow({"a": 2, "b": "y"})
        with gzip.open(path, "rt", encoding="utf-8") as stream:
            content = stream.read()
        assert content == "a,b\n1,x\n2,y\n"
    passed.append("gzip writer is deterministic and readable")

    base = {
        "repo_full": "r", "detector_family": "mewma", "detector_variant": "mewma",
        "scenario_type": "metric_shift", "affected_metrics": "latency_log",
        "dimensionality": 1, "magnitude_level": "medium", "duration_weeks": 2,
        "volume_condition": "observed", "missingness_condition": "none",
    }
    summary = {}
    for detected, delay in ((True, 0), (False, None)):
        update_summary(summary, {
            **base,
            "strict_incremental_episode_detected": detected,
            "strict_incremental_detection_delay_weeks": delay,
            "reference_alarm_burden": 0.1,
            "incremental_alarm_duration_weeks": int(detected),
            "incremental_spillover_weeks": 0,
            "total_operational_episode_detected": detected,
            "reference_alarm_overlap_fraction": 0.0,
            "incremental_boundary_overlap": 0.5 if detected else 0.0,
            "pair_unevaluable_fraction": 0.0,
            "injection_relative_precision": 0.5 if detected else None,
            "injection_relative_false_alarm_rate": 0.0,
            "incremental_detected_within_0w_post": detected,
            "incremental_detected_within_1w_post": detected,
            "incremental_detected_within_2w_post": detected,
            "incremental_detected_within_4w_post": detected,
        })
    table = summary_frame(summary)
    assert len(table) == 1
    assert table.loc[0, "scenario_detector_rows"] == 2
    assert table.loc[0, "mean_strict_incremental_episode_detected"] == 0.5
    assert table.loc[0, "strict_incremental_detection_delay_weeks_valid_observations"] == 1
    passed.append("summary preserves metric-specific valid denominators")

    print("RQ2 STAGE-B RUNNER SYNTHETIC TESTS")
    for item in passed:
        print(f"PASS: {item}")
    print(f"RESULT: PASS ({len(passed)}/{len(passed)})")


if __name__ == "__main__":
    main()