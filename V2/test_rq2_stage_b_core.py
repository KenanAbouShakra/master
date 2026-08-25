from __future__ import annotations

import json

import numpy as np
import pandas as pd

from rq2_stage_b_core import (
    build_stage_b_scenario_grid,
    evaluate_paired_alarms,
    stable_seed,
    valid_injection_starts,
)


def frame() -> pd.DataFrame:
    weeks = pd.date_range("2026-01-05", periods=20, freq="W-MON")
    return pd.DataFrame({
        "repo_full": "external/repo",
        "week": weeks,
        "attempts_total": 100,
        "external_evaluation_eligible": [False] * 5 + [True] * 15,
        "latency_log": np.linspace(0, 0.1, 20),
        "failure_rate": 0.1,
        "rerun_rate": 0.05,
    })


def config() -> dict:
    return {
        "evidence_domain": {
            "repositories": ["external/repo"],
            "calibration_weeks": 5,
        },
        "scenario_design": {
            "base_seed": 999,
            "repetitions_per_injected_cell": 2,
            "no_injection_repetitions_per_repository": 1,
            "latency_relative_shifts": [0.1, 0.25, 0.5],
            "failure_probability_shifts": [0.05, 0.1, 0.2],
            "rerun_probability_shifts": [0.05, 0.1, 0.2],
            "durations_weeks": [1, 2],
            "signal_combinations": [["latency_log"], ["failure_rate", "rerun_rate"]],
            "condition_profiles": [{
                "name": "observed",
                "volume_condition": "observed",
                "missingness_condition": "none",
                "magnitude_levels": ["low", "high"],
            }],
        },
    }


def main() -> None:
    passed = []
    data = frame()

    starts = valid_injection_starts(data, 1, 5)
    assert starts == list(range(5, 20))
    assert valid_injection_starts(data, 2, 5) == list(range(5, 19))
    passed.append("injections use evaluation-eligible weeks after prior history")

    assert stable_seed(1, "a", 2) == stable_seed(1, "a", 2)
    assert stable_seed(1, "a", 2) != stable_seed(2, "a", 2)
    passed.append("stable seeds are reproducible and namespaced")

    grid1 = build_stage_b_scenario_grid(data, config())
    grid2 = build_stage_b_scenario_grid(data, config())
    assert grid1.equals(grid2)
    assert len(grid1) == 1 + 2 * 2 * 2 * 2
    assert grid1["scenario_id"].is_unique and grid1["seed"].is_unique
    passed.append("scenario grid is deterministic with unique IDs and seeds")

    injected = grid1[grid1["scenario_type"].eq("metric_shift")]
    assert set(injected["dimensionality"]) == {1, 2}
    assert set(injected["magnitude_level"]) == {"low", "high"}
    assert all(json.loads(value) for value in injected["magnitude"])
    passed.append("grid preserves dimensionality and magnitude semantics")

    weeks = data["week"]
    eligible = data["external_evaluation_eligible"]
    truth = pd.Series([False] * 8 + [True, True] + [False] * 10)
    reference = pd.Series([False] * 7 + [True] + [False] * 12, dtype="boolean")
    altered = pd.Series([False] * 7 + [True, False, True] + [False] * 10, dtype="boolean")
    evaluation = evaluate_paired_alarms(
        scenario_id="B1", repo_full="external/repo", detector_id="mad:union",
        weeks=weeks, truth=truth, eligible=eligible,
        reference_alarm=reference, injected_alarm=altered,
    )
    assert evaluation.result["strict_incremental_episode_detected"] is True
    assert evaluation.result["strict_incremental_detection_delay_weeks"] == 1
    assert evaluation.result["incremental_alarm_weeks"] == 1
    assert evaluation.result["reference_alarm_weeks"] == 1
    passed.append("pre-existing reference alarms are not attributed to injection")

    spill = altered.copy()
    spill.iloc[11] = True
    spill_eval = evaluate_paired_alarms(
        scenario_id="B2", repo_full="external/repo", detector_id="mewma:mewma",
        weeks=weeks, truth=truth, eligible=eligible,
        reference_alarm=reference, injected_alarm=spill,
    )
    assert spill_eval.result["incremental_spillover_weeks"] == 1
    assert spill_eval.result["incremental_detected_within_2w_post"] is True
    passed.append("post-interval incremental alarms are counted as spillover")

    missing = altered.copy()
    missing.iloc[9] = pd.NA
    missing_eval = evaluate_paired_alarms(
        scenario_id="B3", repo_full="external/repo", detector_id="mad:latency_log",
        weeks=weeks, truth=truth, eligible=eligible,
        reference_alarm=reference, injected_alarm=missing,
    )
    assert missing_eval.result["pair_unevaluable_weeks"] == 1
    assert missing_eval.result["strict_incremental_episode_detected"] is False
    passed.append("missing alarm weeks remain unevaluable rather than negative")

    control_truth = pd.Series(False, index=data.index)
    control_eval = evaluate_paired_alarms(
        scenario_id="B0", repo_full="external/repo", detector_id="mad:failure_rate",
        weeks=weeks, truth=control_truth, eligible=eligible,
        reference_alarm=reference, injected_alarm=reference,
    )
    assert control_eval.result["strict_incremental_episode_detected"] is None
    assert control_eval.result["incremental_alarm_weeks"] == 0
    assert control_eval.result["reference_alarm_burden"] == 1 / 15
    passed.append("no-injection control reports reference burden without false truth")

    assert len(evaluation.week_rows) == 15
    assert all(row["week"] >= "2026-02-09" for row in evaluation.week_rows)
    passed.append("week audit contains evaluation weeks only")

    print("RQ2 STAGE-B CORE SYNTHETIC TESTS")
    for item in passed:
        print(f"PASS: {item}")
    print(f"RESULT: PASS ({len(passed)}/{len(passed)})")


if __name__ == "__main__":
    main()