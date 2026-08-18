from __future__ import annotations

import importlib.util
import pathlib
import traceback
import warnings

import pandas as pd

path = pathlib.Path("01_build_measurement_panels.py")
spec = importlib.util.spec_from_file_location("build_measurements", path)
assert spec is not None and spec.loader is not None
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
attempts = pd.read_csv("analysis_outputs/attempt_measurements.csv.gz", low_memory=False)
attempts["week"] = pd.to_datetime(attempts["week"])
group = attempts[
    attempts["repo_full"].eq("pytest-dev/pytest")
    & pd.to_numeric(attempts["workflow_key"], errors="coerce").eq(22159234)
    & attempts["week"].eq(pd.Timestamp("2025-11-03", tz="UTC"))
].copy()
print("selected rows:", len(group))
warnings.filterwarnings("error", message="Mean of empty slice", category=RuntimeWarning)
print("QUEUE LATENCY TRACEBACK")
try:
    module.aggregate_ci(group, ["repo_full", "workflow_key", "week"])
except RuntimeWarning:
    traceback.print_exc()
group["queue_latency_min"] = group["queue_latency_min"].fillna(0)
print("EXECUTION SPAN TRACEBACK")
try:
    module.aggregate_ci(group, ["repo_full", "workflow_key", "week"])
except RuntimeWarning:
    traceback.print_exc()
