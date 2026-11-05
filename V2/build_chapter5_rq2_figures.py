"""Verify and export a new Chapter 5 RQ2 figure package from frozen outputs.

This reads, but does not rerun or modify, any detector or scenario result. It
validates the frozen Stage-B files, reconstructs plot values from scenario and
paired-week results, checks them against frozen analysis summaries, and writes
to a new output directory that must not already exist.
"""
from __future__ import annotations

import hashlib
import json
import math
import shutil
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd
try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.lines import Line2D
    import seaborn as sns
    HAS_PLOTTING = True
except ImportError:
    HAS_PLOTTING = False

ROOT = Path(__file__).resolve().parent
AO = ROOT / "analysis_outputs"
ANALYSIS = AO / "rq2_stage_b_analysis"
STAGE_B = AO / "rq2_stage_b"
OLD_FIG = AO / "rq2_stage_b_visualizations"
OUT = AO / "rq2_stage_b_visualizations_chapter5_panels_v2"
INPUTS = {
    "scenario_registry": STAGE_B / "rq2_stage_b_scenarios.csv",
    "detector_results": STAGE_B / "rq2_stage_b_detector_results.csv.gz",
    "paired_week_results": STAGE_B / "rq2_stage_b_paired_week_results.csv.gz",
    "operating_profiles": ANALYSIS / "rq2_operating_profiles.csv",
    "magnitude_profiles": ANALYSIS / "rq2_magnitude_profiles.csv",
    "duration_profiles": ANALYSIS / "rq2_duration_profiles.csv",
    "data_quality_contrasts": ANALYSIS / "rq2_data_quality_contrasts.csv",
    "reference_alarm_burden": ANALYSIS / "rq2_reference_alarm_burden.csv",
    "analysis_metadata": ANALYSIS / "rq2_analysis_metadata.json",
    "results_freeze": AO / "RQ2_STAGE_B_RESULTS_FROZEN.json",
    "protocol_freeze": AO / "RQ2_STAGE_B_PROTOCOL_FROZEN.json",
    "old_visualization_manifest": OLD_FIG / "rq2_visualization_manifest.json",
}
DETECTOR_ORDER = ["MEWMA", "MAD latency", "MAD union", "MAD two-of-three", "MAD failure", "MAD rerun"]
DETECTOR_MAP = {
    "mewma:mewma": "MEWMA",
    "mad:latency_log": "MAD latency",
    "mad:failure_rate": "MAD failure",
    "mad:rerun_rate": "MAD rerun",
    "mad:union": "MAD union",
    "mad:two_of_three": "MAD two-of-three",
}
COLOR_HEX = dict(zip(DETECTOR_ORDER, ["#0173b2", "#de8f05", "#029e73", "#d55e00", "#cc78bc", "#ca9161"]))
COLORS = dict(zip(DETECTOR_ORDER, sns.color_palette("colorblind", n_colors=len(DETECTOR_ORDER)))) if HAS_PLOTTING else COLOR_HEX
REPOSITORIES = ["containerd/containerd", "helm/helm", "pytest-dev/pytest"]
PRIMARY_FILTER = {
    "scenario_type": "metric_shift",
    "volume_condition": "observed",
    "missingness_condition": "none",
}


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def rel(path: Path) -> str:
    return path.resolve().relative_to(ROOT.resolve()).as_posix()


def bools(s: pd.Series) -> pd.Series:
    if pd.api.types.is_bool_dtype(s):
        return s.astype("boolean")
    return s.astype("string").str.lower().map({"true": True, "false": False}).astype("boolean")


def records_hash(path: Path, record: dict, name: str) -> None:
    if not path.is_file():
        raise FileNotFoundError(path)
    if "bytes" in record and int(path.stat().st_size) != int(record["bytes"]):
        raise RuntimeError(f"Frozen byte count differs for {name}")
    if sha(path) != record["sha256"]:
        raise RuntimeError(f"Frozen SHA-256 differs for {name}")


def load_verified_sources():
    meta = json.loads(INPUTS["analysis_metadata"].read_text(encoding="utf-8"))
    if meta.get("status") != "PASS":
        raise RuntimeError("RQ2 analysis metadata is not PASS")
    for name, rec in meta["outputs"].items():
        records_hash(ANALYSIS / name, rec, name)
    frozen = json.loads(INPUTS["results_freeze"].read_text(encoding="utf-8"))
    if frozen.get("status") != "FROZEN":
        raise RuntimeError("RQ2 Stage-B result manifest is not FROZEN")
    for name, rec in frozen["files"].items():
        records_hash(ROOT / name, rec, name)
    protocol = json.loads(INPUTS["protocol_freeze"].read_text(encoding="utf-8"))
    if frozen.get("protocol_manifest_sha256") != sha(INPUTS["protocol_freeze"]):
        raise RuntimeError("Frozen RQ2 protocol hash does not match results manifest")
    scenarios = pd.read_csv(INPUTS["scenario_registry"], low_memory=False)
    outcomes = pd.read_csv(INPUTS["detector_results"], compression="gzip", low_memory=False)
    return meta, frozen, protocol, scenarios, outcomes


def is_primary(frame: pd.DataFrame) -> pd.Series:
    return (frame.scenario_type.eq("metric_shift") & frame.volume_condition.eq("observed")
            & frame.missingness_condition.eq("none"))


def aggregate_detection(frame: pd.DataFrame, group_cols: list[str], expected_n: int) -> pd.DataFrame:
    x = frame.copy()
    x["strict_value"] = bools(x["strict_incremental_episode_detected"])
    if x["strict_value"].isna().any():
        raise RuntimeError("Strict detection output missing in a planned figure denominator; do not coerce to non-alarm")
    rows = []
    for key, g in x.groupby(group_cols, sort=True, dropna=False):
        if not isinstance(key, tuple): key = (key,)
        n = len(g)
        if n != expected_n:
            raise RuntimeError(f"Unexpected denominator for {dict(zip(group_cols,key))}: {n} != {expected_n}")
        successes = int(g.strict_value.astype(bool).sum())
        rows.append({**dict(zip(group_cols,key)), "planned_scenarios":n,
                     "outcome_rows_nonmissing":int(g.strict_value.notna().sum()),
                     "strict_detection_scenarios":successes,
                     "detection_proportion":successes/n,
                     "detection_percent":100*successes/n})
    return pd.DataFrame(rows)


def make_data(scenarios: pd.DataFrame, results: pd.DataFrame):
    scenarios = scenarios.copy()
    results = results.copy()
    scenarios["primary"] = is_primary(scenarios)
    scen_primary = scenarios.loc[scenarios.primary].copy()
    if len(scen_primary) != 25200:
        raise RuntimeError(f"Expected 25,200 primary scenarios, found {len(scen_primary)}")
    if not scen_primary.status.eq("evaluable").all():
        raise RuntimeError("A primary scenario has non-evaluable registry status")
    repo_counts = scen_primary.groupby("repo_full").size().to_dict()
    if repo_counts != {repo: 8400 for repo in REPOSITORIES}:
        raise RuntimeError(f"Unexpected primary scenarios per repository: {repo_counts}")
    if scen_primary.groupby("duration_weeks").size().to_dict() != {1:6300,2:6300,4:6300,8:6300}:
        raise RuntimeError("Unexpected primary scenario denominator by duration")
    if scen_primary.groupby("magnitude_level").size().to_dict() != {"high":8400,"low":8400,"medium":8400}:
        raise RuntimeError("Unexpected primary scenario denominator by magnitude")
    medium = scenarios.loc[scenarios.scenario_type.eq("metric_shift") & scenarios.magnitude_level.eq("medium")]
    conditions = {
        "observed": medium.volume_condition.eq("observed") & medium.missingness_condition.eq("none"),
        "controlled_missingness": medium.volume_condition.eq("observed") & medium.missingness_condition.eq("controlled"),
    }
    for label, m in conditions.items():
        if int(m.sum()) != 8400:
            raise RuntimeError(f"Expected 8,400 medium scenarios for {label}; found {int(m.sum())}")

    results["detector"] = results.detector_id.map(DETECTOR_MAP)
    if results.detector.isna().any():
        raise RuntimeError("Unmapped detector ID in frozen results")
    primary_ids = set(scen_primary.scenario_id)
    primary = results.loc[results.scenario_id.isin(primary_ids)].copy()
    if len(primary) != 25200*6:
        raise RuntimeError(f"Expected 151,200 primary scenario-detector rows, found {len(primary)}")
    if primary.groupby("detector").size().to_dict() != {d:25200 for d in DETECTOR_ORDER}:
        raise RuntimeError("Primary scenario denominator differs by detector")
    # Retain and verify all scenario rows; missing detector outcomes are a hard error.
    for col in ("strict_incremental_episode_detected", "total_operational_episode_detected"):
        if bools(primary[col]).isna().any():
            raise RuntimeError(f"Missing {col} in primary planned scenarios")

    overall = aggregate_detection(primary, ["detector"], 25200)
    repo = aggregate_detection(primary, ["repo_full","detector"], 8400)
    duration = aggregate_detection(primary, ["detector","duration_weeks"], 6300)
    magnitude = aggregate_detection(primary, ["detector","magnitude_level"], 8400)
    return scenarios, primary, overall, repo, duration, magnitude, medium, conditions


def paired_week_stats(primary_ids: set[str]) -> pd.DataFrame:
    path = INPUTS["paired_week_results"]
    chunks = []
    use = ["scenario_id","detector_id","pair_evaluable","reference_alarm"]
    for c in pd.read_csv(path, compression="gzip", usecols=use, chunksize=1_000_000, low_memory=False):
        c = c.loc[c.scenario_id.isin(primary_ids)].copy()
        if c.empty: continue
        c["detector"] = c.detector_id.map(DETECTOR_MAP)
        c["pair_eval"] = bools(c.pair_evaluable)
        c["ref_alarm"] = bools(c.reference_alarm)
        if c.pair_eval.isna().any():
            raise RuntimeError("pair_evaluable missing in paired-week results")
        c["row_n"] = 1
        c["eval_n"] = c.pair_eval.astype(int)
        c["ref_n"] = (c.pair_eval & c.ref_alarm.fillna(False)).astype(int)
        chunks.append(c.groupby(["scenario_id","detector"],as_index=False).agg(
            total_weeks=("row_n","sum"), pair_evaluable_weeks=("eval_n","sum"), reference_alarm_weeks=("ref_n","sum")))
    grouped = pd.concat(chunks, ignore_index=True).groupby(["scenario_id","detector"],as_index=False).sum(numeric_only=True)
    if len(grouped) != 25200*6:
        raise RuntimeError(f"Expected one paired-week summary per primary scenario/detector; got {len(grouped)}")
    if not grouped.total_weeks.eq(46).all():
        raise RuntimeError("Each primary scenario/detector must cover 46 external evaluation weeks")
    grouped["reference_alarm_burden_scenario"] = np.where(
        grouped.pair_evaluable_weeks.gt(0), grouped.reference_alarm_weeks/grouped.pair_evaluable_weeks, np.nan)
    grouped["pair_unavailability_scenario"] = 1-grouped.pair_evaluable_weeks/grouped.total_weeks
    return grouped


def make_tradeoff(primary: pd.DataFrame, paired: pd.DataFrame, operating: pd.DataFrame) -> pd.DataFrame:
    det = primary.copy()
    det["strict_value"] = bools(det.strict_incremental_episode_detected)
    det["burden_value"] = pd.to_numeric(det.reference_alarm_burden,errors="coerce")
    merged = det[["scenario_id","detector","strict_value","burden_value","pair_unevaluable_fraction"]].merge(
        paired, on=["scenario_id","detector"], validate="one_to_one")
    if not np.allclose(merged.pair_unevaluable_fraction, merged.pair_unavailability_scenario, atol=1e-12, equal_nan=True):
        raise RuntimeError("Paired-week unavailability reconstruction differs from detector results")
    # Directly recompute reference alarm burden using alarm weeks / pair-evaluable weeks
    # within each scenario, then average scenario-level burdens as the frozen profile does.
    if not np.allclose(merged.burden_value, merged.reference_alarm_burden_scenario, atol=1e-12, equal_nan=True):
        raise RuntimeError("Reference alarm burden does not match pair-evaluable-week calculation")
    rows=[]
    for detector,g in merged.groupby("detector",sort=False):
        outcome=bools(g.strict_value)
        burden=g.reference_alarm_burden_scenario.dropna()
        unavail=g.pair_unavailability_scenario
        # verify this scenario-level aggregation against all 252 primary operating cells
        op=operating.loc[operating.detector.eq(detector)]
        if len(op) != 252:
            raise RuntimeError(f"Expected 252 primary design cells for {detector}, found {len(op)}")
        op_burden=pd.to_numeric(op.reference_alarm_burden_mean,errors="coerce")
        op_n=pd.to_numeric(op.reference_alarm_burden_n,errors="coerce")
        op_burden_mean=float(np.average(op_burden[op_burden.notna()],weights=op_n[op_burden.notna()]))
        if not np.isclose(float(burden.mean()),op_burden_mean,atol=1e-12):
            raise RuntimeError(f"Scenario burden mean does not reconcile to stored operating profiles: {detector}")
        rows.append({
            "detector":detector,"primary_condition":"observed volume; no additional missingness",
            "planned_scenarios":len(g),"strict_detection_scenarios":int(outcome.astype(bool).sum()),
            "detection_percent":float(outcome.mean()*100),
            "reference_alarm_scenarios_with_defined_burden":int(burden.notna().sum()),
            "pair_evaluable_reference_alarm_weeks":int(g.reference_alarm_weeks.sum()),
            "pair_evaluable_weeks":int(g.pair_evaluable_weeks.sum()),
            "reference_alarm_burden_scenario_mean":float(burden.mean()),
            "pooled_reference_alarm_week_ratio":float(g.reference_alarm_weeks.sum()/g.pair_evaluable_weeks.sum()),
            "mean_pair_week_unavailability":float(unavail.mean()),
            "mean_pair_week_unavailability_percent":float(unavail.mean()*100),
            "marker_area_pt2":float(90+500*unavail.mean()),
            "marker_area_formula":"90 + 500 * mean_pair_week_unavailability; minimum visible area is 90 pt^2 at 0%",
        })
    return pd.DataFrame(rows).set_index("detector").reindex(DETECTOR_ORDER).reset_index()


def make_missingness(medium: pd.DataFrame, results: pd.DataFrame) -> pd.DataFrame:
    ids=set(medium.scenario_id)
    x=results.loc[results.scenario_id.isin(ids)].copy()
    x["detector"]=x.detector_id.map(DETECTOR_MAP)
    x["strict_value"]=bools(x.strict_incremental_episode_detected)
    x=x.loc[x.magnitude_level.eq("medium") & x.volume_condition.eq("observed")
            & x.missingness_condition.isin(["none","controlled"])].copy()
    if x.strict_value.isna().any():
        raise RuntimeError("Missing strict-detection outcome in medium missingness comparison")
    keys=["repo_full","detector","affected_metrics","duration_weeks"]
    rows=[]
    for key,g in x.groupby(keys,sort=True):
        condition_rows={}
        for condition in ["none","controlled"]:
            q=g.loc[g.missingness_condition.eq(condition)]
            n=len(q)
            if n!=100: raise RuntimeError(f"Expected 100 rows per design cell condition, got {n}: {key}, {condition}")
            condition_rows[condition]=(int(q.strict_value.astype(bool).sum()),n)
        obs_n, obs_d=condition_rows["none"]
        miss_n, miss_d=condition_rows["controlled"]
        diff=miss_n/miss_d-obs_n/obs_d
        rows.append({"repo_full":key[0],"detector":key[1],"affected_metrics":key[2],"duration_weeks":key[3],
                     "magnitude_level":"medium","observed_detection_scenarios":obs_n,"observed_planned_scenarios":obs_d,
                     "controlled_missingness_detection_scenarios":miss_n,"controlled_missingness_planned_scenarios":miss_d,
                     "difference_controlled_minus_observed_proportion":diff,
                     "difference_percentage_points":100*diff,"injection_locations_independently_seeded":True})
    cells=pd.DataFrame(rows)
    if len(cells)!=6*84: raise RuntimeError(f"Expected 504 medium design-cell contrasts, found {len(cells)}")
    if not cells.groupby("detector").size().eq(84).all(): raise RuntimeError("Expected 84 contrast cells per detector")
    summaries=[]
    for detector,g in cells.groupby("detector",sort=False):
        # Every cell has 100 planned scenarios per condition; equal-cell mean is
        # therefore also the equally weighted mean of the precomputed cell effects.
        summaries.append({"row_type":"detector_summary","repository":"ALL",
            "detector":detector,"affected_metrics":"ALL","duration_weeks":"ALL","magnitude_level":"medium",
            "observed_detection_scenarios":int(g.observed_detection_scenarios.sum()),"observed_planned_scenarios":int(g.observed_planned_scenarios.sum()),
            "controlled_missingness_detection_scenarios":int(g.controlled_missingness_detection_scenarios.sum()),
            "controlled_missingness_planned_scenarios":int(g.controlled_missingness_planned_scenarios.sum()),
            "difference_controlled_minus_observed_proportion":float(g.difference_controlled_minus_observed_proportion.mean()),
            "difference_percentage_points":float(g.difference_percentage_points.mean()),
            "design_cells":len(g),"injection_locations_independently_seeded":True})
    cell_out=cells.copy(); cell_out.insert(0,"row_type","design_cell")
    cell_out["design_cells"]=np.nan
    return pd.concat([cell_out,pd.DataFrame(summaries)],ignore_index=True)


def set_style():
    if not HAS_PLOTTING: return
    sns.set_theme(context="paper",style="whitegrid",font_scale=1.05)
    plt.rcParams.update({"font.family":"DejaVu Sans","font.size":9,"axes.titlesize":13,
        "axes.labelsize":10,"legend.fontsize":8,"pdf.fonttype":42,"ps.fonttype":42})


def save(fig,stem):
    fig.savefig(OUT/f"{stem}.pdf",bbox_inches="tight",facecolor="white")
    fig.savefig(OUT/f"{stem}.png",dpi=320,bbox_inches="tight",facecolor="white")
    plt.close(fig)


def plot_repository(data):
    fig,ax=plt.subplots(figsize=(13.2,6.2))
    x=np.arange(len(REPOSITORIES)); width=.12
    for i,d in enumerate(DETECTOR_ORDER):
        q=data.loc[data.detector.eq(d)].set_index("repo_full").reindex(REPOSITORIES)
        ax.bar(x+(i-2.5)*width,q.detection_percent,width,color=COLORS[d],label=d)
    ax.set_xticks(x,REPOSITORIES,rotation=14,ha="right")
    ax.set_ylim(0,100); ax.set_ylabel("Strict incremental detection (%)")
    ax.set_title("Detection by Repository",loc="left",weight="bold")
    ax.text(0,1.01,"Primary condition: observed volume, no additional missingness; 8,400 planned scenarios per repository and detector.",
            transform=ax.transAxes,fontsize=8.5,va="bottom")
    ax.grid(axis="y",alpha=.24); ax.grid(axis="x",visible=False)
    ax.legend(frameon=False,ncol=3,loc="upper left",bbox_to_anchor=(0,-.24))
    sns.despine(ax=ax); fig.subplots_adjust(bottom=.28,top=.83,left=.09,right=.99)
    save(fig,"figure_rq2_4_detection_by_repository")


def plot_duration(data):
    fig,ax=plt.subplots(figsize=(9.3,6.0))
    for d in DETECTOR_ORDER:
        q=data.loc[data.detector.eq(d)].sort_values("duration_weeks")
        ax.plot(q.duration_weeks,q.detection_percent,color=COLORS[d],marker="o",linewidth=1.8,markersize=5,label=d)
    ax.set_xticks([1,2,4,8],["1","2","4","8"]); ax.set_xlim(.5,8.5); ax.set_ylim(0,100)
    ax.set_xlabel("Injected deviation duration (weeks)"); ax.set_ylabel("Strict incremental detection (%)")
    ax.set_title("Detection by Injected Deviation Duration",loc="left",weight="bold")
    ax.text(0,1.01,"Primary condition; 6,300 planned scenarios per duration and detector.",transform=ax.transAxes,fontsize=8.5,va="bottom")
    ax.text(0,-.19,"Lines connect the four tested durations only; intermediate durations were not evaluated.",transform=ax.transAxes,fontsize=8)
    ax.grid(alpha=.24); ax.legend(frameon=False,ncol=2,loc="upper left",bbox_to_anchor=(1.01,1))
    sns.despine(ax=ax); fig.subplots_adjust(left=.10,right=.78,top=.84,bottom=.18)
    save(fig,"figure_rq2_2_detection_by_duration")


def plot_magnitude(data):
    order=["low","medium","high"]
    fig,ax=plt.subplots(figsize=(9.4,6.0))
    for d in DETECTOR_ORDER:
        q=data.loc[data.detector.eq(d)].set_index("magnitude_level").reindex(order)
        ax.plot(np.arange(3),q.detection_percent,color=COLORS[d],marker="o",linewidth=1.8,markersize=5,label=d)
    ax.set_xticks([0,1,2],["Low","Medium","High"]); ax.set_xlim(-.15,2.15); ax.set_ylim(0,100)
    ax.set_xlabel("Measurement-specific injection setting"); ax.set_ylabel("Strict incremental detection (%)")
    ax.set_title("Detection by Injected Deviation Magnitude",loc="left",weight="bold")
    ax.text(0,1.01,"Primary condition; 8,400 planned scenarios per magnitude and detector.",transform=ax.transAxes,fontsize=8.5,va="bottom")
    ax.text(0,-.19,"Levels are measurement-specific settings, not incident-severity classes or equal standardized perturbations.",
            transform=ax.transAxes,fontsize=8)
    ax.grid(alpha=.24); ax.legend(frameon=False,ncol=2,loc="upper left",bbox_to_anchor=(1.01,1))
    sns.despine(ax=ax); fig.subplots_adjust(left=.10,right=.78,top=.84,bottom=.18)
    save(fig,"figure_rq2_3_detection_by_magnitude")


def plot_missingness(data):
    q=data.loc[data.row_type.eq("detector_summary")].set_index("detector").reindex(DETECTOR_ORDER).reset_index()
    fig,ax=plt.subplots(figsize=(9.4,5.8))
    y=np.arange(len(q)); vals=q.difference_percentage_points.to_numpy()
    ax.barh(y,vals,color=[COLORS[d] for d in q.detector],height=.64)
    ax.set_yticks(y,q.detector); ax.invert_yaxis(); ax.axvline(0,color="#333333",linewidth=.8)
    ax.set_xlabel("Controlled-missingness detection − observed detection (percentage points)")
    ax.set_title("Detection Difference Under Controlled Missingness",loc="left",weight="bold")
    ax.text(0,1.01,"Medium magnitude only; 8,400 planned scenarios per condition and detector.",transform=ax.transAxes,fontsize=8.5,va="bottom")
    ax.text(0,-.17,"Mean of 84 design-cell contrasts; injection locations were independently seeded. Descriptive contrast, not paired or causal.",
            transform=ax.transAxes,fontsize=8)
    ax.text(0,-.23,"A smaller reduction is not by itself evidence of greater robustness; consider the observed-condition rate.",
            transform=ax.transAxes,fontsize=8)
    for yy,val in zip(y,vals):
        ax.annotate(f"{val:+.1f}",(val,yy),xytext=(5 if val>=0 else -5,0),textcoords="offset points",
                    va="center",ha="left" if val>=0 else "right",fontsize=8)
    ax.grid(axis="x",alpha=.24); ax.grid(axis="y",visible=False)
    sns.despine(ax=ax); fig.subplots_adjust(left=.22,right=.97,top=.84,bottom=.25)
    save(fig,"figure_rq2_5_controlled_missingness_contrast")


def plot_tradeoff(data):
    required = [
        "detector",
        "planned_scenarios",
        "detection_percent",
        "reference_alarm_burden_scenario_mean",
        "mean_pair_week_unavailability_percent",
    ]

    missing = set(required) - set(data.columns)
    if missing:
        raise ValueError(f"Missing plotting columns: {sorted(missing)}")

    if data["detector"].duplicated().any():
        raise ValueError("Duplicate detector rows in trade-off data")

    if set(data["detector"]) != set(DETECTOR_ORDER):
        raise ValueError("Unexpected detector set in trade-off data")

    frame = data.set_index("detector").reindex(DETECTOR_ORDER)

    if not frame["planned_scenarios"].eq(25200).all():
        raise ValueError("Expected 25,200 primary scenarios per detector")

    panels = [
        (
            "Strict incremental\ndetection (%)",
            frame["detection_percent"].to_numpy(dtype=float),
        ),
        (
            "Mean reference\nalarm burden (%)",
            100 * frame[
                "reference_alarm_burden_scenario_mean"
            ].to_numpy(dtype=float),
        ),
        (
            "Mean paired-week\nunavailability (%)",
            frame[
                "mean_pair_week_unavailability_percent"
            ].to_numpy(dtype=float),
        ),
    ]

    for title, values in panels:
        if not np.isfinite(values).all():
            raise ValueError(f"Undefined plotting values: {title}")
        if ((values < 0) | (values > 100)).any():
            raise ValueError(f"Invalid percentage values: {title}")
        if (values > 60).any():
            raise ValueError(
                f"Values exceed the specified 0–60% display range: {title}"
            )

    fig, axes = plt.subplots(
        1, 3,
        figsize=(11, 4.5),
        sharey=True,
    )

    positions = np.arange(len(DETECTOR_ORDER))
    colours = [COLORS[name] for name in DETECTOR_ORDER]

    for ax, (title, values) in zip(axes, panels):
        ax.barh(
            positions,
            values,
            height=0.55,
            color=colours,
            zorder=3,
        )

        ax.set_title(title, fontsize=11, pad=14)
        ax.set_xlim(0, 60)
        ax.set_xticks([0, 20, 40, 60])
        ax.set_axisbelow(True)
        ax.grid(axis="x", color="#e5e7eb", linewidth=0.6)
        ax.grid(axis="y", visible=False)
        ax.tick_params(axis="y", length=0)

        for position, value in zip(positions, values):
            ax.annotate(
                f"{value:.2f}",
                xy=(value, position),
                xytext=(4, 0),
                textcoords="offset points",
                va="center",
                fontsize=10,
            )

        for side in ["top", "right", "left"]:
            ax.spines[side].set_visible(False)

        ax.spines["bottom"].set_color("#a0a0a0")

    axes[0].set_yticks(positions)
    axes[0].set_yticklabels(DETECTOR_ORDER, fontsize=11)
    axes[0].invert_yaxis()

    fig.subplots_adjust(
        left=0.18,
        right=0.98,
        top=0.82,
        bottom=0.13,
        wspace=0.25,
    )

    save(fig, "figure_rq2_1_detection_reference_alarm_burden")
    plt.close(fig)


def caption_text():
    return """# Chapter 5 RQ2 figure captions

1. **Detection by Repository (Section 5.4).** Strict incremental detection in the primary observed-volume, no-additional-missingness condition (8,400 planned scenarios per repository and detector). These three external repositories do not establish causes of between-repository differences or represent every project type.
2. **Detection by Injected Deviation Duration (Section 5.5).** Strict incremental detection across the four tested durations (1, 2, 4, and 8 weeks) in the primary condition (6,300 planned scenarios per duration and detector). Lines connect tested durations only; no result is implied for intermediate durations.
3. **Detection by Injected Deviation Magnitude (Section 5.5).** Strict incremental detection across Low, Medium, and High measurement-specific injection settings in the primary condition (8,400 planned scenarios per magnitude and detector). The levels are not incident-severity classes or equal standardized perturbations across measurements.
4. **Detection Difference Under Controlled Missingness (Section 5.7).** Medium-magnitude controlled-missingness detection minus observed-condition detection, in percentage points (8,400 planned scenarios per condition and detector). The plotted mean summarizes 84 design-cell contrasts; injection locations were independently seeded, so this is descriptive rather than a paired or causal missingness effect. Interpret alongside observed-condition detection.
5. **Detection and Reference Alarm Burden (Section 5.8).** Strict incremental detection and scenario-mean reference alarm burden in the primary condition (25,200 planned scenarios per detector). Reference burden is calculated on pair-evaluable weeks and is not a verified false-positive rate. Marker size maps mean paired-week unavailability to area as 90 + 500 × proportion pt², with a 90 pt² visible minimum at zero; size is not uncertainty. MEWMA is the implemented positive-state score without covariance adjustment.

All estimates are point estimates over the specified simulation grid. No uncertainty intervals are shown: repetitions are nested in only three repositories and nominal scenario-level intervals would not express project-population uncertainty. Detection denominators retain all planned scenarios; missing detector outcomes are never recoded as non-detections.
"""


def main():
    if OUT.exists():
        raise FileExistsError(f"Refusing to overwrite existing figure package directory: {OUT}")
    meta,frozen,protocol,scenarios,results=load_verified_sources()
    scenarios,primary,overall,repo,duration,magnitude,medium,conditions=make_data(scenarios,results)
    pair=paired_week_stats(set(scenarios.loc[is_primary(scenarios),"scenario_id"]))
    operating=pd.read_csv(ANALYSIS/"rq2_operating_profiles.csv",low_memory=False)
    operating["detector"]=operating.detector_family.astype(str).map({
        "mewma":"MEWMA","causal_rolling_mad":"MAD"})
    # Map variants by frozen detector IDs to avoid ambiguity.
    op_map={"mewma:mewma":"MEWMA","causal_rolling_mad:latency_log":"MAD latency",
            "causal_rolling_mad:failure_rate":"MAD failure","causal_rolling_mad:rerun_rate":"MAD rerun",
            "causal_rolling_mad:union":"MAD union","causal_rolling_mad:two_of_three":"MAD two-of-three"}
    operating["detector"]=operating.apply(lambda r:op_map.get(f"{r.detector_family}:{r.detector_variant}"),axis=1)
    operating=operating.loc[operating.scenario_type.eq("metric_shift")&operating.volume_condition.eq("observed")&operating.missingness_condition.eq("none")].copy()
    operating=operating.loc[operating.duration_weeks.isin([1,2,4,8])]
    tradeoff=make_tradeoff(primary,pair,operating)
    missingness=make_missingness(medium,results)

    # Cross-check reconstructed detection outputs against frozen analysis tables.
    def reconcile(profile_name, reconstructed, keycols):
        prof=pd.read_csv(ANALYSIS/profile_name,low_memory=False)
        prof["detector"]=prof.apply(lambda r:op_map.get(f"{r.detector_family}:{r.detector_variant}"),axis=1)
        if {"scenario_type","volume_condition","missingness_condition"}.issubset(prof.columns):
            prof=prof.loc[prof.scenario_type.eq("metric_shift")&prof.volume_condition.eq("observed")&prof.missingness_condition.eq("none")].copy()
        if "duration_weeks" in keycols: prof=prof.loc[prof.duration_weeks.isin([1,2,4,8])]
        if "magnitude_level" in keycols: prof=prof.loc[prof.magnitude_level.isin(["low","medium","high"])]
        grouped=prof.groupby(keycols,as_index=False,sort=True).agg(
            scenario_detector_rows=("scenario_detector_rows","sum"),
            strict_incremental_episode_detected_proportion=("strict_incremental_episode_detected_proportion",
                lambda s: float((s * prof.loc[s.index,"scenario_detector_rows"]).sum()) /
                    float(prof.loc[s.index,"scenario_detector_rows"].sum())))
        grouped["strict_detection_successes_reconstructed"] = (
            grouped.strict_incremental_episode_detected_proportion * grouped.scenario_detector_rows).round().astype(int)
        left=reconstructed.set_index(keycols).sort_index()
        right=grouped.set_index(keycols).sort_index()
        if len(left)!=len(right): raise RuntimeError(f"Profile row count differs for {profile_name}")
        if not np.array_equal(left.planned_scenarios.to_numpy(),right.scenario_detector_rows.to_numpy()):
            raise RuntimeError(f"Scenario denominators differ from frozen {profile_name}")
        if not np.array_equal(left.strict_detection_scenarios.to_numpy(),right.strict_detection_successes_reconstructed.to_numpy()):
            raise RuntimeError(f"Detection numerators differ from frozen {profile_name}")
    reconcile("rq2_operating_profiles.csv",overall,["detector"])
    reconcile("rq2_operating_profiles.csv",repo,["detector","repo_full"])
    reconcile("rq2_duration_profiles.csv",duration,["detector","duration_weeks"])
    reconcile("rq2_magnitude_profiles.csv",magnitude,["detector","magnitude_level"])

    OUT.mkdir(parents=False,exist_ok=False)
    out_tables={
        "figure_data_repository.csv":repo,
        "figure_data_duration.csv":duration,
        "figure_data_magnitude.csv":magnitude,
        "figure_data_controlled_missingness.csv":missingness,
        "figure_data_operating_tradeoff.csv":tradeoff,
    }
    for name,frame in out_tables.items(): frame.to_csv(OUT/name,index=False,lineterminator="\n")
    config={
      "primary_condition":PRIMARY_FILTER,"denominators":{"overall_per_detector":25200,
        "repository_per_detector":8400,"duration_per_detector":6300,"magnitude_per_detector":8400,
        "medium_missingness_per_condition_detector":8400,"missingness_design_cells_per_detector":84},
      "detector_order":DETECTOR_ORDER,"colors_hex":{d:("#%02x%02x%02x"%tuple(round(255*c) for c in COLORS[d]) if HAS_PLOTTING else COLORS[d]) for d in DETECTOR_ORDER},
      "uncertainty":"point estimates only; scenario-level intervals omitted because scenarios are nested in three repositories and do not quantify project-population uncertainty",
      "detection_estimator":"number of true strict_incremental_episode_detected scenario outcomes / all planned scenarios; missing outputs cause a hard error and are not converted to false",
      "reference_alarm_burden":"for each scenario, sum(reference_alarm on pair-evaluable weeks) / pair-evaluable weeks; average defined scenario ratios equally across planned primary scenarios",
      "pair_week_unavailability":"mean over scenarios of pair_unevaluable_weeks / all 46 external-evaluation weeks",
      "missingness_estimator":"within each of 84 repo × affected-metric-combination × duration design cells, controlled-medium detection proportion minus observed-medium proportion; mean equally over cells; profiles independently seeded",
      "marker_area":"90 + 500 * mean pair-week unavailability proportion, in points squared; 90 pt^2 visible minimum at zero",
      "me wma_label":"implemented positive-state MEWMA score without covariance adjustment; no recalibration or refitting",
      "notes_found":[],"no_detector_rerun":True,
    }
    # avoid typo-like field naming in a machine-readable config
    config["mewma_label"]=config.pop("me wma_label")
    (OUT/"figure_config.json").write_text(json.dumps(config,indent=2),encoding="utf-8")
    captions=caption_text(); (OUT/"captions.md").write_text(captions,encoding="utf-8")
    if HAS_PLOTTING:
        set_style()
        plot_repository(repo); plot_duration(duration); plot_magnitude(magnitude); plot_missingness(missingness); plot_tradeoff(tradeoff)

    # Compare the current old-export files to their current manifest without relabeling them.
    old_manifest=json.loads(INPUTS["old_visualization_manifest"].read_text(encoding="utf-8"))
    old_audit=[]
    for name,rec in old_manifest.get("files",{}).items():
        path=OLD_FIG/name
        actual=sha(path) if path.is_file() else None
        old_audit.append({"file":name,"exists":path.is_file(),"actual_sha256":actual,
                          "manifest_sha256":rec.get("sha256"),"matches_current_manifest":bool(actual and actual==rec.get("sha256"))})
    pd.DataFrame(old_audit).to_csv(OUT/"existing_export_hash_audit.csv",index=False)
    report=f'''# RQ2 Chapter 5 figure verification report

Audit/export date: 2026-10-23. No detector was rerun; no scenario, calibration, parameter, or stored result was altered.

## Exact source versions inspected

- Requested `Master_CI_Monitoring_Study_Note(20261022-212912)` and `MEWMA_Checked_Methods_Note_EN`: not present in the repository or supplied attachment folder (the only supplied attachment was the request text). No legacy PR/release/CD/flakiness files or Draft(7) outputs were used.
- Current source result set: `V2/analysis_outputs/rq2_stage_b/` with `RQ2_STAGE_B_RESULTS_FROZEN.json` and `RQ2_STAGE_B_PROTOCOL_FROZEN.json`; all result-manifest files were verified by byte size and SHA-256. Current summaries are `V2/analysis_outputs/rq2_stage_b_analysis/` and all seven analysis CSVs match `rq2_analysis_metadata.json`.
- The available visualization manifest is `V2/analysis_outputs/rq2_stage_b_visualizations/rq2_visualization_manifest.json` (SHA-256 `{sha(INPUTS['old_visualization_manifest'])}`). The specifically cited `rq2_visualization_manifest(1).json` was not supplied. In this checkout all listed old PNG/PDF/data files match the current repository manifest; the external PNG copy described in the request cannot be compared because it is absent.
- Plot source is this package's `build_chapter5_rq2_figures.py`; the matching current analytical sources are frozen Stage-B scenario/detector/paired-week outputs. The methods notes are unavailable, so the requested positive-state, no-covariance MEWMA wording is retained as specified in the request and is consistent with the implementation code's positive-part squared statistic.

## Numerical checks

The scenario registry has 25,200 primary observed-volume/no-additional-missingness scenarios: 8,400 per repository. It yields 6,300 per duration and 8,400 per magnitude. Medium observed and controlled-missingness profiles each contain 8,400 scenarios per detector. Raw detector outcomes contain 25,200 rows per detector. Every primary strict-detection outcome is present; if one were missing, export would stop rather than treat it as a non-detection. Reconstructed overall, repository, duration, and magnitude numerators/denominators match the frozen analysis tables exactly.

Reference burden was independently rebuilt from the paired-week file. For each scenario it is reference alarm weeks divided by pair-evaluable weeks, then averaged over scenarios where that ratio is defined. Reconstructed scenario burdens match the stored scenario-level result and primary operating-profile aggregation. Pair-week unavailability was rebuilt from `pair_evaluable` over the 46 evaluation weeks for each scenario-detector pair and matches the stored scenario field. The `pooled_reference_alarm_week_ratio` is included as an audit diagnostic; the plotted reference burden follows the frozen mean-of-scenario-ratios aggregation.

Controlled-missingness contrasts are computed within 84 repository × affected-metric-combination × duration cells per detector at medium magnitude, then averaged equally across cells. Both conditions contain 100 repetitions in each cell (8,400 total each); injection locations were independently seeded.

## Presentation and uncertainty

{('Five revised PDFs are vector exports; PNG companions are exported at 320 dpi. Titles, axes, repository labels, detector colours, numerical duration spacing, medium-only missingness contrast and operating-marker legend follow the request. MAD failure and MAD rerun retain their exact data coordinates; separate marker shapes and offset labels make them distinguishable. Marker area uses `90 + 500 x mean unavailability proportion` pt^2, so zero unavailability remains visible at 90 pt^2. No uncertainty bands or error bars are drawn.') if HAS_PLOTTING else 'Figure rendering was unavailable: matplotlib/seaborn are not installed, and the required matplotlib wheel is not cached for offline use. No figure files were generated. CSV tables and the hash audit are included; run this script in an environment satisfying V2/requirements-lock.txt to render.'} The source analysis has nominal scenario-level intervals, but they do not quantify variation across project populations; repeated scenarios are nested in three repositories. The new figures therefore show point estimates only.

## Limits

The user-described uploaded PNG files and the named `(1)` manifest were not present, so their claimed mismatch cannot be specifically reconciled. Current repository exports match the current manifest. The new manifest records hashes for the actual inputs and all new outputs and script; it does not overwrite or relabel the previous manifest. The two requested methodological notes are also unavailable. No inference is made about applicability beyond the three external repositories.
'''
    (OUT/"verification_report.md").write_text(report,encoding="utf-8")

    # Manifest lists every used frozen input plus the plotting code, config,
    # caption/report, exact data tables, and generated vector/raster figures.
    output_files=sorted([p for p in OUT.iterdir() if p.is_file() and p.name!="figure_manifest.json"])
    manifest={"status":"PASS","created_on":"2026-10-23","purpose":"Chapter 5 RQ2 figures; point estimates only",
        "frozen_results_manifest_sha256":sha(INPUTS["results_freeze"]),
        "protocol_manifest_sha256":sha(INPUTS["protocol_freeze"]),
        "analysis_metadata_sha256":sha(INPUTS["analysis_metadata"]),
        "plot_script_path":rel(Path(__file__)),"plot_script_sha256":sha(Path(__file__)),
        "inputs":{k:{"path":rel(p),"bytes":p.stat().st_size,"sha256":sha(p)} for k,p in INPUTS.items()},
        "outputs":{p.name:{"bytes":p.stat().st_size,"sha256":sha(p)} for p in output_files},
        "verification":{"primary_scenarios":25200,"scenario_outcomes_missing":0,"figure_rendering_available":HAS_PLOTTING,
            "denominators_checked":{"repo":8400,"duration":6300,"magnitude":8400,"medium_missingness_each_condition":8400},
            "rq2_analysis_csvs_verified":len(meta["outputs"]),"no_detector_rerun":True},
        "uncertainty":"not plotted; only point estimates", "mewma_interpretation":"positive-state score without covariance adjustment"}
    (OUT/"figure_manifest.json").write_text(json.dumps(manifest,indent=2,sort_keys=True),encoding="utf-8")
    zip_path = AO / "RQ2_Chapter5_Verified_Figures_Panels_v2.zip"
    with zipfile.ZipFile(zip_path,"w",zipfile.ZIP_DEFLATED) as z:
        for p in sorted(OUT.iterdir()):
            if p.is_file(): z.write(p,arcname=f"rq2_stage_b_visualizations_chapter5_panels_v2/{p.name}")
        z.write(Path(__file__),arcname="rq2_stage_b_visualizations_chapter5_panels_v2/build_chapter5_rq2_figures.py")
    print(json.dumps({"output_directory":str(OUT),"zip":str(zip_path),
        "figures_exported":5 if HAS_PLOTTING else 0,"scenario_outcomes_missing":0,"input_manifest_hash":manifest["frozen_results_manifest_sha256"],
        "outputs":len(manifest["outputs"])},indent=2))


if __name__=="__main__":
    main()
