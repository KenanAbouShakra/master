from __future__ import annotations

import argparse
import json

import numpy as np
import pandas as pd
from scipy.optimize import minimize

from ci_common import load_config, output_dir, write_json


OUTCOMES=("pr_cycle_hours_median","qualified_review_latency_hours_median","merged_pr_count")


def design(frame,outcome,lag):
    x=frame.sort_values(["repo_full","week"]).copy()
    x["y"]=x.groupby("repo_full")[outcome].shift(-lag) if lag else x[outcome]
    x["exposure"]=x["hmm_filtered_adverse_probability"]
    x["log_attempt_volume"]=np.log1p(x["attempts_total"])
    x["log_churn"]=np.log1p(x["churn_median"])
    x["log_changed_files"]=np.log1p(x["changed_files_median"])
    x["log_commits"]=np.log1p(x["commits_median"])
    if outcome!="merged_pr_count": x["y"]=np.log1p(x["y"])
    x["offset"] = np.log(x["created_pr_count"].clip(lower=1)) if outcome=="merged_pr_count" else 0.0
    dummies=pd.get_dummies(x[["repo_full"]].astype(str),drop_first=True,dtype=float)
    month=pd.get_dummies(x["week"].dt.to_period("M").astype(str),prefix="month",drop_first=True,dtype=float)
    controls=x[["exposure","log_attempt_volume","log_churn","log_changed_files","log_commits"]].astype(float)
    matrix=pd.concat([pd.Series(1.0,index=x.index,name="intercept"),controls,dummies,month],axis=1)
    keep=x["y"].notna() & np.isfinite(matrix).all(axis=1)
    return x.loc[keep],matrix.loc[keep],x.loc[keep,"y"].astype(float),x.loc[keep,"offset"].astype(float)


def ols(matrix,y):
    a=matrix.to_numpy(float); b=y.to_numpy(float); beta=np.linalg.lstsq(a,b,rcond=None)[0]; residual=b-a@beta
    return beta,residual


def poisson(matrix,y,offset):
    a=matrix.to_numpy(float); b=y.to_numpy(float); off=offset.to_numpy(float)
    def objective(beta):
        eta=np.clip(off+a@beta,-20,20); return float(np.sum(np.exp(eta)-b*eta)+1e-8*np.sum(beta[1:]**2))
    result=minimize(objective,np.zeros(a.shape[1]),method="L-BFGS-B")
    if not result.success: raise RuntimeError("Poisson model failed: "+result.message)
    return result.x,b-np.exp(np.clip(off+a@result.x,-20,20))


def block_bootstrap(frame,matrix,y,offset,model,repetitions=999,block=4,seed=20260816):
    rng=np.random.default_rng(seed); estimates=[]; repos=frame["repo_full"].unique()
    for _ in range(repetitions):
        picks=[]
        for repo in repos:
            positions=np.where(frame["repo_full"].to_numpy()==repo)[0]; n=len(positions)
            sampled=[]
            while len(sampled)<n:
                start=int(rng.integers(0,max(n-block+1,1))); sampled.extend(positions[start:start+block])
            picks.extend(sampled[:n])
        try:
            beta = poisson(matrix.iloc[picks],y.iloc[picks],offset.iloc[picks])[0] if model=="poisson" else ols(matrix.iloc[picks],y.iloc[picks])[0]
            estimates.append(beta[matrix.columns.get_loc("exposure")])
        except np.linalg.LinAlgError: pass
    return np.asarray(estimates)


def main():
    parser=argparse.ArgumentParser(); parser.add_argument("--config",default="analysis_config.yaml"); parser.add_argument("--bootstrap-repetitions",type=int,default=999); args=parser.parse_args(); cfg=load_config(args.config); out=output_dir(cfg,args.config)
    if not (out/"RQ1_FROZEN.json").is_file(): raise RuntimeError("RQ1 is not frozen. Run 06_freeze_rq1.py --confirm only after reviewing RQ1 validation.")
    panel=pd.read_csv(out/"hmm_results.csv",parse_dates=["week"]); panel=panel[panel["repo_full"].isin(cfg["study"]["development_repositories"]) & panel["split"].isin(["train","holdout"])].copy()
    rows=[]
    for outcome in OUTCOMES:
        for lag in (0,1):
            frame,matrix,y,offset=design(panel,outcome,lag); model="poisson" if outcome=="merged_pr_count" else "ols_log_outcome"
            beta,residual=poisson(matrix,y,offset) if model=="poisson" else ols(matrix,y); idx=matrix.columns.get_loc("exposure"); boot=block_bootstrap(frame,matrix,y,offset,model,args.bootstrap_repetitions,seed=cfg["hmm"]["random_seed"]+lag)
            total=np.sum((y-y.mean())**2); r2=None if total<=1e-12 or model=="poisson" else float(1-np.sum(residual**2)/total)
            rows.append({"outcome":outcome,"lag_weeks":lag,"model":model,"n":len(y),"exposure_coefficient":float(beta[idx]),"exposure_ratio":float(np.exp(beta[idx])),"bootstrap_ci_low":float(np.quantile(boot,.025)),"bootstrap_ci_high":float(np.quantile(boot,.975)),"bootstrap_repetitions":len(boot),"r_squared":r2})
    result=pd.DataFrame(rows); result.to_csv(out/"rq2_effect_estimates.csv",index=False); write_json(out/"rq2_metadata.json",{"claim":"association, not causation","primary_exposure":"frozen filtered adverse-state probability","results":rows})
    print(result.to_string(index=False)); return 0


if __name__=="__main__": raise SystemExit(main())