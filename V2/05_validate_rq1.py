from __future__ import annotations

import argparse
import json

import numpy as np
import pandas as pd

from ci_common import CORE_METRICS, load_config, output_dir, write_json
from importlib import import_module


hmm = import_module("04_fit_hmm")


def metrics(truth, predicted):
    truth=np.asarray(truth,bool); predicted=np.asarray(predicted,bool)
    tp=int(np.sum(truth & predicted)); fp=int(np.sum(~truth & predicted)); fn=int(np.sum(truth & ~predicted)); tn=int(np.sum(~truth & ~predicted))
    return {"tp":tp,"fp":fp,"fn":fn,"tn":tn,"precision":tp/max(tp+fp,1),"recall":tp/max(tp+fn,1),"false_alarm_rate":fp/max(fp+tn,1)}


def delay(truth,predicted):
    starts=np.where(truth & ~np.r_[False,truth[:-1]])[0]; values=[]
    for start in starts:
        end=start
        while end+1<len(truth) and truth[end+1]: end+=1
        found=np.where(predicted[start:end+1])[0]
        values.append(int(found[0]) if len(found) else None)
    valid=[x for x in values if x is not None]
    return float(np.mean(valid)) if valid else None


def synthetic(panel,cfg,model,adverse):
    rng=np.random.default_rng(cfg["synthetic_validation"]["random_seed"]); rows=[]
    development=cfg["study"]["development_repositories"]
    for repo in development:
        base=panel[(panel.repo_full==repo)&(panel.split=="train")].copy()
        x=base[[f"z_{m}" for m in CORE_METRICS]].to_numpy(float)
        if len(x)<35: continue
        for duration in cfg["synthetic_validation"]["durations_weeks"]:
            for magnitude in (0.5,1.0,2.0):
                for rep in range(cfg["synthetic_validation"]["repetitions"]):
                    start=int(rng.integers(26,max(27,len(x)-duration+1))); altered=x.copy(); truth=np.zeros(len(x),bool); truth[start:start+duration]=True
                    signals=int(rng.choice([1,3])); chosen=rng.choice(3,size=signals,replace=False); altered[start:start+duration,chosen]+=magnitude
                    probability=hmm.filtered_probabilities(altered,model)[:,adverse]; predicted=probability>=.5
                    score=metrics(truth,predicted); score.update({"repo_full":repo,"duration":duration,"standardized_shift":magnitude,"signals":signals,"repetition":rep,"detection_delay":delay(truth,predicted)})
                    rows.append(score)
    return pd.DataFrame(rows)


def episodes(panel):
    rows=[]
    for repo,g in panel.sort_values("week").groupby("repo_full"):
        active=g["hmm_filtered_adverse_probability"].ge(.5).to_numpy(); start=None
        for i,value in enumerate(np.r_[active,False]):
            if value and start is None: start=i
            if not value and start is not None:
                block=g.iloc[start:i]
                rows.append({"repo_full":repo,"start_week":str(block.week.iloc[0].date()),"end_week":str(block.week.iloc[-1].date()),"weeks":len(block),"mean_probability":float(block.hmm_filtered_adverse_probability.mean()),"max_probability":float(block.hmm_filtered_adverse_probability.max()),"split":block.split.mode().iloc[0]})
                start=None
    return pd.DataFrame(rows)


def main():
    parser=argparse.ArgumentParser(); parser.add_argument("--config",default="analysis_config.yaml"); args=parser.parse_args(); cfg=load_config(args.config); out=output_dir(cfg,args.config)
    panel=pd.read_csv(out/"hmm_results.csv",parse_dates=["week"]); meta=json.loads((out/"hmm_model.json").read_text())
    model={k:np.asarray(v) if k in {"pi","trans","means","variances"} else v for k,v in meta["model"].items()}; adverse=int(meta["adverse_state"])
    synthetic_result=synthetic(panel,cfg,model,adverse); synthetic_result.to_csv(out/"synthetic_injection_results.csv",index=False)
    summary=synthetic_result.groupby(["duration","standardized_shift","signals"],as_index=False).agg(precision=("precision","mean"),recall=("recall","mean"),false_alarm_rate=("false_alarm_rate","mean"),detection_delay=("detection_delay","mean"))
    summary.to_csv(out/"synthetic_injection_summary.csv",index=False)
    external = panel["split"].eq("external")
    scoring_panel = panel[~external | panel["external_evaluation_eligible"].fillna(False)].copy()
    episode_table=episodes(scoring_panel); episode_table.to_csv(out/"candidate_episodes.csv",index=False)
    split_summary=scoring_panel.groupby(["repo_full","split"],as_index=False).agg(weeks=("week","size"),mean_adverse_probability=("hmm_filtered_adverse_probability","mean"),viterbi_adverse_prevalence=("hmm_viterbi_adverse","mean"),mewma_alarm_prevalence=("mewma_alarm","mean"))
    split_summary.to_csv(out/"holdout_external_summary.csv",index=False)
    write_json(out/"rq1_validation_status.json",{"synthetic_scenarios":len(synthetic_result),"candidate_episodes":len(episode_table),"hmm_stable_occupancy":meta["stable_occupancy"],"interpretation":"candidate adverse operational states; degradation label requires triangulation"})
    print(f"Synthetic evaluations: {len(synthetic_result)}; candidate episodes: {len(episode_table)}")
    return 0


if __name__=="__main__": raise SystemExit(main())