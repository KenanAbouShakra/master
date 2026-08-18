from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
import tempfile
from pathlib import Path

import pandas as pd
import yaml

from ci_common import CORE_METRICS, robust_training_standardize


ROOT=Path(__file__).resolve().parent


def main():
    with tempfile.TemporaryDirectory() as temporary:
        temp=Path(temporary); db=temp/"test.sqlite"; out=temp/"out"
        cfg=yaml.safe_load((ROOT/"analysis_config.yaml").read_text())
        cfg["paths"]={"sqlite":str(db),"output":str(out)}
        cfg["synthetic_validation"]["repetitions"]=2; cfg["hmm"]["random_starts"]=3; cfg["hmm"]["maximum_iterations"]=30
        config=temp/"config.yaml"; config.write_text(yaml.safe_dump(cfg,sort_keys=False))
        con=sqlite3.connect(db)
        con.executescript("""
        CREATE TABLE workflows(repo_full TEXT,workflow_id INTEGER,name TEXT,path TEXT,state TEXT);
        CREATE TABLE workflow_runs(repo_full TEXT,run_id INTEGER,workflow_id INTEGER,current_run_attempt INTEGER,event TEXT,status TEXT,conclusion TEXT,created_at TEXT,PRIMARY KEY(repo_full,run_id));
        CREATE TABLE run_attempts(repo_full TEXT,run_id INTEGER,attempt_number INTEGER,workflow_id INTEGER,name TEXT,path TEXT,event TEXT,status TEXT,conclusion TEXT,created_at TEXT,run_started_at TEXT,head_sha TEXT,PRIMARY KEY(repo_full,run_id,attempt_number));
        CREATE TABLE workflow_jobs(repo_full TEXT,run_id INTEGER,attempt_number INTEGER,job_id INTEGER,started_at TEXT,completed_at TEXT,PRIMARY KEY(repo_full,run_id,attempt_number,job_id));
        CREATE TABLE pull_requests(repo_full TEXT,pr_number INTEGER,created_at TEXT,merged_at TEXT,closed_at TEXT,additions INTEGER,deletions INTEGER,changed_files INTEGER,commits_count INTEGER,PRIMARY KEY(repo_full,pr_number));
        CREATE TABLE pr_reviews(repo_full TEXT,pr_number INTEGER,user_login TEXT,user_type TEXT,state TEXT,submitted_at TEXT);
        CREATE TABLE pr_ci_links(repo_full TEXT,pr_number INTEGER,run_id INTEGER,link_method TEXT);
        """)
        repos=cfg["study"]["development_repositories"]+cfg["study"]["external_repositories"]
        start=__import__('datetime').datetime(2025,6,23)
        rid=0
        for rix,repo in enumerate(repos):
            con.execute("INSERT INTO workflows VALUES(?,?,?,?,?)",(repo,1,"CI",".github/workflows/ci.yml","active"))
            for week in range(59):
                day=start+__import__('datetime').timedelta(weeks=week)
                for j in range(6):
                    rid+=1; created=day+__import__('datetime').timedelta(hours=j); bad=(week in range(35,39)) or (week < 13 and week % 4 == 0); rerun=2 if (j==0 and bad) else 1
                    conclusion="failure" if bad and j<2 else "success"
                    con.execute("INSERT INTO workflow_runs VALUES(?,?,?,?,?,?,?,?)",(repo,rid,1,rerun,"pull_request","completed",conclusion,created.isoformat()+"Z"))
                    for attempt in range(1,rerun+1):
                        ac=created+__import__('datetime').timedelta(minutes=attempt-1); end=ac+__import__('datetime').timedelta(minutes=20+(40 if bad else 0))
                        aconclusion="failure" if attempt<rerun else conclusion
                        con.execute("INSERT INTO run_attempts VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",(repo,rid,attempt,1,"CI",".github/workflows/ci.yml","pull_request","completed",aconclusion,ac.isoformat()+"Z",ac.isoformat()+"Z",f'sha{rid}'))
                        con.execute("INSERT INTO workflow_jobs VALUES(?,?,?,?,?,?)",(repo,rid,attempt,rid*10+attempt,(ac+__import__('datetime').timedelta(minutes=2)).isoformat()+"Z",end.isoformat()+"Z"))
                    pr=rid; pc=created-__import__('datetime').timedelta(hours=10); merged=created+__import__('datetime').timedelta(hours=10+(20 if bad else 0))
                    con.execute("INSERT INTO pull_requests VALUES(?,?,?,?,?,?,?,?,?)",(repo,pr,pc.isoformat()+"Z",merged.isoformat()+"Z",merged.isoformat()+"Z",10,5,2,1))
                    con.execute("INSERT INTO pr_reviews VALUES(?,?,?,?,?,?)",(repo,pr,"reviewer","User","APPROVED",(pc+__import__('datetime').timedelta(hours=2)).isoformat()+"Z"))
                    con.execute("INSERT INTO pr_ci_links VALUES(?,?,?,?)",(repo,pr,rid,"sha"))
        con.commit(); con.close()
        for script in ("01_build_measurement_panels.py","02_validate_measurements.py"):
            subprocess.run([sys.executable,str(ROOT/script),"--config",str(config)],check=True,cwd=ROOT)
        expected=("repository_week_panel.csv","repository_week_panel_audited.csv","data_quality_report.json","measurement_metadata.json")
        missing=[name for name in expected if not (out/name).is_file()]
        if missing: raise AssertionError(missing)
        report=json.loads((out/"data_quality_report.json").read_text())
        if report["status"]!="PASS": raise AssertionError(report)
        panel=pd.read_csv(out/"repository_week_panel_audited.csv",parse_dates=["week"])
        external=set(cfg["study"]["external_repositories"]); prefix=int(cfg["study"]["external_calibration_weeks"])
        for repo in external:
            group=panel[panel["repo_full"].eq(repo)].sort_values("week")
            selected=group[group["external_calibration"]]["week"].tolist()
            expected_weeks=group[group["attempts_total"].gt(0)]["week"].iloc[:prefix].tolist()
            if selected!=expected_weeks or len(selected)!=prefix: raise AssertionError((repo,selected))
            if (group["external_calibration"] & group["external_evaluation_eligible"]).any(): raise AssertionError(repo)
        original=panel.copy(deep=True)
        _,parameters=robust_training_standardize(panel,cfg)
        pd.testing.assert_frame_equal(panel,original)
        post_prefix=panel.copy(deep=True)
        post_mask=post_prefix["repo_full"].isin(external) & post_prefix["external_evaluation_eligible"]
        post_prefix.loc[post_mask,list(CORE_METRICS)]=post_prefix.loc[post_mask,list(CORE_METRICS)]*17+11
        _,post_parameters=robust_training_standardize(post_prefix,cfg)
        for repo in external:
            if parameters[repo]!=post_parameters[repo]: raise AssertionError(f"Post-calibration leakage: {repo}")
        external_changed=panel.copy(deep=True)
        external_mask=external_changed["repo_full"].isin(external)
        external_changed.loc[external_mask,list(CORE_METRICS)]=external_changed.loc[external_mask,list(CORE_METRICS)]*19+7
        _,changed_parameters=robust_training_standardize(external_changed,cfg)
        for repo in cfg["study"]["development_repositories"]:
            if parameters[repo]!=changed_parameters[repo]: raise AssertionError(f"External-to-development leakage: {repo}")
        print("SMOKE TEST PASSED")


if __name__=="__main__": main()