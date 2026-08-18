from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone

from ci_common import load_config, output_dir, sha256, write_json


def main():
    parser=argparse.ArgumentParser(description="Freeze the validated RQ1 detector before any RQ2 model is run")
    parser.add_argument("--config",default="analysis_config.yaml")
    parser.add_argument("--confirm",action="store_true",help="Confirm that diagnostics and triangulation have been reviewed")
    args=parser.parse_args()
    if not args.confirm: raise SystemExit("Review RQ1 outputs, then rerun with --confirm")
    cfg=load_config(args.config); out=output_dir(cfg,args.config)
    required=[out/"hmm_results.csv",out/"hmm_model.json",out/"rq1_validation_status.json",out/"synthetic_injection_summary.csv"]
    missing=[str(p) for p in required if not p.is_file()]
    if missing: raise FileNotFoundError("Missing RQ1 artifacts: "+", ".join(missing))
    status=json.loads((out/"rq1_validation_status.json").read_text())
    payload={"frozen_at_utc":datetime.now(timezone.utc).isoformat(),"config_sha256":sha256(args.config),"artifacts":{p.name:sha256(p) for p in required},"validation_status":status,
             "rule":"No metric, threshold, repository, state count, or detector hyperparameter may be changed to maximise RQ2 associations."}
    write_json(out/"RQ1_FROZEN.json",payload); print(json.dumps(payload,indent=2)); return 0


if __name__=="__main__": raise SystemExit(main())