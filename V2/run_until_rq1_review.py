from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


STEPS=("01_build_measurement_panels.py","02_validate_measurements.py","03_fit_baselines.py","04_fit_hmm.py","05_validate_rq1.py")


def main():
    parser=argparse.ArgumentParser(); parser.add_argument("--config",default="analysis_config.yaml"); args=parser.parse_args()
    root=Path(__file__).resolve().parent; config=Path(args.config).resolve()
    for step in STEPS:
        print(f"\n=== {step} ===",flush=True)
        subprocess.run([sys.executable,str(root/step),"--config",str(config)],check=True,cwd=root)
    print("\nRQ1 outputs are ready for review. Do not run RQ2 until RQ1 is reviewed and frozen.")
    return 0


if __name__=="__main__": raise SystemExit(main())