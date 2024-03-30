#!/usr/bin/env python3

from argparse import ArgumentParser
import os


def main(first_run, last_run, mode, script_dir):
    init_env_cmd = f"cd {script_dir};"
    init_env_cmd += "source /cvmfs/cms.cern.ch/cmsset_default.sh;"
    init_env_cmd += "cmsenv;"

    name = f"Run_{first_run}_to_{last_run}_{mode}_PCL"
    print(name)

    additional_text = f"CMS Preliminary  -  Run {first_run} to {last_run}"
    # run_sequence_cmd = 'sh sequence.sh "{name}" "{mode}" "{additional_text}"'
    
    run_cmd = "cmsRun Gains_Compute_cfg.py;"
    print("Running gains computation...")
    print(run_cmd)
    result = os.system(init_env_cmd + run_cmd)
    if result > 0:
        print("Gain calibration failed! Exiting...")
        return

    run_cmd = "root -l -b -q KeepOnlyGain.C+"
    print("Running ROOT macro...")
    print(run_cmd)
    result = os.system(init_env_cmd + run_cmd)
    if result > 0:
        print("ROOT macro failed! Exiting...")
        return

    run_cmd = f'sh PlotMacro.sh "\\\"{mode}\\\"" "\\\"{additional_text}\\\"";'
    print("Running plotting script...")
    print(run_cmd)
    result = os.system(init_env_cmd + run_cmd)
    if result > 0:
        print("Plotting script failed! Exiting...")
        return

    print("Job finished! :^)")
    

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("-f", "--firstRun", default=-1)
    parser.add_argument("-l", "--lastRun", default=-1)
    parser.add_argument("-m", "--mode", default="AagBunch", choices=["AagBunch", "StdBunch"])
    parser.add_argument("-s", "--scriptDir", default="")
    args = parser.parse_args()
    main(args.firstRun, args.lastRun, args.mode, args.scriptDir)
