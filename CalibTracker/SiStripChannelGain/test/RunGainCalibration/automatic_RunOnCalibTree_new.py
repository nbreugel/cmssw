#!/usr/bin/env python3

import os
from argparse import ArgumentParser
import subprocess
import json

PCL_DATASET_PATTERN = "/StreamExpress/Run2024*-PromptCalibProdSiStripGains__AAG__-Express-v*/ALCAPROMPT"
CALIBTREE_PATH_PATTERN = "/store/group/dpg_tracker_strip/comm_tracker/Strip/Calibration/calibrationtree/GR24_900GeV__AAG__"
GLOBAL_TAG = "141X_dataRun3_Express_v2"

JOB_SCRIPT_NAME = "submitJob_new.py"
MIN_N_EVENTS = 3000     # Min. events for run to be accepted for gain payload computation
MAX_N_EVENTS = 3000000  # Max. events for run to be accepted for gain payload computation

DQM_DIR_PATTERN = "AlCaReco/SiStripGains__AAG__"

def print_error(exit_code, output):
    print(f"Process returned with error: {exit_code}")
    print(f"Output: {output}")
    print("Exiting...")

def main(first_run, last_run, publish, use_pcl, mode):
    script_dir = os.getcwd()
    if mode == "AagBunch":
        pcl_dataset = PCL_DATASET_PATTERN.replace("__AAG__", "AAG")
        calibtree_path = CALIBTREE_PATH_PATTERN.replace("__AAG__", "_Aag")
        DQM_dir = DQM_DIR_PATTERN.replace("__AAG__", "AAG")
    elif mode == "StdBunch":
        pcl_dataset = PCL_DATASET_PATTERN.replace("__AAG__", "")
        calibtree_path = CALIBTREE_PATH_PATTERN.replace("__AAG__", "")
        DQM_dir = DQM_DIR_PATTERN.replace("__AAG__", "")

    is_automatic = (first_run == -1 and last_run == -1)
    # for now, we do not consider automatic running

    print("\nGain payload computing configuration:")
    print(f"    first_run = {first_run}")
    print(f"    last_run  = {last_run}")
    print(f"    publish   = {publish}")
    print(f"    use_PCL   = {use_pcl}")
    print(f"    mode      = {mode}")
    print(f"    DQM_dir   = {DQM_dir}\n")

    init_env_cmd =  f"cd {script_dir};"
    init_env_cmd += "source /cvmfs/cms.cern.ch/cmsset_default.sh;"
    init_env_cmd += "cmsenv;"

    n_events_total = 0
    run = 0
    run_info_text = ""
    file_info_text = ""

    if not use_pcl:
        print("Currently we have not implemented running over Calibration Trees. Exiting...")
        return

    get_pcl_datasets_cmd = f"dasgoclient --limit=9999 --query='dataset={pcl_dataset}'"
    print("Getting list of PCL output files from DAS...")
    print(get_pcl_datasets_cmd)
    status, output = subprocess.getstatusoutput(init_env_cmd + get_pcl_datasets_cmd)
    if status > 0:
        print_error(status, output)
        return
        
    dataset_path_list = [line for line in output.split("\n") if line != "\n"]
    print(f"Found {len(dataset_path_list)} datasets matching the pattern {pcl_dataset}:")
    print(output)

    dataset_run_list = []
    for dataset_path in dataset_path_list:
        get_dataset_runs_cmd = f"dasgoclient --limit=9999 --query='run dataset={dataset_path}'"
        print(get_dataset_runs_cmd)
        status, output = subprocess.getstatusoutput(init_env_cmd + get_dataset_runs_cmd)
        if status > 0:
            print_error(status, output)
            return
        das_dataset_run_list = [int(line) for line in output.split("\n") if line != "\n"]
        for run in das_dataset_run_list:
            dataset_run_list += [(dataset_path, run)]

    print(f"Found {len(dataset_run_list)} runs:")
    print(output)

    included_runs = []
    for dataset_path, run in dataset_run_list:
        print(f"Gathering information for run {run}...", end=" ")
        run_in_user_range = (run > first_run
                             and run < last_run
                             and last_run > 0)
        if not run_in_user_range:
            print(f"[NOT INCLUDED] Run {run} is not in range {first_run} - {last_run}")
            continue
        get_run_info_cmd = f"dasgoclient --limit=-9999 --query='summary dataset={dataset_path} run={run}'"
        status, output = subprocess.getstatusoutput(init_env_cmd + get_run_info_cmd)
        if status > 0:
            print_error(status, output)
            return
        run_info_dict = json.loads(output)[0]
        n_events_run = run_info_dict["nevents"]
        if n_events_run < MIN_N_EVENTS:
            print(f"[NOT INCLUDED] Too few events ({n_events_run})")
            continue    
        n_events_text = str(n_events_run/1000).rjust(8) + 'K'
        run_info_text += f"# run = {run} --> n_events = {n_events_text}\n"
        get_run_files_cmd = f"dasgoclient --limit=9999 --query='file dataset={dataset_path} run={run}'"
        status, output = subprocess.getstatusoutput(init_env_cmd + get_run_files_cmd)
        if status > 0:
            print_error(status, output)
            return
        file_list = [line for line in output.split("\n") if line != "\n"]
        file_info_text += "\n".join(["calibTreeList.extend(['" + file + "'])" for file in file_list])
        n_events_total += n_events_run
        print(f"[INCLUDED] {n_events_text} events, cumulative total = {n_events_total}")
        included_runs += [run]

    n_events_total_text = str(n_events_total/1000).rjust(8) + 'K'
    first_run = min(included_runs)
    last_run = max(included_runs)
    print("\n---------------------------------------------------------------------------------------------------")
    print(f"Run range: [{first_run}, {last_run}] --> {n_events_total_text} events.")

    if n_events_total < 2e6:
        print(f"WARNING: Low statistics ({n_events_total_text} events). Need at least 2M events to perform calibration.")
        answer = ""
        while answer not in ["y", "n"]:
            answer = input("Would you like to continue anyway? (y/n) ")
        if answer == "n":
            print("Exiting...")
            return

    tag = f"Run_{first_run}_{last_run}_{mode}_PCL"
    working_dir_path = os.path.abspath(f"../Data_{tag}") # Same level as this folder
    if not os.path.isdir(working_dir_path):
        os.mkdir(working_dir_path)

    os.system(f"cp ../RunGainCalibration/* {working_dir_path}/.")
    with open(working_dir_path + "/FileList_cfg.py", "w") as file:
        file.write("import FWCore.ParameterSet.Config as cms\n")
        file.write("calibTreeList = cms.untracked.vstring()\n")
        file.write(f"#Total number of events considered is: {n_events_total}\n")
        file.write(run_info_text)
        file.write(file_info_text)
        file.write("\n")

    os.system(f"sed -i 's|XXX_FIRSTRUN_XXX|{first_run}|g' {working_dir_path}/*_cfg.py")
    os.system(f"sed -i 's|XXX_LASTRUN_XXX|{last_run}|g' {working_dir_path}/*_cfg.py")
    os.system(f"sed -i 's|XXX_GT_XXX|{GLOBAL_TAG}|g' {working_dir_path}/*_cfg.py")
    os.system(f"sed -i 's|XXX_PCL_XXX|{use_pcl}|g' {working_dir_path}/*_cfg.py")
    os.system(f"sed -i 's|XXX_CALMODE_XXX|{mode}|g' {working_dir_path}/*_cfg.py")
    os.system(f"sed -i 's|XXX_DQMDIR_XXX|{DQM_dir}|g' {working_dir_path}/*_cfg.py")

    # Setup HTCondor submission:
    condor_dir = os.path.join(working_dir_path, "condor_submission")
    if not os.path.isdir(condor_dir):
        os.mkdir(condor_dir)

    launch_script_path = os.path.join(condor_dir, "launch.sh")
    launch_cmd = "#!/bin/bash\n"
    launch_cmd += ";\n".join(init_env_cmd.split(";"))
    launch_cmd += f"cd {working_dir_path};\n"
    launch_cmd += "pwd;\n"
    launch_cmd += "ls;\n"
    launch_cmd += f"./{JOB_SCRIPT_NAME} -f {first_run} -l {last_run} -m {mode} -s {working_dir_path}\n"
    with open(launch_script_path, "w") as launch:
        launch.write(launch_cmd)
    os.system(f"chmod +x {launch_script_path}")

    condor_block = "Universe = vanilla\n"
    condor_block += f"Executable = {launch_script_path}\n"
    condor_block += "Arguments = \n"
    condor_block += "\n"
    condor_block += f"Error = {condor_dir}/job.err\n"
    condor_block += f"Output = {condor_dir}/job.out\n"
    condor_block += f"Log = {condor_dir}/job.log\n"
    condor_block += "\n"
    condor_block += "+JobFlavour = \"workday\"\n"
    condor_block += "queue\n"
    with open(condor_dir + "/condor_submission.submit", "w") as submit:
        submit.write(condor_block)
    
        
if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("-f", "--firstRun", help="First run to process (-1 = automatic)", default=-1, type=int)
    parser.add_argument("-l", "--lastRun", help="Last run to process (-1 = automatic)", default=-1, type=int)
    parser.add_argument("-P", "--publish", help="Publish the results to ...", default=False, action="store_true")
    parser.add_argument("-p", "--pcl", help="Run on PCL dataset instead of calibration tree", default=False, action="store_true")
    parser.add_argument("-m", "--mode", help="Select the statistics type (AagBunch/StdBunch)", default="AagBunch", choices=["AagBunch", "StdBunch"])
    args = parser.parse_args()
    main(args.firstRun, args.lastRun, args.publish, args.pcl, args.mode)
