#!/usr/bin/env python3

import os
from argparse import ArgumentParser
import subprocess
import json
import ROOT

PCL_DATASET_PATTERN = "/StreamExpress/Run2024*-PromptCalibProdSiStripGains__AAG__-Express-v*/ALCAPROMPT"
CALIBTREE_PATH_PATTERN = "/store/group/dpg_tracker_strip/comm_tracker/Strip/Calibration/calibrationtree/GR24_900GeV__AAG__"
GLOBAL_TAG = "140X_dataRun3_Express_v3"

JOB_SCRIPT_NAME = "submitJob_new.py"
MIN_N_EVENTS = 3000     # Min. events for run to be accepted for gain payload computation
MAX_N_EVENTS = 3000000  # Max. events for run to be accepted for gain payload computation

DQM_DIR_PATTERN = "AlCaReco/SiStripGains__AAG__"

class GainSubmissionFactory:
    def __init__(self, first_run, last_run, publish, mode):
        # User variables:
        self.first_run = first_run
        self.last_run = last_run
        self.publish = publish
        self.mode = mode

        # Script init variables:
        self.script_dir = os.getcwd()
        if self.mode == "AagBunch":
            self.pcl_dataset = PCL_DATASET_PATTERN.replace("__AAG__", "AAG")
            self.calibtree_path = CALIBTREE_PATH_PATTERN.replace("__AAG__", "_Aag")
            self.DQM_dir = DQM_DIR_PATTERN.replace("__AAG__", "AAG")
        elif self.mode == "StdBunch":
            self.pcl_dataset = PCL_DATASET_PATTERN.replace("__AAG__", "")
            self.calibtree_path = CALIBTREE_PATH_PATTERN.replace("__AAG__", "")
            self.DQM_dir = DQM_DIR_PATTERN.replace("__AAG__", "")

        self.init_env_cmd =  f"cd {self.script_dir};"
        self.init_env_cmd += "source /cvmfs/cms.cern.ch/cmsset_default.sh;"
        self.init_env_cmd += "cmsenv;"

        # Variables that will be filled
        self.included_runs = []
        self.n_events_total = 0
        self.run_info_text = ""
        self.file_info_text = ""
       
    def print_error(self, exit_code, output):
        print(f"Process returned with error: {exit_code}")
        print(f"Output: {output}")
        print("Exiting...")

    def get_n_events_from_calibtree(self, path):
        root_file = ROOT.TFile.Open(path, "read")
        tree = root_file.Get(f"gainCalibrationTree{self.mode}/tree")
        n_entries = tree.GetEntries()
        root_file.Close()
        return n_entries
        
    def run_on_pcl(self):
        print("Running on PCL files...")
        get_pcl_datasets_cmd = f"dasgoclient --limit=9999 --query='dataset={self.pcl_dataset}'"
        print("Getting list of PCL output files from DAS...")
        print(get_pcl_datasets_cmd)
        status, output = subprocess.getstatusoutput(self.init_env_cmd + get_pcl_datasets_cmd)
        if status > 0:
            self.print_error(status, output)
            return

        dataset_path_list = [line for line in output.split("\n") if line != "\n"]
        print(f"Found {len(dataset_path_list)} datasets matching the pattern {self.pcl_dataset}:")
        print(output)

        dataset_run_list = []
        for dataset_path in dataset_path_list:
            get_dataset_runs_cmd = f"dasgoclient --limit=9999 --query='run dataset={dataset_path}'"
            print(get_dataset_runs_cmd)
            status, output = subprocess.getstatusoutput(self.init_env_cmd + get_dataset_runs_cmd)
            if status > 0:
                self.print_error(status, output)
                return
            das_dataset_run_list = [int(line) for line in output.split("\n") if line != "\n"]
            for run in das_dataset_run_list:
                dataset_run_list += [(dataset_path, run)]

        print(f"Found {len(dataset_run_list)} runs:")
        print(output)

        for dataset_path, run in dataset_run_list:
            
            run_in_user_range = (run >= self.first_run
                                 and run <= self.last_run)
            if not run_in_user_range:
                continue
            print(f"Gathering information for run {run}...", end=" ")
            get_run_info_cmd = f"dasgoclient --limit=-9999 --query='summary dataset={dataset_path} run={run}'"
            status, output = subprocess.getstatusoutput(self.init_env_cmd + get_run_info_cmd)
            if status > 0:
                self.print_error(status, output)
                return
            run_info_dict = json.loads(output)[0]
            n_events_run = run_info_dict["nevents"]
            if n_events_run < MIN_N_EVENTS:
                print(f"[NOT INCLUDED] Too few events ({n_events_run})")
                continue    
            n_events_text = str(n_events_run/1000).rjust(8) + 'K'
            self.run_info_text += f"# run = {run} --> n_events = {n_events_text}\n"
            get_run_files_cmd = f"dasgoclient --limit=9999 --query='file dataset={dataset_path} run={run}'"
            status, output = subprocess.getstatusoutput(self.init_env_cmd + get_run_files_cmd)
            if status > 0:
                self.print_error(status, output)
                return
            file_list = [line for line in output.split("\n") if line != "\n"]
            self.file_info_text += "\n".join(["calibTreeList.extend(['" + file + "'])" for file in file_list]) + "\n"
            print(f"[INCLUDED] {n_events_text} events, cumulative total = {self.n_events_total}")

            self.n_events_total += n_events_run
            self.included_runs += [run]

    def run_on_calibtrees(self):
        print("Running on calibtrees...")
        print(f"Getting list of calibration trees from {self.calibtree_path}")
        eos_cmd = f"eos ls -l {self.calibtree_path}"
        status, output = subprocess.getstatusoutput(self.init_env_cmd + eos_cmd)
        if status > 0:
            self.print_error(status, output)
            return
        calibtree_info = output.split("\n")
        info_list = []
        for line in calibtree_info:
            name = line.split()[8]
            name_split = name.split("_")
            run = 0
            for piece in name_split:
                if "." in piece:
                    piece = piece.split(".")[0]
                if len(piece) == 6 and piece.isdigit():
                    run = int(piece)
                    break
            if run == 0:
                print(f"Could not figure out run from filename: {name}")
                continue
            file_size = int(line.split()[4]) / 1048576
            info_list += [(name, run, file_size)]
        info_list.sort(key = lambda x: x[1])
        print("Checking number of events available...")
        for info in info_list:
            file_name, run, size = info
            print(f"Processing file {file_name}...", end=" ")
            run_in_user_range = (run > self.first_run) and (run < self.last_run)
            if run_in_user_range:
                file_path = f"root://eoscms//eos/cms{self.calibtree_path}/{file_name}"
                n_events = self.get_n_events_from_calibtree(file_path)
                if n_events < 300:
                    print(f"[NOT INCLUDED] Not enough events ({n_events})")
                    continue
                size_text = str(size).rjust(6)
                n_events_txt = str(n_events/1000).rjust(8)
                self.file_info_text += f'calibTreeList.extend(["{file_path}"])'
                self.file_info_text += f' # {size_text}MB, NEvents={n_events_txt}K\n'
                self.n_events_total += n_events
                if run not in self.included_runs:
                    self.included_runs += [run]
                print(f"[INCLUDED] Cumulative number of events: {self.n_events_total}")
    
    def __str__(self):
        text = "\nGain payload computing configuration:\n"
        text += f"    first_run = {self.first_run}\n"
        text += f"    last_run  = {self.last_run}\n"
        text += f"    publish   = {self.publish}\n"
        text += f"    mode      = {self.mode}\n"
        text += f"    DQM_dir   = {self.DQM_dir}\n"
        return text

    
def main(first_run, last_run, publish, use_pcl, mode):

    factory = GainSubmissionFactory(first_run, last_run, publish, mode)
    print(factory)
    if use_pcl:
        factory.run_on_pcl()
    else:
        factory.run_on_calibtrees()

    if factory.included_runs == []:
        print("No runs were found. Exiting...")
        return
    
    n_events_total_text = str(factory.n_events_total/1000).rjust(8) + 'K'
    first_run = min(factory.included_runs)
    last_run = max(factory.included_runs)
    print("---------------------------------------------------------------------------------------------------\n")
    print(f"Run range: [{first_run}, {last_run}] --> {n_events_total_text} events.")

    if factory.n_events_total < 2e6:
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
        file.write(f"#Total number of events considered is: {factory.n_events_total}\n")
        file.write(factory.run_info_text)
        file.write(factory.file_info_text)
        file.write("\n")

    os.system(f"sed -i 's|XXX_FIRSTRUN_XXX|{first_run}|g' {working_dir_path}/*_cfg.py")
    os.system(f"sed -i 's|XXX_LASTRUN_XXX|{last_run}|g' {working_dir_path}/*_cfg.py")
    os.system(f"sed -i 's|XXX_GT_XXX|{GLOBAL_TAG}|g' {working_dir_path}/*_cfg.py")
    os.system(f"sed -i 's|XXX_PCL_XXX|{use_pcl}|g' {working_dir_path}/*_cfg.py")
    os.system(f"sed -i 's|XXX_CALMODE_XXX|{mode}|g' {working_dir_path}/*_cfg.py")
    os.system(f"sed -i 's|XXX_DQMDIR_XXX|{factory.DQM_dir}|g' {working_dir_path}/*_cfg.py")

    # Setup HTCondor submission:
    condor_dir = os.path.join(working_dir_path, "condor_submission")
    if not os.path.isdir(condor_dir):
        os.mkdir(condor_dir)

    launch_script_path = os.path.join(condor_dir, "launch.sh")
    launch_cmd = "#!/bin/bash\n"
    launch_cmd += ";\n".join(factory.init_env_cmd.split(";"))
    launch_cmd += f"cd {working_dir_path};\n"
    launch_cmd += "pwd;\n"
    launch_cmd += "ls;\n"
    launch_cmd += f"./{JOB_SCRIPT_NAME} -f {factory.first_run} -l {factory.last_run} -m {mode} -s {working_dir_path}\n"
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
