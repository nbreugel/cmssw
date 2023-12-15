#!/usr/bin/env python3
import argparse
import os
import subprocess
from Config import Configuration


parser = argparse.ArgumentParser()
parser.add_argument("-r", "--run",    help="Run number to process.",     default=-1, type=int)
parser.add_argument("-f", "--files",  help="Files to process.",          default="")
parser.add_argument("-a", "--aag",    help="Use AAG dataset.",           default=False, action="store_true")
parser.add_argument("-n", "--number", help="The Nth batch of files from the run that will be processed by this job.", type=int)

args = parser.parse_args()

print("Running Calibration Tree production...\n")

FILES_TO_PROCESS = args.files
RUN              = args.run
USE_AAG          = args.aag
BATCH_NUMBER     = args.number

CONFIG           = Configuration(use_AAG = USE_AAG)
CASTOR_DIR       = CONFIG.CASTOR_dir
FILES_PER_JOB    = CONFIG.n_files_per_job
GLOBAL_TAG       = CONFIG.global_tag
COLLECTION       = CONFIG.collection

N_FILES          = len(FILES_TO_PROCESS.split(","))
FILE_START       = (BATCH_NUMBER - 1) * FILES_PER_JOB + 1
FILE_END         = FILE_START + N_FILES - 1

PWD              = os.getcwd()

if BATCH_NUMBER != 1:
    suffix_format = "_" + str(BATCH_NUMBER)
else:
    suffix_format = ""
    
OUTFILE        = "calibtree_%s%s.root" % (str(RUN),suffix_format)

cmsrun_command =  "cmsRun produceCalibrationTree_template_cfg.py"
cmsrun_command += " outputFile=" + OUTFILE
cmsrun_command += " conditionGT=" + GLOBAL_TAG
cmsrun_command += " inputCollection=" + COLLECTION
cmsrun_command += " inputFiles=\"%s\"" % FILES_TO_PROCESS
cmsrun_command += " runNumber=" + str(RUN)

print(CONFIG)
print("")
print("Processing files %i to %i of run %i (%i files total):\n" % (FILE_START, FILE_END, RUN, N_FILES))
print(FILES_TO_PROCESS.replace(",","\n") + "\n")
print(cmsrun_command + "\n")

# (exit_code, output) = subprocess.getstatusoutput(CONFIG.init_env_command + "export XRD_LOGLEVEL=Debug; " + cmsrun_command)
(exit_code, output) = subprocess.getstatusoutput(CONFIG.init_env_command + cmsrun_command)
LOCAL_TEST = False

if int(exit_code) != 0:
    print("Job failed with exit code " + str(exit_code))
    print("Job output:\n")
    print(output)
    os.system("echo %i %s >> FailedRuns%s.txt" % (RUN, FILES_TO_PROCESS, "_Aag" if USE_AAG else ""))
else:
    file_size_in_kilobytes = int(os.path.getsize(OUTFILE) * 1e-3)
    if file_size_in_kilobytes > 10:
        print("Preparing for stageout of " + OUTFILE + " on " + CASTOR_DIR + "/" + OUTFILE + ". The file size is %d KB" % file_size_in_kilobytes)
        cp_command =  "eos cp " + OUTFILE
        
        if LOCAL_TEST:
            CONFIG.init_env_command += "export EOS_MGM_URL=root://eosuser.cern.ch;"
            cp_command += " root://eosuser.cern.ch//eos/user/n/nbreugel/%s" % (OUTFILE)
        else:
            cp_command += " root://eoscms.cern.ch//eos/cms/%s/%s" % (CASTOR_DIR, OUTFILE)
            
        (stageout_exit_code, output) = subprocess.getstatusoutput(CONFIG.init_env_command + cp_command)
        
        if stageout_exit_code != 0:
            print("STAGE OUT FAILED WITH EXIT CODE " + str(stageout_exit_code))
            print(output)
            
        os.system("rm " + OUTFILE)

if OUTFILE in os.listdir(PWD):
    os.system("rm " + OUTFILE)


