#!/usr/bin/env python3
import os
import sys
import subprocess
import time
from submitCalibTree.Config import Configuration
from submitCalibTree.JobSubmitter import JobSubmitter
import argparse

parser =argparse.ArgumentParser()
parser.add_argument("-d", "--dryrun", default=False, action="store_true", help="Dryrun: jobs will not be submitted.")
parser.add_argument("-v", "--verbose", default=False, action="store_true", help="Verbosity: will print more information in debug mode.")

args = parser.parse_args()

DEBUG        = args.verbose
DRYRUN       = args.dryrun
START_TIME   = time.strftime("%D %H:%M")

def SubmitJobs(use_AAG = False, use_debug = False):
    print("===> PROCESSING %s BUNCH...\n" % ("STD" if not use_AAG else "AAG"))
    config = Configuration(debug_mode = use_debug, use_AAG = use_AAG)

    LastRun_textfile = "LastRun%s.txt" % ("" if not use_AAG else "_Aag")
    FailedRun_textfile = "FailedRun%s.txt" % ("" if not use_AAG else "_Aag")

    if not os.path.isfile(LastRun_textfile):
        os.system("touch " + LastRun_textfile)

    if not os.path.isfile(FailedRun_textfile):
        os.system("touch " + FailedRun_textfile)
    
    if not config.integrity:
        print("")
        print("There were some problems with the configuration:")
        print(config)
        print("")
    
    if config.integrity:

        print("Using the following configuration:")
        print(config)
        print("")

        with open(LastRun_textfile, "r") as last_run:
            for line in last_run:
                line = line.replace("\n", "").replace("\r", "")
                if line.isdigit():
                    config.first_run = int(line)

        with open(FailedRun_textfile, "r") as failed:
            for line in failed:
                line = line.split()
                if len(line) == 1:
                    if line[0].isdigit() and len(line[0]) == 6:
                        config.relaunch_list.append(line)
                    elif len(line) == 3:
                        if (line[0].isdigit() and line[1].isdigit() and
                                line[2].isdigit() and len(line[0])) == 6:
                            config.relaunch_list.append(line)

        if not use_debug:
            with open(FailedRun_textfile, "w") as failed:
                failed.write("")
                
        condor_submission_handler = JobSubmitter(config)
        last_run_processed        = condor_submission_handler.generateJobs()
        config.launched_runs_dict = condor_submission_handler.launched_runs_dict

        if len(config.launched_runs_dict) > 0:
            if not DRYRUN:
                condor_submission_handler.launchJobs()
            else:
                print("")
                print("You specified 'dryrun' so the jobs have not been submitted.")
                print("In order to submit the jobs, use the following command:")
                print("    condor_submit %s/condor_submission.submit" % config.condor_dir)
                print("")
        
        if not DEBUG:
            with open(LastRun_textfile, "w") as last_run:
                last_run.write(str(last_run_processed))

        return config

def mail(std_runs, aag_runs, mail_address):

    if mail_address == "":
        print("No email address specified.")
        return

    message  = "Job production started at %s\n" % START_TIME
    message += "Job production ended at %s\n" % time.strftime("%D %H:%M")
    message += "\n\n\n"
    message += "Std bunch: The following runs will be processed:\n"
    
    for run in sorted(std_runs.keys()):
        message += "Run %s (%s files, %s jobs)\n" % (run, std_runs[run]["n_files"], std_runs[run]["n_jobs"])

    message += "\n"
    message += "AAG bunch: The following runs will be processed:\n"

    for run in sorted(aag_runs.keys()):
        message += "Run %s (%s files, %s jobs)\n" % (run, aag_runs[run]["n_files"], aag_runs[run]["n_jobs"])
    
    os.system('echo "%s" | mail -s "CalibTree production status" '
              % message + mail_address)



    
if __name__ == "__main__":
    config     = SubmitJobs(use_AAG=False, use_debug=DEBUG)
    config_AAG = SubmitJobs(use_AAG=True, use_debug=DEBUG)

    if not DRYRUN:
        mail(config.launched_runs_dict, config_AAG.launched_runs_dict, config.mail_address)
