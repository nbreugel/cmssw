import os
import sys
import subprocess
import time
import submitCalibTree.Config
import submitCalibTree.launchJobs

MAIL_ADD = ""
START = time.strftime("%D %H:%M")


def mail(std_runs, aag_runs, cleanup_log):
    """Formats mail to be sent to user."""
    if MAIL_ADD == "":
        print("No email address specified.")
        return

    message = "Production started at %s\n" % START
    message += "             ended at %s\n" % time.strftime("%D %H:%M")
    message += "\n\n\n"
    message += "Std bunch : processes the following runs :\n"

    previous_run = 0
    n_jobs = 1
    runs = {}
    for run in std_runs:
        if not run[0] in runs.keys():
            runs[run[0]] = 1
        else:
            runs[run[0]] += 1

    runs_ordered = sorted(runs.keys())

    for r in runs_ordered:
        message += " Run %s (%s jobs) \n" % (r, runs[r])
    message += "AAG bunch : processed the following runs :\n"
    runs = {}

    for run in aag_runs:
        if not run[0] in runs.keys():
            runs[run[0]] = 1
        else:
            runs[run[0]] += 1

    runs_ordered = runs.keys()
    runs_ordered.sort()

    for run in runs_ordered:
        message += " Run %s (%s jobs) \n" % (run, runs[run])

    message += "\n\n **** Cleaning report **** \n\n"
    message += cleanup_log.replace("\"", "").replace("'", "")

    os.system('echo "%s" | mail -s "CalibTree production status" '
              % message + MAIL_ADD)


DEBUG = False

for arg in sys.argv:
    DEBUG += "debug" in arg.lower()

if DEBUG:
    print("DEBUG MODE DETECTED")
    print("--> Will not submit jobs")
    print('--> Will not modify status files')
    print()

print("Processing Std bunch")
CONFIG = submitCalibTree.Config.configuration(debug=DEBUG)
cleanup_message = "Unable to clean up folder. Back clean-up integrity?"

if CONFIG.integrity:
    print("Cleaning up directory...")
    cleanup_message = subprocess.getstatusoutput(
        "cd %s; python cleanFolders.py; cd -" % CONFIG.RUNDIR)[1]

    print(cleanup_message)

    with open("LastRun.txt", "r") as last_run:
        for line in last_run:
            line = line.replace("\n", "").replace("\r", "")
            if line.isdigit():
                CONFIG.firstRun = int(line)

    with open("FailledRun.txt", "r") as failed:
        for line in failed:
            line = line.split()
            if len(line) == 1:
                if line[0].isdigit() and len(line[0]) == 6:
                    CONFIG.relaunchList.append(line)
                elif len(line) == 3:
                    if (line[0].isdigit() and line[1].isdigit() and
                            line[2].isdigit() and len(line[0])) == 6:
                        CONFIG.relaunchList.append(line)

    if not DEBUG:
        with open("FailledRun.txt", "w") as failed:
            failed.write("")

    last_run_processed = submitCalibTree.launchJobs.generateJobs(CONFIG)

    print(CONFIG.launchedRuns)

    if not DEBUG:
        with open("LastRun.txt", "w") as last_run:
            last_run.write(str(last_run_processed))


print("Processing AAG")

CONFIG_AAG = submitCalibTree.Config.configuration(True, debug=DEBUG)
if CONFIG_AAG.integrity:
    with open("LastRun_Aag.txt", "r") as last_run:
        for line in last_run:
            line = line.replace("\n", "").replace("\r", "")
            if line.isdigit():
                CONFIG_AAG.firstRun = int(line)

    with open("FailledRun_Aag.txt", "r") as failed:
        for line in failed:
            line = line.split()
            if len(line) == 1:
                if line[0].isdigit() and len(line[0]) == 6:
                    CONFIG_AAG.relaunchList.append(line)
            elif len(line) == 3:
                if (line[0].isdigit() and line[1].isdigit() and
                        line[2].isdigit() and len(line[0])) == 6:
                    CONFIG_AAG.relaunchList.append(line)
    if not DEBUG:
        with open("FailledRun_Aag.txt", "w") as failed:
            failed.write("")

    last_run_processed = submitCalibTree.launchJobs.generateJobs(CONFIG_AAG)

    if not DEBUG:
        with open("LastRun_Aag.txt", "w") as last_run:
            last_run.write(str(last_run_processed))

mail(CONFIG.launchedRuns, CONFIG_AAG.launchedRuns, cleanup_message)
