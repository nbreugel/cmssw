#!/usr/bin/env python

import os
import sys
import commands
import ROOT
import argparse

skip_small_files = False
skip_low_runs = True
calibtree_min_filesize = 10 # MB
runs_min_nevents = 3

minNEvents = 3000
maxNEvents = 3000000

globaltag = "92X_dataRun2_Express_v2"

parser = argparse.ArgumentParser(description='')
parser.add_argument('-f',
                    '--firstRun',
                    help='First run to process (-1 --> automatic)',
                    default='-1')
parser.add_argument('-l',
                    '--lastRun',
                    help='Last run to process (-1 --> automatic)',
                    default='-1')
parser.add_argument('-p',
                    '--pcl',
                    dest='usePCL',
                    help='Use PCL output instead of CalibTree.',
                    default='True')
parser.add_argument('-m',
                    '--mode',
                    dest='calMode',
                    help='Select the statistics type (Aagbunch/StdBunch.',
                    default='Aagbunch')
args = parser.parse_args()

PCLDATASETPATH_TEMPLATE = "/StreamExpress/Commissioning2021-PromptCalibProdSiStripGains-Express-v*/ALCAPROMPT"
CALIBTREEPATH_TEMPLATE = '/store/group/dpg_tracker_strip/comm_tracker/Strip/Calibration/calibrationtree/GR21__AAG__'

firstRun = int(args.firstRun)
lastRun = int(args.lastRun)
calMode = args.calMode.lower()usePCL = (args.usePCL == 'True')

automatic = (firstRun == -1 and lastRun == -1)

PCLDATASETPATH = PCLDATASETPATH_TEMPLATE.replace("__AAG__","") if calMode=="stdbunch" else PCLDATASETPATH_TEMPLATE.replace("__AAG__","AAG")
CALIBTREEPATH = CALIBTREEPATH_TEMPLATE.replace("__AAG__","") if calMode=="stdbunch" else CALIBTREEPATH_TEMPLATE.replace("__AAG__","_Aag")

DQM_dir = 'AlCaReco/SiStripGains'
if calMode == 'aagbunch':
    DQM_dir += 'AAG'

sys.stdout.write((
    "\n\nGain payload computing configuration:\n"
    "    firstRun = %s\n"
    "    lastRun  = %s\n"
    "    usePCL   = %s\n"
    "    calMode  = %s\n"
    "    DQM_dir  = %s\n\n"
    ""%(str(firstRun), str(lastRun), str(usePCL), calMode, DQM_dir)
))

scriptDir = os.getcwd()
