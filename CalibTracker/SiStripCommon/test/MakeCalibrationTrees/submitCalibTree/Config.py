#!/usr/bin/env python3
import os
import subprocess
import time


class Configuration:
    # Variables to be edited by user:
    # Heavy Ions: '/StreamHIExpress/HIRun2023A-SiStripCalMinBias-Express-v2/ALCARECO'
    #            (make sure you are using the correct HLT paths, you may need to add some "HI" keywords)
    #
    # 2023: '/StreamExpress/Run2023*-SiStripCalMinBias__AAG__-Express-v*/ALCARECO'
    # 2022: '/StreamExpress/Run2022*-SiStripCalMinBias__AAG__-Express-v*/ALCARECO'
    # 
    # global tag 2023 express: "130X_dataRun3_Express_v2"
    # global tag 2022 express: "124X_dataRun3_Express_v9"
    dataset_path         = '/StreamExpress/Run2024*-SiStripCalMinBias__AAG__-Express-v*/ALCARECO'
    CASTOR_dir           = '/eos/cms/store/group/dpg_tracker_strip/comm_tracker/Strip/Calibration/calibrationtree/GR24_900GeV__AAG__'
    first_run            = 378236  # default -1
    last_run             = 999999  # default 999999
    collection           = "ALCARECOSiStripCalMinBias__AAG__"
    global_tag           = "141X_dataRun3_Express_v2"
    mail_address          = "nordin.breugelmans@cern.ch"

    # Optional variables:
    n_files_per_job      = 25
    CMSSW_dir            = ''

    # Could break script if modified:
    das_client_command   = "dasgoclient "
    eos_ls_command       = "eos ls "
    proxy_file_path      = "/afs/cern.ch/user/%s/%s/x509up_u%s" % (os.environ["USER"][0],
                                                                   os.environ["USER"],
                                                                   os.geteuid())
    init_env_command     = ""
    working_dir          = ''
    
    relaunch_runs_list = []
    launched_runs_list = []

    def __init__(self, use_AAG = False, debug_mode = False, use_run_list=""):
        self.use_AAG      = use_AAG
        self.debug_mode   = debug_mode
        self.use_run_list = use_run_list != ""
        if self.use_run_list:
            self.runs_to_process = self.readRunList(use_run_list)
        
        self.CASTOR_dir   = self.CASTOR_dir.replace("__AAG__", "_Aag" if self.use_AAG else "")
        self.dataset_path = self.dataset_path.replace("__AAG__", "AAG" if self.use_AAG else "")
        self.collection   = self.collection.replace("__AAG__", "AAG" if self.use_AAG else "")
        
        self.integrity    = self.checkIntegrity()
        self.condor_dir   = self.working_dir + "/condor_submission__AAG__".replace("__AAG__", "_AAG" if self.use_AAG else "")

        self.checkProxy()
        self.init_env_command += "cd " + self.CMSSW_dir + ";"
        self.init_env_command += "eval `scram runtime -sh`;"
        if self.proxy_file_path != '':
            self.init_env_command += "export X509_USER_PROXY=%s;" % self.proxy_file_path
        self.init_env_command += "cd - >/dev/null;"

    def checkIntegrity(self):
        config_is_good = True
        
        # Check if dataset path makes sense:
        split_dataset_path = self.dataset_path.split("/")
        if not len(split_dataset_path) == 4:
            config_is_good = False
            self.printWarning("Expected four slashes (/) in dataset path.")
        if not split_dataset_path[0] == '':
            config_is_good = False
            self.printWarning("Expected no characters before first slash (/).")
        if (not len(split_dataset_path[1]) > 0
            or not len(split_dataset_path[2]) > 0
            or not len(split_dataset_path[3]) > 0):
            config_is_good = False
            self.printWarning("Expected text between slashes (/).")
        if os.path.isdir(self.dataset_path):
            config_is_good = False
            self.printWarning("Dataset path cannot be an existing directory.")

        # Check if CMSSW path makes sense:
        if self.CMSSW_dir == '' or not os.path.isdir(self.CMSSW_dir):
            self.printDebug("CMSSW directory was not found. Directory will be set based on content of CMSSW_BASE variable.")
            if not "CMSSW_BASE" in os.environ:
                self.printWarning("CMSSW_BASE variable has not been set. Are you in a CMSSW environment?")
                # Figure out if we are running from CMSSW environment:
                # cmd = "eval `scram runtime -sh`;"
                # cmd += "echo $CMSSW_BASE;"
                # status, output = subprocess.getstatusoutput(cmd)
                
                config_is_good = False
            else:
                self.CMSSW_dir = os.environ["CMSSW_BASE"] + "/src"

        # Check if CASTOR path exists:
        dir_name = self.CASTOR_dir.split("/")[-1]
        command = self.eos_ls_command + self.CASTOR_dir[:-len(dir_name)]
        (status, output) = subprocess.getstatusoutput(command)
        if status or not dir_name in output:
            self.printWarning("CASTOR directory '%s' does not exist." % dir_name)
            self.printDebug("Command used: " + command)
            self.printDebug(output)
            config_is_good = False

        # Check if working dir makes sense:
        if self.working_dir == '' or os.path.isdir(self.working_dir):
            self.printDebug("Working directory not found/specified. Setting it to directory this script is run from.")
            self.working_dir = os.path.abspath(".")

        # Check if global tag is filled:
        if self.global_tag == "":
            self.printWarning("The global tag was not specified.")
            config_is_good = False

        # If we use a list of runs as input, check if it was loaded correctly:
        if self.use_run_list:
            if len(self.runs_to_process) == 0:
                self.printWarning(f"No runs were found in the given run list file.")
                config_is_good = False

        return config_is_good

    def checkProxy(self):
        proxy_is_good = False
                
        if not os.path.isfile(self.proxy_file_path):
            self.printWarning("No private proxy file to use. Can't run on data outside of CERN.")
        else:
            proxy_age   = int(time.time() - os.stat(self.proxy_file_path).st_mtime)
            proxy_age_h = int(proxy_age/3600)
            proxy_age_m = int(proxy_age/60) - 60*(int(proxy_age/3600))
            if proxy_age < 36000:
                # Proxy valid for 12hours --> Ignore files created more than 10h ago"
                proxy_is_good = True
            elif proxy_age >= 36000:
                self.printWarning("Proxy was created %ih and %i min ago." % (proxy_age_h,
                                                                           proxy_age_m))
                self.printWarning("Please refresh your proxy")
            elif proxy_age < 36000 and self.debug_mode:
                self.printDebug("Proxy was created %ih and %i min ago." % (proxy_age_h,
                                                                         proxy_age_m))

    def __str__(self):
        description  = "    First run       = %s\n" % self.first_run
        description += "    Last run        = %s\n" % self.last_run
        description += "    Use AAG         = %s\n" % self.use_AAG
        description += "    Dataset         = %s\n" % self.dataset_path
        description += "    Collection      = %s\n" % self.collection
        description += "    Global tag      = %s\n" % self.global_tag
        description += "    Proxy file path = %s\n" % self.proxy_file_path
        description += "    CMSSW           = %s\n" % self.CMSSW_dir
        description += "    CASTOR          = %s\n" % self.CASTOR_dir
        description += "    Working dir     = %s\n" % self.working_dir
        description += "    Files per job   = %s\n" % self.n_files_per_job
        description += "    Debug mode      = %s  " % ("True" if self.debug_mode else "False")
        return description

    def printWarning(self, message):
        print("\033[91m" + "CONFIG WARNING: " + message + "\033[0m")

    def printDebug(self, message):
        if self.debug_mode:
            print("\033[33m" + "CONFIG DEBUG: " + message + "\033[0m")

    def readRunList(self, path_to_run_list):
        if not os.path.isfile(path_to_run_list):
            self.printWarning(f"Could not find file containing runs: {path_to_run_list}")
            return []

        runs = []
        with open(path_to_run_list, "r") as runs_file:
            for line in runs_file.readlines():
                number = line.replace("\n", "")
                if number.isdigit():
                    runs += [int(number)]

         return runs
                
        

if __name__ == "__main__":
    c = Configuration(False, True)
    print("")
    print(c)
       
       
        
