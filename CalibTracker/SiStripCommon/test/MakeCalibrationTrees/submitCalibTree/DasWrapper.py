#!/usr/bin/env python3
import subprocess
import sys
import os

class DasWrapper:

    def __init__(self, config):
         self.das_client_command = config.das_client_command
         self.debug_mode         = config.debug_mode
         self.init_env_command   = config.init_env_command

    def getDatasetFromPattern(self, pattern):
        if not self.checkDatasetStructure(pattern):
            self.printError("Bad dataset pattern: " + pattern)
            return([])

        result = self.dasQuery("dataset dataset=%s" % pattern)
        dataset_list = []
        for line in result:
            self.printDebug(line)
            if self.checkDatasetStructure(line):
                dataset_list.append(line)
                
        return dataset_list

    def checkDatasetStructure(self, dataset):
        dataset_is_good = True
        
        split_dataset_path = dataset.split("/")
        if not len(split_dataset_path) == 4:
            dataset_is_good = False
            self.printDebug("Expected four slashes (/) in dataset path: " + dataset)
        elif not split_dataset_path[0] == '':
            dataset_is_good = False
            self.printDebug("Expected no characters before first slash (/): " + dataset)
        elif (not len(split_dataset_path[1]) > 0
            or not len(split_dataset_path[2]) > 0
            or not len(split_dataset_path[3]) > 0):
            dataset_is_good = False
            self.printDebug("Expected text between slashes (/): " + dataset)
        elif os.path.isdir(dataset):
            dataset_is_good = False
            self.printDebug("Dataset path cannot be an existing directory" + dataset)

        return dataset_is_good

    def getRunsFromDataset(self, dataset):
        result = self.dasQuery("run dataset=%s" % dataset)
        runs = []
        for line in result:
            if line.isdigit():
                if len(line) == 6:
                    runs.append(int(line))
        runs.sort()
        return runs

    def getFilesFromDataset(self, dataset, run):
        return self.dasQuery("file dataset=%s run=%s" % (dataset, run))

    def getNumberOfEvents(self, dataset, run):
        das_n_events = self.dasQuery("summary dataset=%s run=%s | grep summary.nevents" % (dataset, run))
        das_n_events = das_n_events[-1].replace(" ", "")

        if not das_n_events.isdigit():
            self.printError("Invalid number of events: " + das_n_events)
            return 0
        else:
            return int(das_n_events)

    def getNumberOfFiles(self, dataset, run):
        das_n_files = self.dasQuery("summary dataset=%s run=%s | grep summary.nfiles" % (dataset, run))
        das_n_files = das_n_files[-1].replace(" ","")
        if not das_n_files.isdigit():
            self.printError("Invalid number of files: " + das_n_files)
        else:
            return int(das_n_files)

    def dasQuery(self, query):
        command = self.das_client_command + " --limit=9999 --query=\"%s\"" % query
        self.printDebug("Using " + command)
        
        (status, output) = subprocess.getstatusoutput(self.init_env_command + command)
        
        if status != 0:
            self.printError(str(status) + " - " + output)
            sys.exit()

        return output.splitlines()
            

    def printDebug(self, message):
        if self.debug_mode:
            print("\033[33m" + "DEBUG: " + message + "\033[0m")

    def printError(self, message):
        print("\033[91m" + "DAS CLIENT ERROR: " + message + "\033[0m")
