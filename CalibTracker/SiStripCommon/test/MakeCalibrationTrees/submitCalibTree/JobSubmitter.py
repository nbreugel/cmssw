#!/usr/bin/env python3
import os
from submitCalibTree.DasWrapper import DasWrapper
from submitCalibTree.Config import Configuration

class JobSubmitter:
    run_n_events_threshold = 250

    def __init__(self, config_):
        self.config = config_
        self.das_wrapper = DasWrapper(self.config)
        self.job_number = 0
        self.run_and_job_numbers = ""
        self.launched_runs_dict = {}
        
    def generateJobs(self):
        print("Generating HTCondor submission directory and submission script...")
        dataset_list       = self.das_wrapper.getDatasetFromPattern(self.config.dataset_path)
        last_run_processed = self.config.first_run
        condor_dir         = self.config.condor_dir

        if os.path.isdir(condor_dir):
            self.cleanSubmissionDir()
        else:
            os.system("mkdir " + condor_dir)
        
        self.n_jobs = 0

        for dataset in dataset_list:
            dataset_run_list = self.das_wrapper.getRunsFromDataset(dataset)
            for run in dataset_run_list:
                if run >= self.config.first_run and run <= self.config.last_run:
                    print("Checking run " + str(run) + "...")
                    run_n_events = self.das_wrapper.getNumberOfEvents(dataset, run)
                    if run_n_events < self.run_n_events_threshold:
                        print("Run %i skipped: Not enough events (%i < %i).\n" % (run,
                                                                                run_n_events,
                                                                                self.run_n_events_threshold))
                    else:
                        run_n_files = self.das_wrapper.getNumberOfFiles(dataset, run)
                        run_info = (run, run_n_events, run_n_files)
                        if run_n_files > 0:
                            print("Run %i will be processed! (%i events, %i files)\n" % run_info)
                            self.distributeJobs(dataset, run, run_n_files)
                            
                            if run > last_run_processed:
                                last_run_processed = run
                        else:
                            print("Run %i skipped: No files found. (%s events, %s files)\n" % run_info)

        if self.n_jobs == 0:
            print("---> No runs were found for %s bunch!" % ("STD" if not self.config.use_AAG else "AAG"))
            return last_run_processed
                            
        with open(self.config.condor_dir + "/input_numbers.txt", "w") as input_numbers:
            input_numbers.write(self.run_and_job_numbers)

        self.generateCondorScript()
        print("Done generating HTCondor submission directory.")

        return last_run_processed
                
    def distributeJobs(self, dataset, run, run_n_files):
        files_to_process = self.das_wrapper.getFilesFromDataset(dataset, run)
        files_in_job = []
        job_number = 1
        
        for file_name in files_to_process:
            if not file_name.startswith("/store"): continue
            if len(files_in_job) < self.config.n_files_per_job:
                files_in_job += [file_name]
            else:
                self.setupJob(run, dataset, files_in_job, job_number)
                job_number += 1
                files_in_job = []
                
        self.setupJob(run, dataset, files_in_job, job_number)

        self.launched_runs_dict[str(run)] = {}
        self.launched_runs_dict[str(run)]["n_jobs"] = job_number
        self.launched_runs_dict[str(run)]["n_files"] = len(files_to_process)

    def setupJob(self, run, dataset, files_to_process, job_number):
        self.n_jobs += 1
        job_dir = self.config.condor_dir + "/job_%i_%i" % (run, job_number)
        os.system("mkdir " + job_dir)

        files_to_process_string = ",".join(files_to_process)
        py_command = "python3 %s/submitCalibTree/runJob.py -r %i -f %s -n %i" % (self.config.working_dir,
                                                                                 run,
                                                                                 files_to_process_string,
                                                                                 job_number)
        if self.config.use_AAG:
            py_command += " -a"

        launch_script_path = job_dir + "/launch.sh"
            
        with open(launch_script_path, "w") as launch:
            launch.write("#!/bin/bash\n")
            launch.write(self.config.init_env_command.replace(";", ";\n"))
            launch.write(py_command + "\n")

        os.system("chmod u+x " + launch_script_path)
        
        self.run_and_job_numbers += "%i,%i" % (run, job_number) + "\n"
        
    def cleanSubmissionDir(self):
        print("---> Found old HTCondor submission directory. Removing the contents...")
        os.system("rm -rf " + self.config.condor_dir + "/job_*")
        os.system("rm -f " + self.config.condor_dir + "/condor_submission.submit")
        os.system("rm -f " + self.config.condor_dir + "/input_numbers.txt")
        print("---> Done!\n")

    def launchJobs(self):
        os.system("condor_submit %s/condor_submission.submit -batch-name CalibTrees%s" % (self.config.condor_dir, "" if not self.config.use_AAG else "_Aag"))

    def generateCondorScript(self):
        with open(self.config.condor_dir + "/condor_submission.submit", "w") as submit:
            submit.write("Universe = vanilla\n")
            submit.write("Executable = %s/job_$(run)_$(number)/launch.sh\n" % self.config.condor_dir)
            submit.write("Arguments = \n")
            submit.write("\n")
            submit.write("Error = %s/job_$(run)_$(number)/job.err\n" % self.config.condor_dir)
            submit.write("Output = %s/job_$(run)_$(number)/job.out\n" % self.config.condor_dir)
            submit.write("Log = %s/job_$(run)_$(number)/job.log\n" % self.config.condor_dir)
            submit.write("")
            submit.write("transfer_input_files = %s/produceCalibrationTree_template_cfg.py\n" % self.config.working_dir)
            # if self.config.mail_address != "": # sends mail for each failed job
            #     submit.write("notify_user = %s\n" % self.config.mail_address)
            #     submit.write("notification = error\n")
            submit.write("\n")
            submit.write("+JobFlavour = \"workday\"\n")
            submit.write("queue run,number from %s/input_numbers.txt" % self.config.condor_dir)

    
if __name__ == "__main__":
    c = Configuration(debug_mode=True)
    print(c)
    j = JobSubmitter(c)
