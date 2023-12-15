#!/usr/bin/env python3
import os
from submitCalibTree.DasWrapper import DasWrapper
from submitCalibTree.Config import Configuration
from submitCalibTree.PrintHelpers import printSkipped, printIncluded

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
                run_meets_condition = run >= self.config.first_run and run <= self.config.last_run
                if self.config.use_run_list:
                    run_meets_condition = run in self.config.runs_to_process
                    
                if run_meets_condition:
                    print("Checking run " + str(run) + "... ", end="", flush=True)
                    run_n_events = self.das_wrapper.getNumberOfEvents(dataset, run)
                    if run_n_events < self.run_n_events_threshold:
                        printSkipped()
                        print("Not enough events (%i events)" % run_n_events)
                    else:
                        run_n_files = self.das_wrapper.getNumberOfFiles(dataset, run)
                        run_info = (run, run_n_events, run_n_files)
                        if run_n_files > 0:
                            printIncluded()
                            print("%i events, %i files" % (run_n_events, run_n_files))
                            self.distributeJobs(dataset, run, run_n_files)
                            
                            if run > last_run_processed:
                                last_run_processed = run
                        else:
                            print("No files found. (%s events, %s files)" % (run_n_events, run_n_files))

        if self.n_jobs == 0:
            print("---> No runs were found for %s bunch!" % ("STD" if not self.config.use_AAG else "AAG"))
            return last_run_processed
                            
        with open(self.config.condor_dir + "/input_numbers.txt", "w") as input_numbers:
            input_numbers.write(self.run_and_job_numbers)

        self.generateCondorScript()
        print("Done generating HTCondor submission directory.")

        if self.config.use_run_list:
            print("")
            n_runs_run_list = len(self.config.runs_to_process)
            n_runs_launched = len(self.launched_runs_dict.keys())
            fraction = n_runs_launched / n_runs_run_list
            print(f"Out of the {n_runs_run_list} runs supplied, {n_runs_launched} runs will be processed ({100*fraction:.2f}%)")
            if n_runs_run_list != n_runs_launched:
                missing_runs = [int(run) for run in self.launched_runs_dict.keys() if int(run) not in self.config.runs_to_process]
                print("The following runs are missing:")
                print(missing_runs)
            

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
        os.system("condor_submit -batch-name CalibTrees%s %s/condor_submission.submit" % (self.config.condor_dir, "" if not self.config.use_AAG else "_Aag"))

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
