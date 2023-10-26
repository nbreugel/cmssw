# Automated submission of jobs for the production of Calibration Trees

## Quick start
### Before running the script:
Make sure that the following conditions are satisfied:
1. You have a valid VOMS proxy.
2. The shell variable `X509_USER_PROXY` points to this proxy file, preferrably in your home directory.
3. You are running from a CMSSW environment.

### Using the submission script:
The production of Calibration Trees (CTs) can be launched using the following command:
```
./SubmitJobs.py [--dryrun] [--verbose]
```
Optional arguments include:
- `--dryrun`: The HTCondor submission directory is created but the jobs are not launched. They can still be launched afterwards using `condor_submit`.
- `--verbose`: Verbosity flag: greatly increases the printed output. Useful for debugging e.g. DAS query failures.

## Configuration
The production can be configured by making changes to [submitCalibTree/Config.py](./submitCalibTree/Config.py). The variables which can be safely edited are given below:
| Variable      | Description |
| -----------		 | ----------- |
| `dataset_path`	 | DAS path to dataset. Can be a pattern with wildcards, for example `/StreamExpress/Run2023*-SiStripCalMinBias__AAG__-Express-v*/ALCARECO`. Make sure the `__AAG__` placeholder is placed correctly to allow the production to run over AAG datasets aswell. |
| `CASTOR_dir`		 | Path to the directory where the produced CTs will be staged, for example `/store/group/dpg_tracker_strip/comm_tracker/Strip/Calibration/calibrationtree/GR18__AAG__`. CTs produced from AAG datasets would in this case be staged in `GR18_Aag`. These directories need to have been created before the production is launched. |
| `first_run`		 | Specifies the first run that should be processed. |
| `last_run`		 | Specifies the last run that should be processed. Can use `-1` to indicate running over the full dataset. |
| `collection`		 | The collection to be used for the production of CTs, for example `ALCARECOSiStripCalMinBias__AAG__`. |
| `global_tag`		 | The global tag which provides the conditions of the data taking, for example `"130X_dataRun3_Express_v2`. |
| `mail_address`	 | (optional) The mail adress of the user. An email containing the list of runs to be processed will be sent as soon as the production starts. |
| `n_files_per_job`	 | (optional) Specifies the maximum number of files that can be processed by a single job. Default is 25. |
| `CMSSW_dir`		 | (optional) In case another CMSSW environment needs to be used, the path can be provided here. |
