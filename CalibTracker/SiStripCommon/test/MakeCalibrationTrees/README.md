# Automated submission of jobs for the production of Calibration Trees

## Quick start
### Before running the script:
Make sure that the following conditions are satisfied:
1. You have a valid VOMS proxy.
2. The shell variable `X509_USER_PROXY` points to this proxy file, preferrably in your home directory.
3. You are running from a CMSSW environment.

### Using the submission script:
The production of Calibration Trees (CTs) for a given dataset and/or run interval can be launched using the following command:
```
python3 SubmitJobs.py
```
Optional arguments include:
- `--dryrun`: The HTCondor submission directory is created but the jobs are not launched. They can still be launched afterwards using `condor_submit`.
- `--verbose`: Verbosity flag: greatly increases the printed output. Useful for debugging e.g. DAS query failures.

## Configuration
The production can be configured by making changes to [Config.py](./submitCalibTree/Config.py). The variables which can be safely edited are given below:
| Variable      | Description |
| ----------- | ----------- |
| `dataset_path` | DAS path to dataset. Can be a pattern with wildcards, for example `/StreamExpress/Run2023*-SiStripCalMinBias__AAG__-Express-v*/ALCARECO`. Make sure that the `__AAG__` placeholder is placed correctly to allow the production to run over AAG datasets aswell.|
| `CASTOR_dir`   | Path to the directory where the produced CTs will be staged, for example `/store/group/dpg_tracker_strip/comm_tracker/Strip/Calibration/calibrationtree/GR18__AAG__` |
| `first_run` | Specifies the first run that should be processed. |
| `last_run` | Specifies the last run thath should be processed. Can use `-1` to indicate running over the full dataset. |
| `collection` | The collection to be used for the production of CTs, for example `ALCARECOSiStripCalMinBias__AAG__`. |
| `global_tag` | The global tag which provides the conditions of the data taking, for example `"130X_dataRun3_Express_v2` |
| `mail_address` | The mail adress of the user. An email containing the list of runs to be processed will be sent as soon as the production starts. |
| `n_files_per_job` | OPTIONAL: specifies the maximum number of files that can be processed by a single job. Default is 25. |
| `CMSSW_dir` | OPTIONAL: in case another CMSSW environment needs to be used, the path can be provided here. |
