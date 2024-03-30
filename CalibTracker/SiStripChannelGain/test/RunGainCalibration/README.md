# RunGainCalibration
Collection of scripts to run gain calibration on PCL files or on pre-produced calibration trees.

## Quick start:
In order to run on PCL files from the dataset pattern specified within the script, the following command can be used:
```shell
./automatic_RunOnCalibrationTree.py -f FIRSTRUN -l LASTRUN
```
where the runs used in the calibration are constrained to `[FIRSTRUN, LASTRUN]`.

## Arguments:
| Argument           | Default    | Description                                                                    |
|:-------------------|:-----------|:-------------------------------------------------------------------------------|
| `-f`, `--firstRun` | -1         | Lower limit for the range of runs that will be processed from the PCL datasets |
| `-l`, `--lastRun`  | -1         | Upper limit for run range.                                                     |
| `-m`, `--mode`     | `StdBunch` | Statistics type to use. Can be either 'StdBunch' or 'AagBunch'.                |
| `-p`, `--pcl`      | `False`    | Pass this argument to run on PCL dataset instead of calibration trees.         |


