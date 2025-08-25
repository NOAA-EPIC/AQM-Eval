# AQM-Eval
Scripts and utilities related to UFS-AQM evaluation and verification.

## Installation

```shell
git clone -b develop https://github.com/NOAA-EPIC/AQM-Eval
cd AQM-Eval
conda env create -f environment.yml
conda run -n aqm-eval pip install .

# Optionally run tests
conda run -n aqm-eval pytest src/tes
```

## _aqm-data-sync_ Utility

A command line utility to synchronize AQM datasets. Currently, tuned to specialized use cases related to Short-Range Weather App use cases. Only S3 download is supported.

### Usage

```shell
aqm-data-sync --help
```
