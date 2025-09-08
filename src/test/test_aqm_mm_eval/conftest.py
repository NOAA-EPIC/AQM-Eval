from pathlib import Path

import pytest
import yaml

from aqm_eval.aqm_mm_eval.driver.interface import SRWInterface


@pytest.fixture(params=[True, False], ids=lambda x: f"use_base_model={x}")
def use_base_model(request) -> bool:
    return request.param


@pytest.fixture
def expt_dir(tmp_path: Path, use_base_model: bool) -> Path:
    ret = tmp_path / f"use_base_model-{use_base_model}"
    ret.mkdir(exist_ok=False, parents=True)
    return ret


@pytest.fixture()
def config_path_user(expt_dir: Path, use_base_model: bool) -> Path:
    yaml_content = {
        "metadata": {
            "description": "config for SRW-AQM, AQM_NA_13km, AEROMMA field campaign"
        },
        "user": {"RUN_ENVIR": "community", "MACHINE": "GAEAC6", "ACCOUNT": "bil-fire8"},
        "workflow": {
            "USE_CRON_TO_RELAUNCH": True,
            "CRON_RELAUNCH_INTVL_MNTS": 3,
            "EXPT_SUBDIR": "aqm_AQMNA13km_AEROMMA",
            "PREDEF_GRID_NAME": "AQM_NA_13km",
            "CCPP_PHYS_SUITE": "FV3_GFS_v16",
            "DATE_FIRST_CYCL": "2023060112",
            "DATE_LAST_CYCL": "2023060212",
        },
        "task_melodies_monet_prep": {
            "MM_OUTPUT_DIR": None,
            "MM_EVAL_PACKAGES": ["chem"],
            "MM_OBS_AIRNOW_FN_TEMPLATE": "AirNow_20230601_20230701.nc",
            "MM_BASE_MODEL_EXPT_DIR": str(expt_dir) if use_base_model else None,
        },
    }
    yaml_path = expt_dir / "config.yaml"
    with open(yaml_path, "w") as f:
        yaml.dump(yaml_content, f)
    return yaml_path


@pytest.fixture()
def config_path_rocoto(expt_dir: Path) -> Path:
    yaml_content = {"foo": "bar", "foo2": {"second": "baz"}}
    yaml_path = expt_dir / "rocoto_defns.yaml"
    with open(yaml_path, "w") as f:
        yaml.dump(yaml_content, f)
    return yaml_path

@pytest.fixture()
def config_path_var_defns(tmp_path: Path, expt_dir: Path) -> Path:
    path = tmp_path / "NaturalEarth"
    path.mkdir(exist_ok=True, parents=True)
    yaml_content = {"platform": {"FIXshp": str(path)}}
    yaml_path = expt_dir / "var_defns.yaml"
    with open(yaml_path, "w") as f:
        yaml.dump(yaml_content, f)
    return yaml_path


@pytest.fixture()
def dummy_dyn_files(expt_dir: Path) -> None:
    for dirname in ["2023060112", "2023060212"]:
        dyn_dir = expt_dir / dirname
        dyn_dir.mkdir(exist_ok=False, parents=False)
        for fhr in range(25):
            dyn_file = dyn_dir / f"dynf{fhr:03d}.nc"
            dyn_file.touch()


@pytest.fixture
def srw_interface(
    expt_dir: Path,
    config_path_user: Path,
    config_path_rocoto: Path,
        config_path_var_defns: Path,
    dummy_dyn_files: None,
) -> SRWInterface:
    return SRWInterface(expt_dir=expt_dir)
