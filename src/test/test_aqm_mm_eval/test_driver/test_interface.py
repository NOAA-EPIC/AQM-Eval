import logging
from pathlib import Path

import pytest
import yaml

from aqm_eval.aqm_mm_eval.driver.interface import SRWInterface
from aqm_eval.logging_aqm_eval import LOGGER


@pytest.fixture(autouse=True)
def config_path_user(tmp_path: Path) -> Path:
    yaml_content = {
        "metadata": {"description": "config for SRW-AQM, AQM_NA_13km, AEROMMA field campaign"},
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
        "task_mm_pre_chem_eval": {"MM_OUTPUT_DIR": None, "MM_EVALS": ["chem"]},
    }

    yaml_path = tmp_path / "config.yaml"
    with open(yaml_path, "w") as f:
        yaml.dump(yaml_content, f)

    return yaml_path


@pytest.fixture(autouse=True)
def config_path_rocoto(tmp_path: Path) -> Path:
    yaml_content = {"foo": "bar", "foo2": {"second": "baz"}}

    yaml_path = tmp_path / "rocoto_defns.yaml"
    with open(yaml_path, "w") as f:
        yaml.dump(yaml_content, f)

    return yaml_path


@pytest.fixture
def srw_interface(tmp_path) -> SRWInterface:
    return SRWInterface(expt_dir=tmp_path)


class TestSRWInterface:
    def test_init_path_happy(self, srw_interface: SRWInterface) -> None:
        LOGGER(srw_interface, level=logging.DEBUG)
        assert True

    def test_find_nested_key_happy_second_yaml(self, srw_interface: SRWInterface) -> None:
        actual = srw_interface.find_nested_key(("foo2", "second"))
        assert actual == "baz"

    def test_find_nested_key_sad(self, srw_interface: SRWInterface) -> None:
        with pytest.raises(KeyError):
            srw_interface.find_nested_key(("fail", "badly"))

    def test_find_nested_key_sad_no_child(self, srw_interface: SRWInterface) -> None:
        with pytest.raises(TypeError):
            srw_interface.find_nested_key(("foo", "bar"))
