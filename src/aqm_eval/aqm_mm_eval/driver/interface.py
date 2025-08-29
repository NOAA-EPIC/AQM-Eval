import logging
from datetime import datetime
from enum import StrEnum, unique
from functools import cached_property
from pathlib import Path
from typing import Annotated, Any

import yaml
from pydantic import BaseModel, BeforeValidator, computed_field

from aqm_eval.aqm_mm_eval.driver.helpers import create_symlinks
from aqm_eval.logging_aqm_eval import LOGGER


def _format_path_existing_(value: Path | str) -> Path:
    ret = Path(value)
    if not ret.exists():
        raise ValueError(f"path does not exist: {ret}")
    return ret


def _convert_date_string_to_mm_(date_str: str) -> str:
    dt = datetime.strptime(date_str, '%Y%m%d%H')
    return dt.strftime('%Y-%m-%d-%H:00:00')


PathExisting = Annotated[Path, BeforeValidator(_format_path_existing_)]


@unique
class EvalType(StrEnum):
    CHEM = "chem"
    MET = "met"
    AQS_PM25 = "aqs_pm25"
    VOCS = "vocs"


class SRWInterface(BaseModel):
    model_config = {"frozen": True}

    expt_dir: PathExisting

    dyn_file_template: str = "dynf*.nc"

    @computed_field
    @property
    def config_path_user(self) -> PathExisting:
        return self.expt_dir / "config.yaml"

    @computed_field
    @property
    def config_path_rocoto(self) -> PathExisting:
        return self.expt_dir / "rocoto_defns.yaml"

    @computed_field
    @property
    def date_first_cycle_srw(self) -> str:
        return self.find_nested_key(("workflow", "DATE_FIRST_CYCL"))

    @computed_field
    @property
    def date_last_cycle_srw(self) -> str:
        return self.find_nested_key(("workflow", "DATE_LAST_CYCL"))

    @computed_field
    @property
    def date_first_cycle_mm(self) -> str:
        return _convert_date_string_to_mm_(
            self.date_first_cycle_srw
        )

    @computed_field
    @property
    def date_last_cycle_mm(self) -> str:
        return _convert_date_string_to_mm_(
            self.date_last_cycle_srw
        )

    @computed_field
    @property
    def mm_output_dir(self) -> PathExisting:
        config_path = self.find_nested_key(("task_mm_pre_chem_eval", "MM_OUTPUT_DIR"))
        if config_path is None:
            config_path = self.expt_dir / "mm_output"
        if not config_path.exists():
            config_path.mkdir(exist_ok=True, parents=True)
        return config_path

    @computed_field
    @property
    def mm_run_dir(self) -> PathExisting:
        ret = self.expt_dir / "mm_run"
        ret.mkdir(exist_ok=True, parents=True)
        return ret

    @computed_field
    @property
    def mm_eval_types(self) -> tuple[EvalType, ...]:
        return tuple([EvalType(ii) for ii in self.find_nested_key(("task_mm_pre_chem_eval", "MM_EVAL_TYPES"))])

    @computed_field
    @property
    def mm_eval_prefix(self) -> str:
        return self.find_nested_key(("task_mm_pre_chem_eval", "MM_EVAL_PREFIX"))

    @computed_field
    @property
    def link_simulation(self) -> tuple[str, ...]:
        return tuple(set([f"{str(ii.year)}*" for ii in [self.datetime_first_cycl, self.datetime_last_cycl]]))

    @computed_field
    @property
    def link_alldays_path(self) -> PathExisting:
        ret = self.mm_run_dir / "Alldays"
        ret.mkdir(exist_ok=True, parents=True)
        return ret

    @cached_property
    def datetime_first_cycl(self) -> datetime:
        return datetime.strptime(self.date_first_cycle_srw, '%Y%m%d%H')

    @cached_property
    def datetime_last_cycl(self) -> datetime:
        return datetime.strptime(self.date_last_cycle_srw, "%Y%m%d%H")

    @cached_property
    def yaml_data(self) -> dict[Path, dict[str, Any]]:
        """Cache loaded YAML data from config files."""
        data = {}
        for yaml_path in self.get_yaml_paths():
            with open(yaml_path, "r") as f:
                data[yaml_path] = yaml.safe_load(f)
        return data

    def find_nested_key(self, key_tuple: tuple[str, ...]) -> Any:
        """Find a nested key in the YAML dictionaries using a tuple of string keys.

        Args:
            key_tuple: Tuple of strings representing nested dictionary keys

        Returns
        -------
            The value found at the nested key location
        """
        for yaml_path, yaml_dict in self.yaml_data.items():
            current = yaml_dict
            try:
                for key in key_tuple:
                    current = current[key]
                return current
            except KeyError:
                continue
            except:
                LOGGER(f"unexpected error: {key_tuple=}, {type(current)=}", level=logging.ERROR)
                raise
        raise KeyError(f"{key_tuple=} not found in any YAML files: {self.yaml_data.keys()}")

    def get_yaml_paths(self) -> tuple[PathExisting, ...]:
        return self.config_path_user, self.config_path_rocoto


class MMEvalRunner(BaseModel):
    model_config = {"frozen": True}

    iface: SRWInterface

    def initialize(self) -> None:
        LOGGER("initializing MMEvalRunner")
        LOGGER(f"{self.iface=}")
        LOGGER("creating symlinks")
        create_symlinks(self.iface.expt_dir, self.iface.mm_run_dir, self.iface.mm_eval_prefix, self.iface.link_alldays_path, self.iface.dyn_file_template)

    def run(self) -> None:
        tdk

    def finalize(self) -> None:
        tdk
