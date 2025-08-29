import logging
from functools import cached_property
from pathlib import Path
from typing import Annotated, Any

import yaml
from pydantic import BaseModel, computed_field, BeforeValidator

from aqm_eval.logging_aqm_eval import LOGGER


def _format_path_existing_(value: Path | str) -> Path:
    ret = Path(value)
    if not ret.exists():
        raise ValueError(f"path does not exist: {ret}")
    return ret

PathExisting = Annotated[Path, BeforeValidator(_format_path_existing_)]

class SRWInterface(BaseModel):
    model_config = {"frozen": True}

    expt_dir: PathExisting

    @computed_field
    def config_path_user(self) -> PathExisting:
        return self.expt_dir / "config.yaml"

    @computed_field
    def config_path_rocoto(self) -> PathExisting:
        return self.expt_dir / "rocoto_defns.yaml"

    @computed_field
    def date_first_cycle(self) -> str:
        return self.find_nested_key(("workflow", "DATE_FIRST_CYCL"))

    @computed_field
    def mm_output_dir(self) -> PathExisting:
        config_path = self.find_nested_key(("task_mm_pre_chem_eval", "MM_OUTPUT_DIR"))
        if config_path is None:
            config_path = self.expt_dir / "mm_output"
        if not config_path.exists():
            config_path.mkdir(exist_ok=True, parents=True)
        return config_path

    @cached_property
    def yaml_data(self) -> dict[Path, dict[str, Any]]:
        """Cache loaded YAML data from config files."""
        data = {}
        for yaml_path in self.get_yaml_paths():
            with open(yaml_path, 'r') as f:
                data[yaml_path] = yaml.safe_load(f)
        return data

    def find_nested_key(self, key_tuple: tuple[str, ...]) -> Any:
        """Find a nested key in the YAML dictionaries using a tuple of string keys.

        Args:
            key_tuple: Tuple of strings representing nested dictionary keys

        Returns:
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


class MMEvalContext(BaseModel):
    iface: SRWInterface
