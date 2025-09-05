import logging
from datetime import datetime
from functools import cached_property
from pathlib import Path
from typing import Any

import yaml
from jinja2 import Environment, FileSystemLoader, StrictUndefined
from pydantic import BaseModel, computed_field

from aqm_eval.aqm_mm_eval.driver.helpers import PathExisting
from aqm_eval.aqm_mm_eval.driver.model import Model, ModelRole
from aqm_eval.aqm_mm_eval.driver.package import PackageKey, ChemEvalPackage
from aqm_eval.logging_aqm_eval import LOGGER


def _convert_date_string_to_mm_(date_str: str) -> str:
    dt = datetime.strptime(date_str, "%Y%m%d%H")
    return dt.strftime("%Y-%m-%d-%H:00:00")


class SRWInterface(BaseModel):
    model_config = {"frozen": True}

    expt_dir: PathExisting

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
        return _convert_date_string_to_mm_(self.date_first_cycle_srw)

    @computed_field
    @property
    def date_last_cycle_mm(self) -> str:
        return _convert_date_string_to_mm_(self.date_last_cycle_srw)

    @computed_field
    @property
    def mm_output_dir(self) -> PathExisting:
        config_path = self.find_nested_key(
            ("task_melodies_monet_prep", "MM_OUTPUT_DIR")
        )
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
    def mm_package_keys(self) -> tuple[PackageKey, ...]:
        return tuple(
            [
                PackageKey(ii)
                for ii in self.find_nested_key(
                    ("task_melodies_monet_prep", "MM_EVAL_PACKAGES")
                )
            ]
        )

    # @computed_field
    # @property
    # def mm_eval_prefix(self) -> str:
    #     return self.find_nested_key(("task_melodies_monet_prep", "MM_EVAL_PREFIX"))

    @computed_field
    @property
    def mm_obs_airnow_fn_template(self) -> str:
        return self.find_nested_key(
            ("task_melodies_monet_prep", "MM_OBS_AIRNOW_FN_TEMPLATE")
        )

    @computed_field
    @property
    def link_simulation(self) -> tuple[str, ...]:
        return tuple(
            set(
                [
                    f"{str(ii.year)}*"
                    for ii in [self.datetime_first_cycl, self.datetime_last_cycl]
                ]
            )
        )

    @computed_field
    @property
    def link_alldays_path(self) -> PathExisting:
        ret = self.mm_run_dir / "Alldays"
        ret.mkdir(exist_ok=True, parents=True)
        return ret

    @computed_field
    @property
    def link_alldays_path_template(self) -> str:
        return str(self.link_alldays_path / "*.nc")

    @computed_field
    @property
    def template_dir(self) -> PathExisting:
        return (Path(__file__).parent.parent / "yaml_template").absolute().resolve()

    @cached_property
    def mm_packages(self) -> tuple[ChemEvalPackage, ...]:
        ret = []
        for package_key in self.mm_package_keys:
            match package_key:
                case PackageKey.CHEM:
                    klass = ChemEvalPackage
                case _:
                    raise ValueError(package_key)
            ret.append(klass(root_dir=self.mm_run_dir))
        return tuple(ret)

    @cached_property
    def datetime_first_cycl(self) -> datetime:
        return datetime.strptime(self.date_first_cycle_srw, "%Y%m%d%H")

    @cached_property
    def datetime_last_cycl(self) -> datetime:
        return datetime.strptime(self.date_last_cycle_srw, "%Y%m%d%H")

    @cached_property
    def yaml_data(self) -> dict[Path, dict[str, Any]]:
        """Cache loaded YAML data from config files."""
        data = {}
        for yaml_path in self.yaml_srw_config_paths:
            with open(yaml_path, "r") as f:
                data[yaml_path] = yaml.safe_load(f)
        return data

    @cached_property
    def yaml_srw_config_paths(self) -> tuple[PathExisting, ...]:
        return self.config_path_user, self.config_path_rocoto

    @cached_property
    def mm_models(self) -> tuple[Model, ...]:
        return (
            Model(
                expt_dir=self.expt_dir,
                label="eval_aqm",
                title="Eval AQM",
                prefix="eval",
                role=ModelRole.EVAL,
                dyn_file_template=("dynf*.nc",),
                cycle_dir_template=self.link_simulation,
            ),
        )

    @cached_property
    def mm_model_labels(self) -> list[str]:
        return [mm_model.label for mm_model in self.mm_models]

    @cached_property
    def mm_model_titles_j2(self) -> str:
        return ", ".join([f'"{ii.title}"' for ii in self.mm_models])

    def create_control_configs(self) -> None:
        for package in self.mm_packages:
            iface = self
            searchpath = iface.template_dir
            LOGGER(f"creating MM evaluation templates. {searchpath=}")
            package_run_dir = package.run_dir
            LOGGER(f"{package_run_dir=}")
            if not package_run_dir.exists():
                LOGGER(f"{package_run_dir=} does not exist. creating.")
                package_run_dir.mkdir(exist_ok=True, parents=True)
            env = Environment(
                loader=FileSystemLoader(searchpath=searchpath),
                undefined=StrictUndefined,
            )

            #tdk:rm
            # cfg = iface.model_dump()
            # cfg["mm_tasks"] = [ii.value for ii in package.tasks]
            # cfg["mm_models"] = self.mm_models
            # cfg["mm_model_labels"] = self.mm_model_labels
            # cfg["mm_model_titles_j2"] = self.mm_model_titles_j2

            cfg = {"iface": self, "mm_tasks": [ii.value for ii in package.tasks]}
            namelist_config_str = env.get_template(package.namelist_template).render(
                cfg
            )
            namelist_config = yaml.safe_load(namelist_config_str)
            with open(package_run_dir / "namelist.yaml", "w") as f:
                f.write(namelist_config_str)

            #tdk:rm
            try:
                with open(r"C:\Users\bkozi\Dropbox\dtmp\namelist.yaml", "w") as f:
                    f.write(namelist_config_str)
            except:
                pass

            for task in cfg["mm_tasks"]:
                LOGGER(f"{task=}")
                template = env.get_template(f"template_{task}.j2")
                LOGGER(f"{template=}")
                config_yaml = template.render(**namelist_config)
                curr_control_path = package_run_dir / f"control_{task}.yaml"
                LOGGER(f"{curr_control_path=}")
                with open(curr_control_path, "w") as f:
                    f.write(config_yaml)

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
                LOGGER(
                    f"unexpected error: {key_tuple=}, {type(current)=}",
                    level=logging.ERROR,
                )
                raise
        raise KeyError(
            f"{key_tuple=} not found in any YAML files: {self.yaml_data.keys()}"
        )
