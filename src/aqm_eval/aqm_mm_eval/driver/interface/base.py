from abc import ABC, abstractmethod
from functools import cached_property
from pathlib import Path

import yaml
from jinja2 import Environment, FileSystemLoader, StrictUndefined
from pydantic import BaseModel, computed_field

from aqm_eval.aqm_mm_eval.driver.helpers import PathExisting
from aqm_eval.aqm_mm_eval.driver.model import Model
from aqm_eval.aqm_mm_eval.driver.package import ChemEvalPackage, TaskKey
from aqm_eval.logging_aqm_eval import LOGGER


class AbstractInterface(ABC, BaseModel):
    model_config = {"frozen": True}

    @abstractmethod
    @computed_field
    @property
    def cartopy_data_dir(self) -> PathExisting: ...

    @abstractmethod
    @cached_property
    def mm_packages(self) -> tuple[ChemEvalPackage, ...]: ...

    @abstractmethod
    @cached_property
    def mm_models(self) -> tuple[Model, ...]: ...

    @abstractmethod
    @computed_field
    @property
    def mm_run_dir(self) -> PathExisting: ...

    @computed_field
    @property
    def template_dir(self) -> PathExisting:
        return (Path(__file__).parent.parent.parent / "yaml_template").absolute().resolve()

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

            cfg = {"iface": self, "mm_tasks": tuple([ii.value for ii in package.tasks])}

            namelist_config_str = env.get_template(package.namelist_template).render(cfg)
            namelist_config = yaml.safe_load(namelist_config_str)
            with open(package_run_dir / "namelist.yaml", "w") as f:
                f.write(namelist_config_str)

            # tdk:last:rm
            # try:
            #     with open(r"C:\Users\bkozi\Dropbox\dtmp\namelist.yaml", "w") as f:
            #         f.write(namelist_config_str)
            # except:
            #     pass

            assert isinstance(cfg["mm_tasks"], tuple)
            for task in cfg["mm_tasks"]:
                match task:
                    case TaskKey.SCORECARD_RMSE:
                        namelist_config["scorecard_eval_method"] = '"RMSE"'
                    case TaskKey.SCORECARD_IOA:
                        namelist_config["scorecard_eval_method"] = '"IOA"'
                    case TaskKey.SCORECARD_NMB:
                        namelist_config["scorecard_eval_method"] = '"NMB"'
                    case TaskKey.SCORECARD_NME:
                        namelist_config["scorecard_eval_method"] = '"NME"'

                LOGGER(f"{task=}")
                template = env.get_template(f"template_{task}.j2")
                LOGGER(f"{template=}")
                config_yaml = template.render(**namelist_config)
                curr_control_path = package_run_dir / f"control_{task}.yaml"
                LOGGER(f"{curr_control_path=}")
                with open(curr_control_path, "w") as f:
                    f.write(config_yaml)
