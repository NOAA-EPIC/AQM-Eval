from abc import ABC, abstractmethod
from functools import cached_property
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, StrictUndefined
from pydantic import BaseModel, computed_field

from aqm_eval.logging_aqm_eval import LOGGER
from aqm_eval.mm_eval.driver.helpers import PathExisting
from aqm_eval.mm_eval.driver.model import Model
from aqm_eval.mm_eval.driver.package import ChemEvalPackage


class AbstractContext(ABC, BaseModel):
    model_config = {"frozen": True}

    @computed_field
    @property
    @abstractmethod
    def cartopy_data_dir(self) -> PathExisting: ...

    @computed_field
    @property
    @abstractmethod
    def date_first_cycle_mm(self) -> str: ...

    # @abstractmethod
    @computed_field
    @property
    @abstractmethod
    def date_last_cycle_mm(self) -> str: ...

    @cached_property
    @abstractmethod
    def mm_packages(self) -> tuple[ChemEvalPackage, ...]: ...

    @cached_property
    @abstractmethod
    def mm_models(self) -> tuple[Model, ...]: ...

    @cached_property
    def mm_model_labels(self) -> list[str]:
        return [mm_model.label for mm_model in self.mm_models]

    @cached_property
    def mm_model_titles_j2(self) -> str:
        return ", ".join([f'"{ii.title}"' for ii in self.mm_models])

    @computed_field
    @property
    @abstractmethod
    def mm_obs_airnow_fn_template(self) -> str: ...

    @computed_field
    @property
    @abstractmethod
    def mm_output_dir(self) -> PathExisting: ...

    @computed_field
    @property
    @abstractmethod
    def mm_run_dir(self) -> PathExisting: ...

    @computed_field
    @property
    def template_dir(self) -> PathExisting:
        return (Path(__file__).parent.parent.parent / "yaml_template").absolute().resolve()

    @cached_property
    def j2_env(self) -> Environment:
        searchpath = self.template_dir
        LOGGER(f"creating J2 environment {self.template_dir=}")
        return Environment(
            loader=FileSystemLoader(searchpath=searchpath),
            undefined=StrictUndefined,
        )

    @abstractmethod
    def create_control_configs(self) -> None: ...
