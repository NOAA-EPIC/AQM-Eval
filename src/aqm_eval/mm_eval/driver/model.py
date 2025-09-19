from enum import StrEnum, unique

from pydantic import BaseModel, computed_field

from aqm_eval.logging_aqm_eval import log_it
from aqm_eval.mm_eval.driver.helpers import PathExisting, create_symlinks


@unique
class ModelRole(StrEnum):
    EVAL = "eval"
    BASE = "base"


class Model(BaseModel):
    model_config = {"frozen": True}

    expt_dir: PathExisting
    label: str
    title: str
    prefix: str
    role: ModelRole
    cycle_dir_template: tuple[str, ...]
    dyn_file_template: tuple[str, ...]
    link_alldays_path: PathExisting

    @computed_field
    @property
    def link_alldays_path_template(self) -> str:
        return str(self.link_alldays_path / f"{self.prefix}*.nc")

    @computed_field
    @property
    def plot_kwargs_color(self) -> str:
        match self.role:
            case ModelRole.EVAL:
                return "forestgreen"
            case ModelRole.BASE:
                return "magenta"
            case _:
                raise ValueError(f"Unknown role: {self.role}")

    @log_it
    def create_symlinks(
        self,
    ) -> None:
        create_symlinks(
            self.expt_dir,
            self.link_alldays_path,
            self.prefix,
            self.cycle_dir_template,
            self.dyn_file_template,
        )
