from enum import StrEnum, unique

from pydantic import BaseModel

from aqm_eval.aqm_mm_eval.driver.helpers import PathExisting, create_symlinks
from aqm_eval.logging_aqm_eval import log_it


@unique
class ModelRole(StrEnum):
    EVAL = "eval"
    BASE = "base"


class Model(BaseModel):
    model_config = {"frozen": True}

    expt_dir: PathExisting
    label: str
    prefix: str
    role: ModelRole
    cycle_dir_template: tuple[str, ...]
    dyn_file_template: tuple[str, ...]

    @log_it
    def create_symlinks(
        self,
        dst_dir: PathExisting,
    ) -> None:
        create_symlinks(
            self.expt_dir,
            dst_dir,
            self.prefix,
            self.cycle_dir_template,
            self.dyn_file_template,
        )
