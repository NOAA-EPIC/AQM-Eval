from pydantic import BaseModel

from aqm_eval.aqm_mm_eval.driver.helpers import create_symlinks
from aqm_eval.aqm_mm_eval.driver.interface import (
    SRWInterface,
    EvalType,
    ChemEvalPackage,
    MMTask,
)
from aqm_eval.logging_aqm_eval import LOGGER

import matplotlib


from melodies_monet import driver
import os, sys
import dask
import cartopy


class MMEvalRunner(BaseModel):
    model_config = {"frozen": True}

    iface: SRWInterface

    def initialize(self) -> None:
        LOGGER("initializing MMEvalRunner")
        LOGGER(f"{self.iface=}")
        LOGGER("creating symlinks")
        create_symlinks(
            self.iface.expt_dir,
            self.iface.link_alldays_path,
            self.iface.mm_eval_prefix,
            self.iface.link_simulation,
            (self.iface.dyn_file_template,),
        )

        LOGGER("creating MM control configs")
        for eval_type in self.iface.mm_eval_types:
            LOGGER(f"{eval_type=}")
            match eval_type:
                case EvalType.CHEM:
                    klass = ChemEvalPackage
                case _:
                    raise ValueError(eval_type)
            eval_package = klass()
            eval_package.create_control_configs(self.iface)

    def run(self, task_selector: tuple[MMTask, ...] | None = None, finalize: bool = False) -> None:
        LOGGER("running MMEvalRunner")
        if task_selector is None:
            task_selector = self.iface.mm_eval_tasks
        try:
            matplotlib.use("Agg")
            # tdk: need to set cartopy directory from configs
            cartopy.config["data_dir"] = (
                "/gpfs/f6/bil-fire8/world-shared/UFS_SRW_data/develop/NaturalEarth"
            )
            dask.config.set(**{"array.slicing.split_large_chunks": True})
            for task in task_selector:
                an = driver.analysis()
                an.control = self.iface.
            tdk
        finally:
            if finalize:
                self.finalize()

    def finalize(self) -> None:
        LOGGER("finalizing MMEvalRunner")
