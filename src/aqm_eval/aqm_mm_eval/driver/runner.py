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

    def run(self, package_selector: tuple[EvalType, ...] | None = None, task_selector: tuple[MMTask, ...] | None = None, finalize: bool = False) -> None:
        LOGGER("running MMEvalRunner")
        try:
            if package_selector is None:
                package_selector = self.iface.mm_eval_types
            matplotlib.use("Agg")
            # tdk: need to set cartopy directory from configs
            cartopy.config["data_dir"] = (
                "/gpfs/f6/bil-fire8/world-shared/UFS_SRW_data/develop/NaturalEarth"
            )
            dask.config.set(**{"array.slicing.split_large_chunks": True})
            for package in package_selector:
                for task in task_selector:
                    an = driver.analysis()
                    control_yaml = self.iface.mm_run_dir / package.value / f"control_{task.value}.yaml"
                    LOGGER(f"{control_yaml=}")
                    an.control = control_yaml
                    an.read_control()

                    match task:
                        case MMTask.SAVE_PAIRED:
                            an.open_models()
                            an.open_obs()
                            an.pair_data()
                            an.save_analysis()
                        case MMTask.SPATIAL_OVERLAY | MMTask.SPATIAL_BIAS:
                            an.read_analysis()
                            an.open_models()
                            an.plotting()
                        case MMTask.STATS:
                            an.read_analysis()
                            an.stats()
                        case _:
                            an.read_analysis()
                            an.plotting()
        finally:
            if finalize:
                self.finalize()

    def finalize(self) -> None:
        LOGGER("finalizing MMEvalRunner")
