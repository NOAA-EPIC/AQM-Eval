import cartopy
import dask
import matplotlib
from melodies_monet import driver
from pydantic import BaseModel

from aqm_eval.aqm_mm_eval.driver.helpers import create_symlinks
from aqm_eval.aqm_mm_eval.driver.interface import (
    SRWInterface,
)
from aqm_eval.aqm_mm_eval.driver.package import PackageKey, TaskKey
from aqm_eval.logging_aqm_eval import LOGGER, log_it


class MMEvalRunner(BaseModel):
    model_config = {"frozen": True}

    iface: SRWInterface

    @log_it
    def initialize(self) -> None:
        LOGGER(f"{self.iface=}")

        for model in self.iface.mm_models:
            model.create_symlinks()

        LOGGER("creating MM control configs")
        self.iface.create_control_configs()

    @log_it
    def run(
        self,
        package_selector: tuple[PackageKey, ...] | list[PackageKey] = tuple(PackageKey),
        task_selector: tuple[TaskKey, ...] | list[TaskKey] = tuple(TaskKey),
        finalize: bool = False,
    ) -> None:
        try:
            matplotlib.use("Agg")
            cartopy.config["data_dir"] = self.iface.cartopy_data_dir
            dask.config.set(**{"array.slicing.split_large_chunks": True})
            for package in self.iface.mm_packages:
                LOGGER(f"{package.key=}")
                if package.key not in package_selector:
                    continue
                for task in package.tasks:
                    if task not in task_selector:
                        continue
                    LOGGER(f"{task=}")
                    an = driver.analysis()
                    control_yaml = self.iface.mm_run_dir / package.key.value / f"control_{task.value}.yaml"
                    LOGGER(f"{control_yaml=}")
                    an.control = control_yaml
                    an.read_control()

                    match task:
                        case TaskKey.SAVE_PAIRED:
                            an.open_models()
                            an.open_obs()
                            an.pair_data()
                            an.save_analysis()
                        case TaskKey.SPATIAL_OVERLAY | TaskKey.SPATIAL_BIAS:
                            an.read_analysis()
                            an.open_models()
                            an.plotting()
                        case TaskKey.STATS:
                            an.read_analysis()
                            an.stats()
                        case _:
                            an.read_analysis()
                            an.plotting()
        finally:
            if finalize:
                self.finalize()

    @log_it
    def finalize(self) -> None: ...
