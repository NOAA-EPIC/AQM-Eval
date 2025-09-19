import cartopy  # type: ignore[import-untyped]
import dask
import matplotlib
from melodies_monet import driver  # type: ignore[import-untyped]
from melodies_monet.driver import analysis  # type: ignore[import-untyped]
from pydantic import BaseModel

from aqm_eval.logging_aqm_eval import LOGGER, log_it
from aqm_eval.mm_eval.driver.context.base import AbstractContext
from aqm_eval.mm_eval.driver.package import PackageKey, TaskKey


class MMEvalRunner(BaseModel):
    model_config = {"frozen": True}

    ctx: AbstractContext

    @log_it
    def initialize(self) -> None:
        LOGGER(f"{self.ctx=}")

        for model in self.ctx.mm_models:
            model.create_symlinks()

        LOGGER("creating MM control configs")
        self.ctx.create_control_configs()

    @log_it
    def run(
        self,
        package_selector: tuple[PackageKey, ...] = tuple(PackageKey),
        task_selector: tuple[TaskKey, ...] = tuple(TaskKey),
        finalize: bool = False,
    ) -> None:
        LOGGER(f"{package_selector=}")
        LOGGER(f"{task_selector=}")
        LOGGER(f"{finalize=}")
        try:
            matplotlib.use("Agg")
            cartopy.config["data_dir"] = self.ctx.cartopy_data_dir
            dask.config.set({"array.slicing.split_large_chunks": True})
            for package in self.ctx.mm_packages:
                if package.key not in package_selector:
                    continue
                LOGGER(f"{package.key=}")
                for task in package.tasks:
                    if task not in task_selector:
                        continue
                    LOGGER(f"{task=}")
                    an = driver.analysis()
                    control_yaml = self.ctx.mm_run_dir / package.key.value / f"control_{task.value}.yaml"
                    LOGGER(f"{control_yaml=}")
                    an.control = control_yaml
                    an.read_control()

                    self._run_task_(an, task)
        finally:
            if finalize:
                self.finalize()

    @staticmethod
    @log_it
    def _run_task_(an: analysis, task: TaskKey) -> None:
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

    @log_it
    def finalize(self) -> None: ...
