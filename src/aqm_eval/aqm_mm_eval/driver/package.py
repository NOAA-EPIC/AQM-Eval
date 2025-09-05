from enum import unique, StrEnum
from pathlib import Path

from pydantic import BaseModel, computed_field

from aqm_eval.aqm_mm_eval.driver.helpers import PathExisting


@unique
class TaskKey(StrEnum):
    SAVE_PAIRED = "save_paired"
    TIMESERIES = "timeseries"
    TAYLOR = "taylor"
    SPATIAL_BIAS = "spatial_bias"
    SPATIAL_OVERLAY = "spatial_overlay"
    BOXPLOT = "boxplot"
    MULTI_BOXPLOT = "multi_boxplot"
    SCORECARD_RMSE = "scorecard_rmse"  # tdk: handle situation with multiple models where scorecards make sense
    SCORECARD_IOA = "scorecard_ioa"
    SCORECARD_NMB = "scorecard_nmb"
    SCORECARD_NME = "scorecard_nme"
    CSI = "csi"
    STATS = "stats"


@unique
class PackageKey(StrEnum):
    CHEM = "chem"
    MET = "met"
    AQS_PM25 = "aqs_pm25"
    VOCS = "vocs"


class ChemEvalPackage(BaseModel):
    model_config = {"frozen": True}
    root_dir: PathExisting
    key: PackageKey = PackageKey.CHEM
    namelist_template: str = "namelist.chem.j2"

    @computed_field
    @property
    def run_dir(self) -> Path:
        return self.root_dir / self.key.value

    @computed_field
    @property
    def tasks(self) -> tuple[TaskKey, ...]:
        return self.get_default_tasks()

    @classmethod
    def get_default_tasks(cls) -> tuple[TaskKey, ...]:
        # return tuple([ii for ii in TaskKey if not ii.name.startswith("SCORECARD")]) #tdk: handle scorecard scenario
        #tdk: option to enable scorecard tasks
        return tuple([ii for ii in TaskKey]) #tdk: handle scorecard scenario
