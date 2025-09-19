from enum import StrEnum, unique
from pathlib import Path

from pydantic import BaseModel, computed_field

from aqm_eval.mm_eval.driver.helpers import PathExisting


@unique
class TaskKey(StrEnum):
    SAVE_PAIRED = "save_paired"
    TIMESERIES = "timeseries"
    TAYLOR = "taylor"
    SPATIAL_BIAS = "spatial_bias"
    SPATIAL_OVERLAY = "spatial_overlay"
    BOXPLOT = "boxplot"
    MULTI_BOXPLOT = "multi_boxplot"
    SCORECARD_RMSE = "scorecard_rmse"
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
    use_base_model: bool
    key: PackageKey = PackageKey.CHEM
    namelist_template: str = "namelist.chem.j2"

    @computed_field
    @property
    def run_dir(self) -> Path:
        return self.root_dir / self.key.value

    @computed_field
    @property
    def tasks(self) -> tuple[TaskKey, ...]:
        if self.use_base_model:
            return tuple([ii for ii in TaskKey])
        else:
            return tuple([ii for ii in TaskKey if not ii.name.startswith("SCORECARD")])
