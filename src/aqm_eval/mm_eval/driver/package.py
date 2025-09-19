"""Defines package objects used when generating MM files. A package is a collection of tasks specfiic to an evaluation type."""

from enum import StrEnum, unique
from functools import cached_property
from pathlib import Path

from pydantic import BaseModel, Field, computed_field

from aqm_eval.mm_eval.driver.helpers import PathExisting


@unique
class TaskKey(StrEnum):
    """Unique MM task keys."""

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
    """Unique MM package keys."""

    CHEM = "chem"
    MET = "met"
    AQS_PM25 = "aqs_pm25"
    VOCS = "vocs"


class ChemEvalPackage(BaseModel):
    """Defines a chemistry evaluation package."""

    model_config = {"frozen": True}
    root_dir: PathExisting = Field(description="Root directory for MM evaluation package.")
    use_base_model: bool = Field(description="If True, a base model will be used to generate scorecards.")
    key: PackageKey = Field(default=PackageKey.CHEM, description="MM package key.")
    namelist_template: str = Field(default="namelist.chem.j2", description="Package template file.")

    @computed_field(description="Run directory for the MM evaluation package.")
    @cached_property
    def run_dir(self) -> Path:
        return self.root_dir / self.key.value

    @computed_field(description="Tasks that the package will run.")
    @cached_property
    def tasks(self) -> tuple[TaskKey, ...]:
        if self.use_base_model:
            return tuple([ii for ii in TaskKey])
        else:
            return tuple([ii for ii in TaskKey if not ii.name.startswith("SCORECARD")])
