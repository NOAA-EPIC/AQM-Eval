from aqm_eval.base import AeBaseModel
from aqm_eval.mm_eval.driver.config import TaskKey


class PlotTask(AeBaseModel):
    type: TaskKey
    fig_kwargs: dict
    default_plot_kwargs: dict
    text_kwargs: dict
    domain_type: list[str]
    domain_name: list[str]
    data: list[str]
    data_proc: dict
