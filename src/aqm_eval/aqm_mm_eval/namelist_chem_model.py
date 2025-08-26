"""
Pydantic model for namelist.chem.yaml configuration.
"""
from typing import Dict, List, Union, Literal, Any
from pathlib import Path
from pydantic import BaseModel
import yaml


class ObservationVariable(BaseModel):
    """Configuration for an observation variable."""
    unit_scale: Union[int, float]
    unit_scale_method: Literal["*", "+", "-", "/"]
    nan_value: float
    ylabel_plot: str
    ty_scale: float
    vmin_plot: Union[int, float]
    vmax_plot: Union[int, float]
    vdiff_plot: Union[int, float]
    nlevels_plot: int


class ModelKwargs(BaseModel):
    """Model-specific keyword arguments."""
    surf_only: bool
    mech: str


class PlotKwargs(BaseModel):
    """Plot styling keyword arguments."""
    color: str
    marker: str
    linestyle: str
    markersize: Union[int, float]


class FigKwargs(BaseModel):
    """Figure keyword arguments."""
    figsize: List[Union[int, float]]


class SpatialFigKwargs(BaseModel):
    """Spatial figure keyword arguments."""
    states: bool
    figsize: List[Union[int, float]]


class TextKwargs(BaseModel):
    """Text styling keyword arguments."""
    fontsize: Union[int, float]


class DataProc(BaseModel):
    """Data processing configuration."""
    rem_obs_nan: bool
    set_axis: bool


class TimeseriesDataProc(DataProc):
    """Timeseries-specific data processing."""
    ts_select_time: str
    ts_avg_window: str


class TimeseriesPlotKwargs(BaseModel):
    """Timeseries plot kwargs."""
    linewidth: float
    markersize: Union[int, float]


class StatsTableKwargs(BaseModel):
    """Statistics table configuration."""
    figsize: List[Union[int, float]]
    fontsize: Union[int, float]
    xscale: float
    yscale: float
    edges: str


class NamelistChemConfig(BaseModel):
    """Main configuration model for namelist.chem.yaml."""
    
    # General settings
    start_time: str
    end_time: str
    output_dir: str
    debug_option: bool
    mm_tasks: List[str]
    
    # Link simulation settings
    link_simulation: str
    link_Alldays_path: str
    link_eval_path: str
    link_eval_predix: str
    link_eval_target: str
    
    # Rename settings
    rename_dir: str
    rename_date1: str
    rename_date2: str
    
    # Paired file settings
    paired_format: str
    paired_predix: str
    paired_save_data: str
    paired_dataset: Dict[str, str]
    
    # Model eval settings
    model_eval_label: str
    model_eval_files: str
    model_eval_type: str
    model_eval_kwargs: ModelKwargs
    model_eval_radius: int
    model_eval_mapping: Dict[str, str]
    model_eval_variables: str
    model_eval_projection: str
    model_eval_plot_kwargs: PlotKwargs
    
    # Observation settings
    obs_label: str
    obs_file: str
    obs_variables: Dict[str, ObservationVariable]
    
    # Timeseries settings
    timeseries_fig_kwargs: FigKwargs
    timeseries_plot_kwargs: TimeseriesPlotKwargs
    timeseries_text_kwargs: TextKwargs
    timeseries_domain_type: List[str]
    timeseries_domain_name: List[str]
    timeseries_dataset: List[str]
    timeseries_data_proc: TimeseriesDataProc
    
    # Taylor diagram settings
    taylor_fig_kwargs: FigKwargs
    taylor_plot_kwargs: PlotKwargs
    taylor_text_kwargs: TextKwargs
    taylor_domain_type: List[str]
    taylor_domain_name: List[str]
    taylor_dataset: List[str]
    taylor_data_proc: DataProc
    
    # Spatial bias settings
    spatial_bias_fig_kwargs: SpatialFigKwargs
    spatial_bias_text_kwargs: TextKwargs
    spatial_bias_domain_type: List[str]
    spatial_bias_domain_name: List[str]
    spatial_bias_dataset: List[str]
    spatial_bias_data_proc: DataProc
    
    # Spatial overlay settings
    spatial_overlay_fig_kwargs: SpatialFigKwargs
    spatial_overlay_text_kwargs: TextKwargs
    spatial_overlay_domain_type: List[str]
    spatial_overlay_domain_name: List[str]
    spatial_overlay_dataset: List[str]
    spatial_overlay_data_proc: DataProc
    
    # Boxplot settings
    boxplot_fig_kwargs: FigKwargs
    boxplot_text_kwargs: TextKwargs
    boxplot_domain_type: List[str]
    boxplot_domain_name: List[str]
    boxplot_dataset: List[str]
    boxplot_data_proc: DataProc
    
    # Multi-boxplot settings
    multi_boxplot_fig_kwargs: FigKwargs
    multi_boxplot_text_kwargs: TextKwargs
    multi_boxplot_domain_type: List[str]
    multi_boxplot_domain_name: List[str]
    multi_boxplot_region_type: List[str]
    multi_boxplot_region_name: List[str]
    multi_boxplot_urban_rural_name: List[str]
    multi_boxplot_urban_rural_value: str
    multi_boxplot_eval_method: str
    multi_boxplot_dataset: List[str]
    multi_boxplot_dataset_label: List[str]
    multi_boxplot_data_proc: DataProc
    
    # Scorecard settings
    scorecard_fig_kwargs: FigKwargs
    scorecard_text_kwargs: TextKwargs
    scorecard_domain_type: List[str]
    scorecard_domain_name: List[str]
    scorecard_region_type: List[str]
    scorecard_region_name: List[str]
    scorecard_urban_rural_name: List[str]
    scorecard_urban_rural_value: str
    scorecard_eval_method: str
    scorecard_dataset: List[str]
    scorecard_dataset_label: List[str]
    scorecard_data_proc: DataProc
    
    # CSI settings
    csi_fig_kwargs: FigKwargs
    csi_text_kwargs: TextKwargs
    csi_domain_type: List[str]
    csi_domain_name: List[str]
    csi_threshold: List[Union[int, float]]
    csi_score: str
    csi_dataset: List[str]
    csi_dataset_label: List[str]
    csi_data_proc: DataProc
    
    # Statistics settings
    stats_score_list: List[str]
    stats_table_option: bool
    stats_table_kwargs: StatsTableKwargs
    stats_domain_type: List[str]
    stats_domain_name: List[str]
    stats_dataset: List[str]

    @classmethod
    def from_yaml(cls, yaml_path: Union[str, Path]) -> "NamelistChemConfig":
        """Load configuration from a YAML file."""
        yaml_path = Path(yaml_path)
        
        with open(yaml_path, 'r') as f:
            data = yaml.safe_load(f)
        
        return cls(**data)