import os
from pathlib import Path

from pydantic import BaseModel, computed_field


class SRWInterface(BaseModel):
    model_config = {"frozen": True}

    expt_dir: Path

    @computed_field
    def config_path_user(self) -> Path:
        return self.expt_dir / "config.yaml"

    @computed_field
    def config_path_rocoto(self) -> Path:
        return self.expt_dir / "rocoto_defns.yaml"

    def get_yaml_paths(self) -> tuple[Path, ...]:
        return self.config_path_user, self.config_path_rocoto
