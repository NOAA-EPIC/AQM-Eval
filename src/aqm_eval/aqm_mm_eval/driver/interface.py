import os
from pathlib import Path
from typing import Annotated

from pydantic import BaseModel, computed_field, BeforeValidator

PathExisting = Annotated[Path, BeforeValidator(lambda x: Path(x).exists())]

class SRWInterface(BaseModel):
    model_config = {"frozen": True}

    expt_dir: PathExisting

    @computed_field
    def config_path_user(self) -> PathExisting:
        return self.expt_dir / "config.yaml"

    @computed_field
    def config_path_rocoto(self) -> PathExisting:
        return self.expt_dir / "rocoto_defns.yaml"

    def get_yaml_paths(self) -> tuple[PathExisting, ...]:
        return self.config_path_user, self.config_path_rocoto
