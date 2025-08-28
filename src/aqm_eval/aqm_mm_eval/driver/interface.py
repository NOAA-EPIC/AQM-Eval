import os
from pathlib import Path
from typing import Annotated

from pydantic import BaseModel, computed_field, BeforeValidator

def _format_path_existing_(value: Path | str) -> Path:
    ret = Path(value)
    if not ret.exists():
        raise ValueError(f"path does not exist: {ret}")
    return ret

PathExisting = Annotated[Path, BeforeValidator(_format_path_existing_)]

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
