import logging
from pathlib import Path

import pytest

from aqm_eval.aqm_mm_eval.driver.helpers import create_symlinks
from aqm_eval.logging_aqm_eval import LOGGER


@pytest.fixture
def dummy_dyn_files(tmp_path: Path) -> None:
    for dirname in ['2023060112', '2023060212']:
        dyn_dir = tmp_path / dirname
        dyn_dir.mkdir(exist_ok=False, parents=False)
        for fhr in range(25):
            dyn_file = dyn_dir / f"dynf{fhr:03d}.nc"
            dyn_file.touch()


def test_create_symlinks(tmp_path: Path, dummy_dyn_files: None) -> None:
    dst_dir = tmp_path / "actual-links"
    dst_dir.mkdir(exist_ok=False, parents=False)

    src_dir_template = ("2023*",)
    src_fn_template = ("dynf*.nc",)

    create_symlinks(tmp_path, dst_dir, src_dir_template, src_fn_template)

    actual_links = [ii for ii in dst_dir.iterdir()]
    LOGGER(str(actual_links), level=logging.DEBUG)
    assert len(actual_links) == 50