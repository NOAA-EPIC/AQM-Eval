from pathlib import Path

import pytest


@pytest.fixture
def bin_dir() -> Path:
    ret = Path(__file__).parent / "bin"
    assert ret.exists()
    return ret
