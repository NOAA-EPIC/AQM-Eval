from unittest.mock import Mock

import melodies_monet  # type: ignore[import-untyped]
import pytest
import xarray as xr
from pydantic import BaseModel
from pytest_mock import MockerFixture

from aqm_eval.mm_eval.driver.context.srw import SRWContext
from aqm_eval.mm_eval.driver.package.core import (
    AbstractDaskOperation,
    AbstractEvalPackage,
    PackageKey,
    TaskKey,
    package_key_to_class,
)


class MMEvalRunnerTestData(BaseModel):
    model_config = {"frozen": True}
    ctx: SRWContext
    package_class: type[AbstractEvalPackage]
    expected_n_links: int
    expected_n_dask_run_calls: int


@pytest.fixture(params=tuple(PackageKey))
def package_key(request: pytest.FixtureRequest) -> PackageKey:
    return request.param


@pytest.fixture
def mm_eval_runner_test_data(srw_context: SRWContext, use_base_model: bool, package_key: PackageKey) -> MMEvalRunnerTestData:
    package_class = package_key_to_class(package_key)
    expected_n_links = 25 * 2  # 25 dynf hourly files * 2 cycle directories
    expected_n_dask_run_calls = 0

    match package_key:
        case PackageKey.ISH | PackageKey.AQS_PM:
            expected_n_links = 2  # 2 combined files (1 per cycle)
            expected_n_dask_run_calls = expected_n_links  # one call per file created

    if use_base_model:
        # Two model adjustment
        expected_n_links *= 2
        expected_n_dask_run_calls *= 2

    return MMEvalRunnerTestData(
        expected_n_links=expected_n_links,
        ctx=srw_context,
        package_class=package_class,
        expected_n_dask_run_calls=expected_n_dask_run_calls,
    )


def fake_run(self: AbstractDaskOperation) -> xr.Dataset:
    assert not self.out_path.exists()
    self.out_path.touch()
    return xr.Dataset()


def test_all_packages(mm_eval_runner_test_data: MMEvalRunnerTestData, mocker: MockerFixture) -> None:
    package = mm_eval_runner_test_data.package_class.model_validate(dict(ctx=mm_eval_runner_test_data.ctx))

    # Mock for dask operations -----------------------------------------------------------------

    _ = mocker.patch.object(AbstractDaskOperation, "run", fake_run)
    spy_m_dask_op_run = mocker.spy(AbstractDaskOperation, "run")

    # Test initialize --------------------------------------------------------------------------

    package.initialize()

    actual_data = [ii.name for ii in package.link_alldays_path.iterdir()]
    assert len(actual_data) == mm_eval_runner_test_data.expected_n_links

    actual_files = package.run_dir.rglob("*.yaml")
    expected_filenames = package.task_control_filenames
    expected_filenames.update({"namelist.yaml"})
    assert set([ii.name for ii in actual_files]) == expected_filenames

    assert package.link_alldays_path.name in [ii.name for ii in package.run_dir.iterdir()]

    # Test run ---------------------------------------------------------------------------------

    m_analysis = Mock()
    m_analysis.read_control = Mock()
    m_analysis.open_models = Mock()
    m_analysis.open_obs = Mock()
    m_analysis.pair_data = Mock()
    m_analysis.save_analysis = Mock()
    _ = mocker.patch.object(melodies_monet.driver, "analysis", return_value=m_analysis)

    package.run(TaskKey.SAVE_PAIRED)

    assert package.mm_package_output_dir.exists()

    m_analysis.read_control.assert_called_once()
    m_analysis.open_models.assert_called_once()
    m_analysis.open_obs.assert_called_once()
    m_analysis.pair_data.assert_called_once()
    m_analysis.save_analysis.assert_called_once()

    assert spy_m_dask_op_run.call_count == mm_eval_runner_test_data.expected_n_dask_run_calls
