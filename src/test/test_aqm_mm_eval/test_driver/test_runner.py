import pytest
from pydantic import BaseModel

from aqm_eval.aqm_mm_eval.driver.interface import SRWInterface
from aqm_eval.aqm_mm_eval.driver.package import PackageKey
from aqm_eval.aqm_mm_eval.driver.runner import MMEvalRunner


class MMEvalRunnerTestData(BaseModel):
    model_config = {"frozen": True}
    iface: SRWInterface
    expected_n_links: int
    expected_fns: set[str]


@pytest.fixture
def mm_eval_runner_test_data(srw_interface: SRWInterface, use_base_model: bool) -> MMEvalRunnerTestData:
    if use_base_model:
        expected_n_links = 100
        expected_fns = {
            "control_multi_boxplot.yaml",
            "control_scorecard_nmb.yaml",
            "control_timeseries.yaml",
            "control_boxplot.yaml",
            "namelist.yaml",
            "control_stats.yaml",
            "control_scorecard_ioa.yaml",
            "control_scorecard_nme.yaml",
            "control_taylor.yaml",
            "control_csi.yaml",
            "control_scorecard_rmse.yaml",
            "control_save_paired.yaml",
            "control_spatial_bias.yaml",
            "control_spatial_overlay.yaml",
        }
    else:
        expected_n_links = 50
        expected_fns = {
            "control_spatial_bias.yaml",
            "control_save_paired.yaml",
            "control_stats.yaml",
            "control_taylor.yaml",
            "control_timeseries.yaml",
            "control_csi.yaml",
            "control_spatial_overlay.yaml",
            "namelist.yaml",
            "control_multi_boxplot.yaml",
            "control_boxplot.yaml",
        }
    return MMEvalRunnerTestData(expected_n_links=expected_n_links, expected_fns=expected_fns, iface=srw_interface)



class TestMMEvalRunner:
    def test(self, mm_eval_runner_test_data: MMEvalRunnerTestData) -> None:
        iface = mm_eval_runner_test_data.iface
        runner = MMEvalRunner(iface=iface)

        runner.initialize()

        # Test links for all days are created
        actual_links = [ii for ii in iface.link_alldays_path.iterdir()]
        # LOGGER(str(actual_links), level=logging.DEBUG)
        assert len(actual_links) == mm_eval_runner_test_data.expected_n_links

        # Test control yaml files are created
        chem_run_dir = iface.mm_run_dir / PackageKey.CHEM.value
        actual_files = chem_run_dir.rglob("*")
        assert set([ii.name for ii in actual_files]) == mm_eval_runner_test_data.expected_fns

        # tdk: real test?
        with pytest.raises(ValueError) as excinfo:
            runner.run()
        assert str(excinfo.value).startswith(
            "did not find a match in any of xarray's currently installed IO backends"
        )
