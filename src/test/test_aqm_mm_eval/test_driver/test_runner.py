from aqm_eval.aqm_mm_eval.driver.interface import SRWInterface, EvalType
from aqm_eval.aqm_mm_eval.driver.runner import MMEvalRunner


class TestMMEvalRunner:

    def test(self, srw_interface: SRWInterface) -> None:
        runner = MMEvalRunner(iface=srw_interface)

        runner.initialize()

        # Test links for all days are created
        actual_links = [ii for ii in srw_interface.link_alldays_path.iterdir()]
        # LOGGER(str(actual_links), level=logging.DEBUG)
        assert len(actual_links) == 50

        # Test control yaml files are created
        chem_run_dir = srw_interface.mm_run_dir / EvalType.CHEM.value
        actual_files = chem_run_dir.rglob("*")
        assert set([ii.name for ii in actual_files]) == {
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

        tdk

        # runner.run(finalize=True)
