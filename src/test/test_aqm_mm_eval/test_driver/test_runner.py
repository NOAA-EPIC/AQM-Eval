from aqm_eval.aqm_mm_eval.driver.interface import SRWInterface, EvalType
from aqm_eval.aqm_mm_eval.driver.runner import MMEvalRunner


class TestMMEvalRunner:

    def test(self, srw_interface: SRWInterface) -> None:
        runner = MMEvalRunner(iface=srw_interface)

        runner.initialize()

        actual_links = [ii for ii in srw_interface.link_alldays_path.iterdir()]
        # LOGGER(str(actual_links), level=logging.DEBUG)
        assert len(actual_links) == 50

        assert (srw_interface.mm_run_dir / EvalType.CHEM.value / "namelist.yaml").exists()
        tdk

        # runner.run(finalize=True)
