from pydantic import BaseModel

from aqm_eval.aqm_mm_eval.driver.helpers import create_symlinks
from aqm_eval.aqm_mm_eval.driver.interface import SRWInterface, EvalType, ChemEvalPackage
from aqm_eval.logging_aqm_eval import LOGGER


class MMEvalRunner(BaseModel):
    model_config = {"frozen": True}

    iface: SRWInterface

    def initialize(self) -> None:
        LOGGER("initializing MMEvalRunner")
        LOGGER(f"{self.iface=}")
        LOGGER("creating symlinks")
        create_symlinks(
            self.iface.expt_dir,
            self.iface.link_alldays_path,
            self.iface.mm_eval_prefix,
            self.iface.link_simulation,
            (self.iface.dyn_file_template,),
        )

        LOGGER("creating MM control configs")
        for eval_type in self.iface.mm_eval_types:
            LOGGER(f"{eval_type=}")
            match eval_type:
                case EvalType.CHEM:
                    klass = ChemEvalPackage
                case _:
                    raise ValueError(eval_type)
            eval_package = klass()
            eval_package.create_control_configs(self.iface)

    def run(self, finalize: bool = False) -> None:
        LOGGER("running MMEvalRunner")
        try:
            tdk
        finally:
            if finalize:
                self.finalize()

    def finalize(self) -> None:
        LOGGER("finalizing MMEvalRunner")
