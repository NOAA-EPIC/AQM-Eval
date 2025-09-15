import os
from pathlib import Path

import typer

from aqm_eval.aqm_mm_eval.driver.package import PackageKey, TaskKey
from aqm_eval.aqm_mm_eval.driver.runner import MMEvalRunner

os.environ["NO_COLOR"] = "1"
app = typer.Typer(pretty_exceptions_enable=False)


@app.command(
    name="srw-init",
    help="Initialize the MELODIES-MONET UFS-AQM evaluation from the SRW workflow.",
)
def srw_init(expt_dir: Path = typer.Option(..., "--expt-dir", help="Experiment directory.")) -> None:
    from aqm_eval.aqm_mm_eval.driver.interface.srw import SRWInterface

    iface = SRWInterface(expt_dir=expt_dir)
    runner = MMEvalRunner(iface=iface)
    runner.initialize()


@app.command(
    name="srw-run",
    help="Run the MELODIES-MONET UFS-AQM evaluation from the SRW workflow.",
)
def srw_run(
    expt_dir: Path = typer.Option(..., "--expt-dir", help="Experiment directory."),
    package_selector: list[PackageKey] = typer.Option(list(PackageKey), "--package-selector", help="Package selector."),
    task_selector: list[TaskKey] = typer.Option(list(TaskKey), "--task-selector", help="Task selector."),
) -> None:
    from aqm_eval.aqm_mm_eval.driver.interface.srw import SRWInterface

    iface = SRWInterface(expt_dir=expt_dir)
    runner = MMEvalRunner(iface=iface)
    runner.run(task_selector=tuple(task_selector), package_selector=tuple(package_selector))


if __name__ == "__main__":
    app()
