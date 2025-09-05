import os
from pathlib import Path

import typer

from aqm_eval.aqm_mm_eval.driver.interface import SRWInterface
from aqm_eval.aqm_mm_eval.driver.package import PackageKey, TaskKey
from aqm_eval.aqm_mm_eval.driver.runner import MMEvalRunner

os.environ["NO_COLOR"] = "1"
app = typer.Typer(pretty_exceptions_enable=False)


@app.command(
    name="srw-init",
    help="Initialize the MELODIES-MONET UFS-AQM evaluation from the SRW workflow.",
)
def srw_init(
    expt_dir: Path = typer.Option(..., "--expt-dir", help="Experiment directory.")
):
    iface = SRWInterface(expt_dir=expt_dir)
    runner = MMEvalRunner(iface=iface)
    runner.initialize()


@app.command(
    name="srw-run",
    help="Run the MELODIES-MONET UFS-AQM evaluation from the SRW workflow.",
)
def srw_run(
    expt_dir: Path = typer.Option(..., "--expt-dir", help="Experiment directory."),
    package_selector: list[PackageKey] | None = typer.Option(
        None, "--package-selector", help="Package selector."
    ),
    task_selector: list[TaskKey] | None = typer.Option(
        None, "--task-selector", help="Task selector."
    ),
):
    iface = SRWInterface(expt_dir=expt_dir)
    runner = MMEvalRunner(iface=iface)
    runner.run(task_selector=task_selector, package_selector=package_selector)


if __name__ == "__main__":
    app()
