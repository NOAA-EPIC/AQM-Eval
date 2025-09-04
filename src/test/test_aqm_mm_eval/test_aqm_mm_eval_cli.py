from pathlib import Path

from pytest_mock import MockerFixture
from typer.testing import CliRunner

from aqm_eval.aqm_mm_eval.aqm_mm_eval_cli import app
from aqm_eval.aqm_mm_eval.driver.interface import SRWInterface
from aqm_eval.aqm_mm_eval.driver.package import TaskKey
from aqm_eval.aqm_mm_eval.driver.runner import MMEvalRunner


def test_help() -> None:
    """Test that the help message can be displayed."""
    runner = CliRunner()
    for subcommand in ("srw-init", "srw-run"):
        result = runner.invoke(app, [subcommand, "--help"], catch_exceptions=False)
        print(result.output)
        assert result.exit_code == 0


def test_srw_run_task_selector(tmp_path: Path, srw_interface: SRWInterface, mocker: MockerFixture) -> None:
    mock = mocker.patch.object(MMEvalRunner, "run")
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "srw-run",
            "--expt-dir",
            tmp_path,
            "--task-selector",
            "save_paired",
            "--task-selector",
            "timeseries",
        ],
        catch_exceptions=False,
    )
    print(result.output)
    assert result.exit_code == 0
    mock.assert_called_once_with(task_selector=(TaskKey.SAVE_PAIRED, TaskKey.TIMESERIES))

