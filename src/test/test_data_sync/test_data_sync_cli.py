from pathlib import Path

from typer.testing import CliRunner

from aqm_eval.data_sync.core import UseCaseKey
from aqm_eval.data_sync.data_sync_cli import app


def test_help() -> None:
    """Test that the help message can be displayed."""
    runner = CliRunner()
    for subcommand in ("time-varying", "srw-fixed", "observations"):
        result = runner.invoke(app, [subcommand, "--help"], catch_exceptions=False)
        print(result.output)
        assert result.exit_code == 0


def test_time_varying_use_case(tmp_path: Path) -> None:
    """Test the use case pathway for a snippet."""
    runner = CliRunner()

    args = [
        "time-varying",
        "--use-case",
        UseCaseKey.AEROMMA.value,
        "--dst-dir",
        str(tmp_path),
        "--dry-run",
        "--snippet",
    ]
    result = runner.invoke(app, args, catch_exceptions=False)
    print(result.output)
    assert result.exit_code == 0


def test_observations(tmp_path: Path) -> None:
    """Test downloading observations with a dry run."""
    runner = CliRunner()

    args = [
        "observations",
        "--dst-dir",
        str(tmp_path),
        "--dry-run",
    ]
    result = runner.invoke(app, args, catch_exceptions=False)
    print(result.output)
    assert result.exit_code == 0
