
from typer.testing import CliRunner

from aqm_eval.aqm_mm_eval.aqm_mm_eval_cli import app


def test_help() -> None:
    """Test that the help message can be displayed."""
    runner = CliRunner()
    for subcommand in ("srw-init", "srw-run"):
        result = runner.invoke(app, [subcommand, "--help"], catch_exceptions=False)
        print(result.output)
        assert result.exit_code == 0

