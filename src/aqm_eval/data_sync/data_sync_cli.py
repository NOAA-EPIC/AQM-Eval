"""Command-line interface for AQM data synchronization.

Provides commands for downloading various datasets for UFS-AQM.
"""

import os
from pathlib import Path

import typer
from pydantic import BaseModel

from aqm_eval.data_sync.core import (
    ObservationsContext,
    ObservationsSyncRunner,
    SRWFixedContext,
    SRWFixedSyncRunner,
    TimeVaryingContext,
    TimeVaryingSyncRunner,
    UseCase,
    UseCaseKey,
)

os.environ["NO_COLOR"] = "1"
app = typer.Typer(pretty_exceptions_enable=False)


class _HelpMessage(BaseModel):
    dst_dir: str = "Destination directory for sync."
    max_concurrent_requests: str = "Maximum number of concurrent requests."
    dry_run: str = "Dry run. Nothing will be materially synchronized."


class _DefaultValue(BaseModel):
    max_concurrent_requests: int = 5


class _FlagName(BaseModel):
    dst_dir: str = "--dst-dir"
    dry_run: str = "--dry-run"
    max_concurrent_requests: str = "--max-concurrent-requests"


_HELP = _HelpMessage()
_DEFAULT = _DefaultValue()
_FLAG_NAME = _FlagName()


@app.command(name="time-varying", help="Download time varying input data for UFS-AQM.")
def time_varying(
    dst_dir: Path = typer.Option(..., _FLAG_NAME.dst_dir, help=_HELP.dst_dir),
    first_cycle_date: str = typer.Option(
        None,
        "--first-cycle-date",
        help="First cycle date in yyyymmdd format. Required if --use-case is not provided.",
    ),
    fcst_hr: int = typer.Option(0, "--fcst-hr", help="Forecast hour."),
    last_cycle_date: str = typer.Option(
        None,
        "--last-cycle-date",
        help="Last cycle date in yyyymmdd format. If not provided, defaults to 24 hours after --first-cycle-date.",
    ),
    use_case: UseCaseKey = typer.Option(UseCaseKey.UNDEFINED, "--use-case", help="Use case."),
    max_concurrent_requests: int = typer.Option(
        _DEFAULT.max_concurrent_requests,
        _FLAG_NAME.max_concurrent_requests,
        help=_HELP.max_concurrent_requests,
    ),
    dry_run: bool = typer.Option(False, _FLAG_NAME.dry_run, help=_HELP.dry_run),
    snippet: bool = typer.Option(
        False,
        "--snippet",
        help="If provided, download data for two forecast cycles (e.g. two days).",
    ),
) -> None:
    """Download time-varying input data for UFS-AQM. See help messages for parameter documentation."""
    kwds = dict(
        first_cycle_date=first_cycle_date,
        dst_dir=dst_dir,
        fcst_hr=fcst_hr,
        last_cycle_date=last_cycle_date,
        max_concurrent_requests=max_concurrent_requests,
        dry_run=dry_run,
        snippet=snippet,
    )
    if use_case == UseCaseKey.UNDEFINED:
        ctx = TimeVaryingContext.model_validate(kwds)
    else:
        ctx = UseCase.from_key(use_case, **kwds)
    runner = TimeVaryingSyncRunner(ctx)
    runner.run()


@app.command(name="srw-fixed", help="Download SRW fixed data.")
def srw_fixed(
    dst_dir: Path = typer.Option(..., _FLAG_NAME.dst_dir, help=_HELP.dst_dir),
    max_concurrent_requests: int = typer.Option(
        _DEFAULT.max_concurrent_requests,
        _FLAG_NAME.max_concurrent_requests,
        help=_HELP.max_concurrent_requests,
    ),
    dry_run: bool = typer.Option(False, _FLAG_NAME.dry_run, help=_HELP.dry_run),
) -> None:
    """Download SRW fixed data. See help messages for parameter documentation."""
    kwds = dict(
        dst_dir=dst_dir,
        max_concurrent_requests=max_concurrent_requests,
        dry_run=dry_run,
    )
    ctx = SRWFixedContext.model_validate(kwds)
    runner = SRWFixedSyncRunner(ctx)
    runner.run()


@app.command(name="observations", help="Download observations for UFS-AQM evaluation.")
def observations(
    dst_dir: Path = typer.Option(..., _FLAG_NAME.dst_dir, help=_HELP.dst_dir),
    max_concurrent_requests: int = typer.Option(
        _DEFAULT.max_concurrent_requests,
        _FLAG_NAME.max_concurrent_requests,
        help=_HELP.max_concurrent_requests,
    ),
    dry_run: bool = typer.Option(False, _FLAG_NAME.dry_run, help=_HELP.dry_run),
) -> None:
    """Download observations. See help messages for parameter documentation."""
    kwds = dict(
        dst_dir=dst_dir,
        max_concurrent_requests=max_concurrent_requests,
        dry_run=dry_run,
    )
    ctx = ObservationsContext.model_validate(kwds)
    runner = ObservationsSyncRunner(ctx)
    runner.run()


if __name__ == "__main__":
    app()
