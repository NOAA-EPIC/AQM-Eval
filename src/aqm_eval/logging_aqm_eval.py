import logging
import logging.config
from enum import StrEnum, unique

_PROJECT_NAME = "aqm-eval"


@unique
class LogLevel(StrEnum):
    """Log level enum.

    Attributes
    ----------
    INFO : str
        Equivalent to `logging.INFO`.
    DEBUG : str
        Equivalent to `logging.DEBUG`.
    """

    INFO = "info"
    DEBUG = "debug"


class LoggerWrapper:
    """Wrapper for logging functionality.

    Provides a convenient interface for logging with configurable behavior.

    Attributes
    ----------
    logger : logging.Logger | None
        The underlying logger instance. If `None`, the logger is not initialized.
    exit_on_error : bool
        Whether to exit on error or log exception information and continue execution.
    """

    logger: logging.Logger | None = None
    exit_on_error: bool = True

    def __call__(
        self,
        msg: str,
        level: int = logging.INFO,
        exc_info: Exception | None = None,
        stacklevel: int = 2,
    ) -> None:
        """Log a message.

        Parameters
        ----------
        msg : str
            The message to log.
        level : int, optional
            An optional override for the message level.
        exc_info : Exception | None, optional
            If provided, log this exception and raise an error if `self.exit_on_error`
            is `True`.
        stacklevel : int, optional
            If greater than 1, the corresponding number of stack frames are skipped
            when computing the line number and function name.
        """
        if exc_info is not None:
            level = logging.ERROR
        self._get_logger_().log(level, msg, exc_info=exc_info, stacklevel=stacklevel)
        if exc_info is not None and self.exit_on_error:
            raise exc_info

    def initialize(
        self,
        log_level: LogLevel = LogLevel.INFO,
        exit_on_error: bool = True,
        rank: int = 0,
        again: bool = False,
    ) -> None:
        """Initialize the logger.

        Parameters
        ----------
        log_level : LogLevel, optional
            The logging level to use.
        exit_on_error : bool, optional
            Whether to exit on error conditions.
        rank : int, optional
            Rank identifier for MPI-based logging.
        again : bool, optional
            Whether to allow re-initialization.
        """
        if self.logger is not None and not again:
            raise RuntimeError("logger already initialized and again is False")
        self.exit_on_error = exit_on_error

        logging_config: dict = {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "plain": {
                    # pylint: disable=line-too-long
                    # Uncomment to report verbose output in logs; try to keep these two in sync
                    # "format": f"[%(name)s][%(levelname)s][%(asctime)s][%(pathname)s:%(lineno)d][%(process)d][%(thread)d][rank={rank}]: %(message)s" # noqa: E501
                    "format": f"[%(name)s][%(levelname)s][%(asctime)s][%(filename)s:%(lineno)d][rank={rank}]: %(message)s"
                    # pylint: enable=line-too-long
                },
            },
            "handlers": {
                "default": {
                    "formatter": "plain",
                    "class": "logging.StreamHandler",
                    "stream": "ext://sys.stdout",
                    "filters": [],
                },
            },
            "loggers": {
                _PROJECT_NAME: {
                    "handlers": ["default"],
                    "level": getattr(logging, log_level.value.upper()),  # pylint: disable=no-member
                },
            },
        }
        logging.config.dictConfig(logging_config)
        self.logger = logging.getLogger(_PROJECT_NAME)
        self("logging initialized")

    def _get_logger_(self) -> logging.Logger:
        if self.logger is None:
            raise ValueError
        return self.logger


LOGGER = LoggerWrapper()
LOGGER.initialize(log_level=LogLevel.DEBUG)
