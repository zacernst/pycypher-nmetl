"""Just some enums for initializers."""

from enum import Enum


class ComputeClassNameEnum(str, Enum):
    """Enum for the back end to use for the session."""

    THREADING = "threading"
    MULTIPROCESSING = "multiprocessing"
    DASK = "dask"


class LoggingLevelEnum(str, Enum):
    """Enum for the logging level to use for the session."""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"
