"""Durable, operator-controlled scheduling for scientific GPU experiments."""

from importlib.metadata import PackageNotFoundError, version


try:
    __version__ = version("experiment-queue")
except PackageNotFoundError:
    __version__ = "0+unknown"


__all__ = ["__version__"]
