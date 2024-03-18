"""Core :class:`.DataZip` object plus mixins and wrappers."""

from etoolbox.datazip.core import DataZip
from etoolbox.datazip.mixin import IOMixin

__all__ = ["DataZip", "IOMixin"]
