"""Use of :class:`.IOMixin` as basis for a wrapper."""
from __future__ import annotations

import logging
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Any
from zipfile import ZIP_STORED
from zoneinfo import ZoneInfo

from etoolbox.datazip.mixin import IOMixin

LOGGER = logging.getLogger(__name__)


class IOWrapper(IOMixin):
    """Wrapper to add :class:`.IOMixin` to an existing object."""

    __slots__ = ("_obj", "_metadata", "_recipes")

    def __init__(self, obj: Any, recipes: dict | None = None):
        """Create an IOWrapper.

        Args:
            obj: the object to wrap
            recipes: add more customization on how attributes will be stored
                organized like :py:const:`etoolbox.datazip.core.RECIPES`
        """
        self._obj = obj
        self._metadata: dict = {
            "created": str(datetime.now(tz=ZoneInfo("UTC"))),
        }
        self._recipes: dict = {} if recipes is None else recipes

    def __getattr__(self, item) -> Any:
        """Pretend to be `self._obj`."""
        return getattr(self._obj, item)

    def to_file(
        self,
        path: Path | str | BytesIO,
        compression=ZIP_STORED,
        clobber=False,
        **kwargs,
    ) -> None:
        """Write out the obj to a file."""
        self._to_file(
            self._obj,
            path,
            compression,
            clobber,
            metadata=self._metadata,
            recipes=self._recipes,
            **kwargs,
        )

    @classmethod
    def from_file(cls, path: Path | str | BytesIO, **kwargs) -> Any:
        """Recreate an instance of :class:`.DataZipWrapper` with the wrapped object."""
        obj, metadata = cls._from_file(None, path, **kwargs)
        self = cls(obj)
        self._metadata = metadata
        return self

    def __repr__(self) -> str:
        return self.__class__.__qualname__ + f"({repr(self._obj)})"
