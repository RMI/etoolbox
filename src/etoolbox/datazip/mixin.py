"""A mixin for adding :class:`.DataZip` functionality to another class.

The goal is to build this out so that most custom classes can be stored
and recovered by inheriting this mixin.
"""
from __future__ import annotations

import getpass
import inspect
import logging
from contextlib import suppress
from datetime import datetime
from importlib import import_module
from io import BytesIO
from pathlib import Path
from typing import Any
from zipfile import ZIP_STORED
from zoneinfo import ZoneInfo

from etoolbox import __version__
from etoolbox.datazip.core import DataZip

LOGGER = logging.getLogger(__name__)


def _get_version(obj: Any) -> str:
    mod = import_module(obj.__class__.__module__.partition(".")[0])
    for v_attr in ("__version__", "version", "release"):
        if hasattr(mod, v_attr):
            return getattr(mod, v_attr)
    return "unknown"


def _get_username():
    try:
        return getpass.getuser()
    except ModuleNotFoundError as exc0:
        import os

        try:
            return os.getlogin()
        except Exception as exc1:
            LOGGER.error("No username %r from %r", exc1, exc0)
            return "unknown"


def _type_check(cls: any, meta: dict, path: Path) -> None:
    if meta["__qualname__"] != cls.__qualname__:
        raise TypeError(
            f"{path.name} represents a `{meta['__qualname__']}` which "
            f"is not compatible with `{cls.__qualname__}.from_file()`"
        )


class IOMixin:
    """Mixin for adding :class:`.DataZip` IO."""

    # in case child uses __slots__
    __slots__ = ()
    recipes = {}

    @classmethod
    def from_dz(cls, *args, **kwargs) -> Any:
        """Alias for :meth:`.IOMixin.from_file`."""
        return cls.from_file(*args, **kwargs)

    @classmethod
    def from_file(cls, path: Path | str | BytesIO, **kwargs) -> Any:
        """Recreate object fom file or buffer."""
        out, metadata = cls._from_file(cls, path, **kwargs)
        with suppress(Exception):
            setattr(out, "_metadata", metadata)
        return out

    @staticmethod
    def _from_file(cls, path: Path | str | BytesIO, **kwargs) -> tuple[Any, dict]:
        with DataZip(path, "r") as z:
            metadata = z.readm()

            if cls is None:
                cls = getattr(
                    import_module(metadata["__module__"]), metadata["__qualname__"]
                )
            else:
                _type_check(cls, metadata, path)

            data_dict = {name: z.read(name) for name in metadata["attr_list"]}

        sig = inspect.signature(cls).parameters
        self = cls(
            **{k: v for k, v in data_dict.items() if k in sig},
        )
        for k, v in data_dict.items():
            if k not in sig:
                setattr(self, k, v)

        return self, metadata

    def to_dz(self, *args, **kwargs) -> None:
        """Alias for :meth:`.IOMixin.to_file`."""
        self.to_file(*args, **kwargs)

    def to_file(
        self,
        path: Path | str | BytesIO,
        compression=ZIP_STORED,
        clobber=False,
        **kwargs,
    ) -> None:
        """Write object to file or buffer."""
        self._to_file(
            self,
            path,
            compression,
            clobber,
            recipes=self.recipes | kwargs.get("recipes", {}),
            **kwargs,
        )

    @staticmethod
    def _to_file(
        self,
        path: Path | str | BytesIO,
        compression,
        clobber,
        **kwargs,
    ) -> None:
        if isinstance(path, (str, Path)):
            if Path(path).with_suffix(".zip").exists() and not clobber:
                raise FileExistsError(f"{path} exists, to overwrite set `clobber=True`")
            if clobber:
                Path(path).with_suffix(".zip").unlink(missing_ok=True)
        metadata = {
            "__module__": self.__class__.__module__,
            "__qualname__": self.__class__.__qualname__,
            "__obj_version": _get_version(self),
            "__io_version__": __version__,
            "__created_by__": _get_username(),
            "__file_created__": str(datetime.now(tz=ZoneInfo("UTC"))),
            **kwargs.get("metadata", {}),
        }
        with DataZip(
            path, "w", compression=compression, recipes=kwargs.get("recipes", {})
        ) as z:
            attr_list = []
            if hasattr(self, "__slots__"):
                for attr_name in self.__slots__:
                    try:
                        written = z.writed(attr_name, getattr(self, attr_name))
                    except AttributeError:
                        pass
                    else:
                        if written:
                            attr_list.append(attr_name)
            if hasattr(self, "__dict__"):
                for attr_name, attr_value in self.__dict__.items():
                    if attr_name not in attr_list:
                        written = z.writed(attr_name, attr_value)
                        if written:
                            attr_list.append(attr_name)
            z.writem(None, metadata | {"attr_list": attr_list})
