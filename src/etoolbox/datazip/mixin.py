"""A mixin for adding :class:`.DataZip` functionality to another class."""
from __future__ import annotations

import inspect

# import pickle
from importlib import import_module
from io import BytesIO
from pathlib import Path
from zipfile import ZIP_STORED

from etoolbox import __version__
from etoolbox.datazip.core import DataZip


class IOMixin:
    """Mixin for adding :class:`.DataZip` IO."""

    __slots__ = ()

    @classmethod
    def from_dz(cls, *args, **kwargs):
        """Alias for :meth:`.IOMixin.from_file`."""
        return cls.from_file(*args, **kwargs)

    # Disabling pickle IO because bandit doesn't like it and implementation is
    # comparatively trivial
    # @classmethod
    # def from_pickle(cls, path: Path | str, **kwargs):
    #     with open(path.with_suffix(".pkl"), "rb") as f:
    #         temp = pickle.load(f)
    #     return temp
    #
    # def to_pickle(self, path: Path | str, **kwargs):
    #     self._to_pickle(self, path, **kwargs)
    #
    # @staticmethod
    # def _to_pickle(self, path: Path | str, **kwargs):
    #     with open(path.with_suffix(".pkl"), "wb") as f:
    #         pickle.dump(self, f, pickle.HIGHEST_PROTOCOL)

    @classmethod
    def from_file(cls, path: Path | str | BytesIO, **kwargs):
        """Recreate object fom file."""
        out, _ = cls._from_file(cls, path, cls_from_meta=False, **kwargs)
        return out

    @staticmethod
    def _from_file(cls, path: Path | str | BytesIO, cls_from_meta, **kwargs):

        data_dict = {}
        _dfs = {}
        with DataZip(path, "r") as z:
            metadata = z.read("metadata")

            if cls_from_meta:
                cls = getattr(
                    import_module(metadata["__module__"]), metadata["__qualname__"]
                )
            else:
                IOMixin._type_check(cls, metadata, path)

            for name in z.contents:
                # if name.startswith("_dfs_"):
                #     _dfs.update({name.removeprefix("_dfs_"): z.read(name)})
                if "<||>" not in name:
                    data_dict.update({name: z.read(name)})

        sig = inspect.signature(cls).parameters
        self = cls(
            **{k: v for k, v in data_dict.items() if k in sig},
        )
        if _dfs:
            self._dfs.update(_dfs)
        for k, v in data_dict.items():
            if k not in sig:
                setattr(self, k, v)

        return self, metadata

    def to_dz(self, *args, **kwargs):
        """Alias for :meth:`.IOMixin.to_file`."""
        return self.to_file(*args, **kwargs)

    def to_file(
        self,
        path: Path | str | BytesIO,
        compression=ZIP_STORED,
        clobber=False,
        **kwargs,
    ):
        """Write object to file."""
        self._to_file(self, path, compression, clobber, **kwargs)

    @staticmethod
    def _to_file(
        self,
        path: Path | str | BytesIO,
        compression,
        clobber,
        recipes=None,
        **kwargs,
    ):
        if isinstance(path, (str, Path)):
            if Path(path).with_suffix(".zip").exists() and not clobber:
                raise FileExistsError(f"{path} exists, to overwrite set `clobber=True`")
            if clobber:
                Path(path).with_suffix(".zip").unlink(missing_ok=True)

        recipes = recipes if recipes is not None else {}

        with DataZip(path, "w", compression=compression, recipes=recipes) as z:
            metadata = {
                "__qualname__": self.__class__.__qualname__,
                "__module__": self.__class__.__module__,
                "__version__": __version__,
            }
            if hasattr(self, "_metadata"):
                metadata = metadata | self._metadata | kwargs.get("metadata", {})

            if hasattr(self, "__slots__"):
                __dict__ = (
                    (attr_name, getattr(self, attr_name))
                    for attr_name in self.__slots__
                )
            else:
                __dict__ = self.__dict__.items()
            for attr_name, attr_value in __dict__:
                if "__" not in attr_name:
                    z.writed(attr_name, attr_value)
            z.writed("metadata", metadata)

    @staticmethod
    def _type_check(cls, meta, path):
        if meta["__qualname__"] != cls.__qualname__:
            raise TypeError(
                f"{path.name} represents a `{meta['__qualname__']}` which "
                f"is not compatible with `{cls.__qualname__}.from_file()`"
            )
        del meta["__qualname__"]
