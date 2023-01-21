"""Code for :class:`.DataZip`."""
from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Generator
from contextlib import suppress
from datetime import datetime
from importlib import import_module
from io import BytesIO
from pathlib import Path
from typing import Any
from zipfile import ZipFile, ZipInfo
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from etoolbox import __version__
from etoolbox.datazip._json import json_dumps, json_loads
from etoolbox.datazip._types import RECIPES, STD_TYPES, DZable, DZableObj
from etoolbox.datazip._utils import (
    _get_username,
    _get_version,
    _objinfo,
    obj_from_recipe,
)

LOGGER = logging.getLogger(__name__)


class DataZip(ZipFile):
    """A :class:`ZipFile` with methods for easier use with Python objects."""

    def __init__(
        self, file: str | Path | BytesIO, mode="r", recipes=None, *args, **kwargs
    ):
        """Create a DataZip.

        Args:
            file: Either the path to the file, or a file-like object.
                If it is a path, the file will be opened and closed by DataZip.
            mode: The mode can be either read 'r', or write 'w'.
            recipes: add more customization on how objects will be stored that cannot
                be stored as JSON and where adding ``__getstate__`` and
                ``__setstate__`` is not feasible. Organized like
                :py:const:`etoolbox.datazip.core.RECIPES`.
            compression: ZIP_STORED (no compression), ZIP_DEFLATED (requires zlib),
                ZIP_BZIP2 (requires bz2) or ZIP_LZMA (requires lzma).

        Examples
        --------
        First we can create a :class:`.DataZip`. In this case we are using a buffer
        (:class:`io.BytesIO`) for convenience. In most cases though, ``file`` would be
        a :class:`pathlib.Path` or :class:`str` that represents a file. In these cases
        a ``.zip`` extension will be added if it is not there already.

        >>> buffer = BytesIO()  # can also be a file-like object
        >>> with DataZip(file=buffer, mode="w") as z0:
        ...     z0["series"] = pd.Series([1, 2, 4], name="series")
        ...     z0["df"] = pd.DataFrame({(0, "a"): [2.4, 8.9], (0, "b"): [3.5, 6.2]})
        ...     z0["foo"] = {
        ...         "a": (1, (2, {3})),
        ...         "b": frozenset({1.5, 3}),
        ...         "c": 0.9 + 0.2j,
        ...     }
        ...

        Getting items from :class:`.DataZip`, like setting them, uses standard Python
        subscripting.

        For :class:`pandas.DataFrame`, it stores them as ``parquet`` and preserves
        :class:`pandas.MultiIndex` columns, even when they cannot normally be stored
        in a ``parquet`` file.

        >>> with DataZip(buffer, "r") as z1:
        ...     z1["df"]  # doctest: +NORMALIZE_WHITESPACE
        ...
             0
             a    b
        0  2.4  3.5
        1  8.9  6.2

        While always preferable to use a context manager as above, here it's more
        convenient to keep the object open.

        >>> z1 = DataZip(buffer, "r")
        >>> z1["series"]
        0    1
        1    2
        2    4
        Name: series, dtype: int64

        Even more unusual types that can't normally be stored in json should work.

        >>> z1["foo"]
        {'a': (1, (2, {3})), 'b': frozenset({1.5, 3}), 'c': (0.9+0.2j)}

        Checking to see if an item is in a :class:`.DataZip` uses standard Python
        syntax.

        >>> "series" in z1
        True

        You can also check by filename. And check the number of items.

        >>> "series.parquet" in z1
        True

        >>> len(z1)
        3

        When not used with a context manager, :class:`.DataZip` should close itself
        automatically but it's not a bad idea to make sure.

        >>> z1.close()

        A :class:`.DataZip` is a write-once, read-many affair because of the way
        ``zip`` files work. Appending to a :class:`.DataZip` effectively means copying
        everything.

        >>> buffer1 = BytesIO()
        >>> with DataZip(buffer, "r") as zold, DataZip(buffer1, "w") as znew:
        ...     for k, v in zold.items():
        ...         znew[k] = v
        ...     znew["new"] = "foo"
        ...

        """
        if mode in ("a", "x"):
            raise ValueError("DataZip does not support modes 'a' or 'x'")

        if isinstance(file, str):
            file = Path(file)

        clobber = kwargs.pop("clobber", False)
        if isinstance(file, Path):
            file = file.with_suffix(".zip")
            if file.exists() and mode == "w":
                if clobber:
                    file.unlink()
                else:
                    raise FileExistsError(
                        f"{file} exists, you cannot write or append to an existing DataZip."
                    )

        super().__init__(file, mode, *args, **kwargs)
        self._no_pqt_cols, self._obj_meta, self._other_meta = {}, {}, {}
        self._attributes, self._contents = {}, defaultdict(list)
        self._recipes = {} if recipes is None else recipes
        if mode == "r":
            self._attributes = self._json_get(
                "__attributes__", "attributes", "other_attrs"
            )
            md = self._json_get("__metadata__", "metadata")
            for attr in (
                "obj_meta",
                "contents",
                "other_meta",
            ):
                setattr(self, f"_{attr}", md.get(attr, self._json_get(attr)))
            self._no_pqt_cols = md.get(
                "no_pqt_cols", md.get("bad_cols", self._json_get("bad_cols"))
            )
            with suppress(KeyError):
                self._recipes = {tuple(k): v for k, v in md["recipes"]}
        self._delete_on_close = None

    @staticmethod
    def dump(obj: DZableObj, file: Path | str | BytesIO, **kwargs) -> None:
        """Write the DataZip representation of ``obj`` to ``file``.

        Args:
            obj: A Python object, it must implement ``__getstate__`` and
                ``__setstate__``. There are other restrictions, especially if it
                contains instances of other custom Python objects, it may be enough
                for all of them to implement ``__getstate__`` and ``__setstate__``.
            file: a file-like object, or a buffer where the :class:`DataZip`
                will be saved.

        Returns:
            None

        Examples
        --------
        Create an object that you would like to save as a :class:`.DataZip`.

        >>> from etoolbox.datazip.test_classes import TestKlass
        >>> obj = TestKlass(a=5, b={"c": [2, 3.5]})
        >>> obj
        TestKlass(a=5, b={'c': [2, 3.5]})

        Save the object as a :class:`.DataZip`.

        >>> buffer = BytesIO()
        >>> DataZip.dump(obj, buffer)
        >>> del obj

        Get it back.

        >>> obj = DataZip.load(buffer)
        >>> obj
        TestKlass(a=5, b={'c': [2, 3.5]})

        """
        if not all((hasattr(obj, "__setstate__"), hasattr(obj, "__getstate__"))):
            raise ValueError(
                "To dump an object, it must implement __setstate__ and __getstate__."
            )
        x = obj.__getstate__()
        if not isinstance(x, dict):
            raise TypeError("__getstate__ does not return a dict.")
        with DataZip(file, "w", **kwargs) as self:
            self._obj_meta["self"] = _objinfo(obj)
            self._other_meta.update(
                {
                    "__obj_version__": _get_version(obj),
                    "__io_version__": __version__,
                    "__created_by__": _get_username(),
                    "__file_created__": str(datetime.now(tz=ZoneInfo("UTC"))),
                    "__attr_list__": x.pop("attr_list", list(x.keys())),
                },
            )
            for k, v in x.items():
                self[k] = v

    @staticmethod
    def load(file: Path | str | BytesIO, klass: type = None):
        """Return the reconstituted object specified in the file.

        Args:
            file: a file-like object, or a buffer from which the :class:`DataZip`
                will be read.
            klass: (Optional) allows passing the class when it is known, this
                is handy when it is not possible to import the module that defines
                the class that ``file`` represents.

        Returns:
            Object from :class:`DataZip`.

        Examples
        --------
        See :meth:`.DataZip.dump` for examples.

        """
        with DataZip(file, "r") as self:
            if klass is None:
                try:
                    mod, qname, _ = self._obj_meta["self"]
                    klass: type = getattr(import_module(mod), qname)
                except KeyError as exc:
                    raise ValueError(
                        f"Unable to use DataZip.load because {self.filename} does not contain module and class metadata."
                    ) from exc
            obj = klass.__new__(klass)
            obj.__setstate__(
                {
                    k: self[k]
                    for k in self._other_meta["__attr_list__"]
                    if k in self._contents
                }
            )
        return obj

    @classmethod
    def replace(cls, file_or_new_buffer, old_buffer=None, save_old=False):
        """Replace an old :class:`DataZip` with an editable new one.

        Args:
            file_or_new_buffer: Either the path to the file to be replaced
                or the new buffer.
            old_buffer: only required if ``file_or_new_buffer`` is a buffer.
            save_old: if True, the old :class:`DataZip` will be
                saved with "_old" appended, if False it will be
                deleted when the new :class:`DataZip` is closed.

        Returns:
            New editable :class:`DataZip` with old data copied into it.

        Examples
        --------
        Create a new test file object and put a datazip in it.
        >>> file = Path.home() / "test.zip"
        >>> with DataZip(file=file, mode="w") as z0:
        ...     z0["series"] = pd.Series([1, 2, 4], name="series")
        ...

        Create a replacement DataZip.
        >>> z1 = DataZip.replace(file, save_old=False)

        The replacement has the old content.
        >>> z1["series"]
        0    1
        1    2
        2    4
        Name: series, dtype: int64

        We can also now add to it.
        >>> z1["foo"] = "bar"

        While the replacement is open, the old verion still exists.
        >>> (Path.home() / "test_old.zip").exists()
        True

        Now we close the replacement which deletes the old file.
        >>> z1.close()
        >>> (Path.home() / "test_old.zip").exists()
        False

        Reopening the replacement, we see it contains all the objects.
        >>> z2 = DataZip(file, "r")

        >>> z2["series"]
        0    1
        1    2
        2    4
        Name: series, dtype: int64

        >>> z1["foo"]
        'bar'

        And now some final test cleanup.
        >>> z2.close()
        >>> file.unlink()

        """
        if isinstance(file_or_new_buffer, BytesIO) and not isinstance(
            old_buffer, BytesIO
        ):
            raise TypeError(
                "If file_or_new_buffer is BytesIO, then old_buffer must be as well."
            )

        _to_delete = None
        if isinstance(file_or_new_buffer, str):
            file_or_new_buffer = Path(file_or_new_buffer)

        if isinstance(file_or_new_buffer, Path):
            file_or_new_buffer = file_or_new_buffer.with_suffix(".zip")
            old_buffer = Path(
                str(file_or_new_buffer).removesuffix(".zip") + "_old"
            ).with_suffix(".zip")
            file_or_new_buffer.rename(old_buffer)
            if not save_old:
                _to_delete = old_buffer

        self = cls(file_or_new_buffer, "w")
        with DataZip(old_buffer, "r") as z:
            for k, v in z.items():
                self[k] = v

        self._delete_on_close = _to_delete

        return self

    def read(self, name: str | ZipInfo, pwd: bytes | None = ..., super_=False) -> Any:
        """Return obj or bytes for name."""
        stem, _, suffix = name.partition(".")
        if self._contents.get(stem, False):
            return self._recursive_read(stem)
        if suffix == "parquet" or f"{stem}.parquet" in self.namelist():
            return self._read_df(stem)
        if (suffix == "zip" or f"{stem}.zip" in self.namelist()) and not super_:
            io = BytesIO(super().read(stem + ".zip"))
            # if stem in self._obj_meta:
            #     return obj_from_recipe(io, *self._obj_meta[stem])
            return DataZip.load(io)
        if suffix == "npy" or f"{stem}.npy" in self.namelist():
            return np.load(BytesIO(super().read(stem + ".npy")))
        if stem in self._attributes:
            out = self._attributes[stem]
            if stem not in self._obj_meta:
                return out
            return obj_from_recipe(out, *self._obj_meta[stem])
        return super().read(name)

    def read_dfs(self) -> Generator[tuple[str, pd.DataFrame | pd.Series]]:
        """Read all dfs lazily."""
        for name, *suffix in map(lambda x: x.split("."), self.namelist()):
            if "parquet" in suffix:
                yield name, self.read(name)

    def writed(
        self,
        name: str,
        data: DZable,
        **kwargs,
    ) -> bool:
        """Write dict, df, str, or some other objects to name."""
        name, _, suffix = name.partition(".")
        if name in ("__metadata__", "__attributes__"):
            raise ValueError(f"`{name}` is reserved, please use a different name")
        if name in self._contents:
            raise FileExistsError(f"{name} already in {self.filename}")
        if isinstance(data, (pd.DataFrame, pd.Series)):
            return self._write_df(name, data, **kwargs)
        if isinstance(data, STD_TYPES) and not isinstance(data, pd.Timestamp):
            try:
                return self._write_jsonable(name, data)
            except TypeError:
                return self._recursive_write(name, data)
        if hasattr(data, "write_image"):
            return self._write_image(name, data)
        if isinstance(data, np.ndarray):
            return self._write_numpy(name, data, **kwargs)
        if hasattr(data, "__getstate__") and hasattr(data, "__setstate__"):
            with suppress(TypeError):
                DataZip.dump(data, temp := BytesIO())
                self.writestr(f"{name}.zip", temp.getvalue())
                _ = self._contents[name]
                return True
        if (obj_info := _objinfo(data)) in self.recipes:
            return self._write_using_recipe(name, data, obj_info)
        return self._write_as_str(name, data, **kwargs)

    def close(self) -> None:
        """Close the file, and for mode 'w' write attributes and metadata."""
        if self.fp is None:
            return

        if self.mode == "w":
            self.writestr(
                "__attributes__.json",
                json_dumps(self._attributes),
            )
            self.writestr(
                "__metadata__.json",
                json_dumps(
                    {
                        "contents": self._contents,
                        "no_pqt_cols": self._no_pqt_cols,
                        "obj_meta": self._obj_meta,
                        "recipes": list(self._recipes.items()),
                        "other_meta": self._other_meta,
                    },
                ),
            )
        super().close()
        if isinstance(self._delete_on_close, Path):
            self._delete_on_close.unlink()

    def __contains__(self, item) -> bool:
        """Provide ``in`` check."""
        return item.partition(".")[0] in self._contents

    def __len__(self) -> int:
        """Provide for use of ``len`` builtin."""
        return len(self._contents)

    def __getitem__(self, item: str) -> DZable:
        """Alias for :meth:`.DataZip.read`."""
        return self.read(item)

    def __setitem__(self, key: str, value: DZable) -> None:
        """Alias for :meth:`.DataZip.writed`."""
        self.writed(key, value)

    def keys(self):
        """Set of names in DataZip as if it was a dict."""
        return self._contents.keys()

    def items(self):
        """Like a :meth:`dict.items`."""
        for k in self._contents.keys():
            yield k, self[k]

    @property
    def recipes(self) -> dict[tuple, dict]:
        """Combination of internal and constant recipes."""
        return RECIPES | self._recipes

    @classmethod
    def dfs_to_zip(cls, path: Path, df_dict: dict[str, pd.DataFrame], clobber=False):
        """Create a zip of parquets.

        Args:
            df_dict: dict of dfs to put into a zip
            path: path for the zip
            clobber: if True, overwrite exiting file with same path

        Returns: None

        """
        path = path.with_suffix(".zip")
        if path.exists():
            if not clobber:
                raise FileExistsError(f"{path} exists, to overwrite set `clobber=True`")
            path.unlink()
        with cls(path, "w") as z:
            other_stuff = {}
            for key, val in df_dict.items():
                if isinstance(val, (pd.Series, pd.DataFrame, dict)):
                    z.writed(key, val)
                elif isinstance(val, (float, int, str, tuple, dict, list)):
                    other_stuff.update({key: val})
            z.writed("other_stuff", other_stuff)

    @classmethod
    def dfs_from_zip(cls, path: Path) -> dict:
        """Dict of dfs from a zip of parquets.

        Args:
            path: path of the zip to load

        Returns: dict of dfs

        """
        with cls(path, "r") as z:
            out_dict = dict(z.read_dfs())
            try:
                other = z.read("other_stuff")
            except KeyError:
                other = {}
            out_dict = out_dict | other

        return out_dict

    def _json_get(self, *args):
        for arg in args:
            with suppress(Exception):
                return json_loads(super().read(f"{arg}.json"))
        return {}

    def _recursive_read(self, stem: str) -> list | dict:
        if "dict" in self._obj_meta[stem][1]:
            out_dict = {}
            for k in self._contents[stem]:
                key = k.rpartition("<||>")[2]
                try:
                    out_dict[key] = self.read(k)
                except KeyError as exc:
                    LOGGER.error("Error loading %s in %s (%s). %r", key, stem, k, exc)
            if self._obj_meta[stem][1] == "defaultdict":
                return defaultdict(lambda: None, out_dict)
            return out_dict
        else:
            out_list = []
            for k in self._contents[stem]:
                try:
                    out_list.append(self.read(k))
                except KeyError as exc:
                    LOGGER.error("Error loading %s in %s. %r", k, stem, exc)
            return out_list

    def _read_df(self, name: str) -> pd.DataFrame | pd.Series:
        out = pd.read_parquet(BytesIO(super().read(name + ".parquet")))
        is_series = "Series" == self._obj_meta.get(name, ("", "Series", ""))[1]
        if name not in self._no_pqt_cols:
            if is_series:
                return out.squeeze()
            return out
        cols, names = self._no_pqt_cols[name]
        if isinstance(names, (tuple, list)) and len(names) > 1:
            idx = pd.MultiIndex.from_tuples(cols, names=names)
        else:
            idx = pd.Index(cols, name=names[0])
        if is_series:
            return out.set_axis(idx, axis=1).squeeze()
        return out.set_axis(idx, axis=1)

    def _recursive_write(self, name: str, data: dict | list | tuple) -> bool:
        if isinstance(data, dict):
            item_iter = data.items()
        elif isinstance(data, (tuple, list)):
            item_iter = enumerate(data)
        else:
            raise TypeError(f"{name} is a {data.__class__} which is not supported")
        self._obj_meta.update({name: _objinfo(data)})
        all_good = []
        for k, v in item_iter:
            good = self.writed(f"{name}<||>{k}", v)
            all_good.append(good)
            if good:
                self._contents[name].append(f"{name}<||>{k}")
        return any(all_good)

    def _write_numpy(self, name: str, data: np.ndarray, **kwargs) -> bool:
        np.save(temp := BytesIO(), data, allow_pickle=False)
        self.writestr(f"{name}.npy", temp.getvalue())
        self._obj_meta.update({name: _objinfo(data)})
        _ = self._contents[name]
        return True

    def _write_jsonable(
        self,
        name: str,
        obj: dict[int | str, list | tuple | dict | str | float | int] | list,
        **kwargs,
    ) -> bool:
        """Write a dict in the ZIP as json."""
        to_write = obj
        _ = json_dumps(to_write)
        self._attributes.update({name: to_write})
        _ = self._contents[name]
        return True

    def _write_using_recipe(self, name: str, data: DZable, obj_info) -> bool:
        recipe = self.recipes[obj_info]
        if not recipe["keep"]:
            return False
        if recipe["method"] == "by_attribute":
            to_write = {
                attr: (func(getattr(data, attr)) if func else getattr(data, attr))
                for attr, func in recipe["attributes"]
            }
        elif recipe["method"] == "as_str":
            to_write = str(data)
        _ = json_dumps(to_write)
        self._attributes.update({name: to_write})
        self._obj_meta.update({name: recipe["constructor"]})
        _ = self._contents[name]
        return True

    def _write_as_str(self, name: str, obj: Any, **kwargs) -> bool:
        """Write an object as whatever str is in the parentheses of its repr."""
        obj_info = _objinfo(obj)
        self._attributes.update(
            {name: repr(obj).removeprefix(obj_info[1] + "(").removesuffix(")")}
        )
        self._obj_meta.update({name: obj_info})
        _ = self._contents[name]
        return True

    def _write_df(self, name: str, df: pd.DataFrame | pd.Series, **kwargs) -> bool:
        """Write a df in the ZIP as parquet."""
        self._obj_meta.update({name: _objinfo(df)})
        if isinstance(df, pd.Series):
            df = df.to_frame(name=name)
        try:
            self.writestr(f"{name}.parquet", df.to_parquet())
        except ValueError:
            self._no_pqt_cols.update({name: (list(df.columns), list(df.columns.names))})
            self.writestr(f"{name}.parquet", self._str_cols(df).to_parquet())
        _ = self._contents[name]
        return True

    def _write_image(self, name: str, data: Any, **kwargs) -> bool:
        data.write_image(temp := BytesIO(), format="pdf")
        self.writestr(f"{name}.pdf", temp.getvalue())
        self._obj_meta.update({name: _objinfo(data)})
        _ = self._contents[name]
        return True

    @staticmethod
    def _str_cols(df: pd.DataFrame, *args) -> pd.DataFrame:
        return df.set_axis(list(map(str, range(df.shape[1]))), axis="columns")
