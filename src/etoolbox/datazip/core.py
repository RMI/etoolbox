"""Code for :class:`.DataZip`."""
from __future__ import annotations

import getpass
import json
import logging
from collections import defaultdict
from collections.abc import Generator, Mapping, Sequence
from contextlib import suppress
from datetime import datetime
from functools import partial
from importlib import import_module
from io import BytesIO
from pathlib import Path
from typing import Any, Protocol
from zipfile import ZipFile, ZipInfo
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from etoolbox import __version__

LOGGER = logging.getLogger(__name__)

RECIPES: dict[tuple, dict] = {
    ("datetime", "datetime", None): {
        "method": "as_str",
        "attributes": None,
        "keep": True,
        "constructor": ("datetime", "datetime", None),
    },
    ("utils._libs.tslibs.timestamps", "Timestamp", None): {
        "method": "as_str",
        "attributes": None,
        "keep": True,
        "constructor": ("utils._libs.tslibs.timestamps", "Timestamp", None),
    },
    ("pudl.workspace.datastore", "Datastore", None): {
        "method": None,
        "attributes": None,
        "keep": False,
        "constructor": ("pudl.workspace.datastore", "Datastore", None),
    },
    ("sqlalchemy.engine.base", "Engine", None): {
        "method": "by_attribute",
        "attributes": (("url", str),),
        "keep": True,
        "constructor": ("sqlalchemy.engine.create", None, "create_engine"),
    },
}


class DZableObj(Protocol):
    """Protocol for an object that can be serialized with :class:`DataZip`."""

    def __getstate__(self) -> dict:
        ...

    def __setstate__(self, state: dict) -> None:
        ...


DZable = (
    float
    | int
    | set
    | frozenset
    | Mapping
    | Sequence
    | DZableObj
    | np.ndarray
    | pd.DataFrame
    | pd.Series
    | None
)


class _TypeHintingEncoder(json.JSONEncoder):
    def encode(self, obj):
        def hint_tuples(item):
            if isinstance(item, (str, int, float, type(None))):
                return item
            if isinstance(item, tuple):
                return {"__tuple__": True, "items": [hint_tuples(e) for e in item]}
            if isinstance(item, dict):
                return {key: hint_tuples(value) for key, value in item.items()}
            if isinstance(item, list):
                return [hint_tuples(e) for e in item]
            if isinstance(item, set):
                return {"__set__": True, "items": [hint_tuples(e) for e in item]}
            if isinstance(item, frozenset):
                return {"__frozenset__": True, "items": [hint_tuples(e) for e in item]}
            if isinstance(item, complex):
                return {"__complex__": True, "real": item.real, "imag": item.imag}
            return item

        return super().encode(hint_tuples(obj))


def _type_hinted_hook(obj: Any) -> Any:
    if "__tuple__" in obj:
        return tuple(obj["items"])
    if "__set__" in obj:
        return set(obj["items"])
    if "__frozenset__" in obj:
        return frozenset(obj["items"])
    if "__complex__" in obj:
        return complex(obj["real"], obj["imag"])
    return obj


json_dumps = partial(json.dumps, ensure_ascii=False, indent=4, cls=_TypeHintingEncoder)


def obj_from_recipe(
    thing: Any,
    module: str,
    klass: str | None = None,
    constructor: str | None = None,
) -> Any:
    """Recreate an object from a recipe.

    Can be module.klass(thing), module.constructor(thing), or
    module.klass.constructor(thing).

    Args:
        thing: what will be passed to ``constructor`` / ``__init__``
        module: the module that the function or class can be found in
        klass: The __qualname__ of the class, if needed. If None, then
            ``constructor`` must be a function.
        constructor: A constructor method on a class if ``klass`` is specified,
            otherwise a function that returns the desired object. If None, the
            class ``klass`` will be created using its ``__init__``.

    """
    if isinstance(thing, str):
        thing = thing.replace("'", "").replace('"', "")
    if module == "builtins" or thing is None:
        return thing

    if constructor is None and klass is not None:
        init_or_const = getattr(import_module(module), klass)
    elif constructor is not None and klass is None:
        init_or_const = getattr(import_module(module), constructor)
    elif all((klass, constructor)):
        init_or_const = getattr(getattr(import_module(module), klass), constructor)
    else:
        raise AssertionError("Must specify at least one of `klass` and `constructor`")

    if isinstance(thing, dict):
        return init_or_const(**thing)
    if isinstance(thing, (tuple, list)):
        return init_or_const(*thing)
    return init_or_const(thing)


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
            recipes: add more customization on how attributes will be stored
                organized like :py:const:`etoolbox.datazip.core.RECIPES`
            compression: ZIP_STORED (no compression), ZIP_DEFLATED (requires zlib),
                ZIP_BZIP2 (requires bz2) or ZIP_LZMA (requires lzma).

        Examples
        --------
        First we can create a

        >>> buffer = BytesIO()  # can also be a file-like object
        >>> with DataZip(buffer, "w") as z0:
        ...     z0["series"] = pd.Series([1, 2, 4], name="series")
        ...     z0["df"] = pd.DataFrame({(0, "a"): [2.4, 8.9], (0, "b"): [3.5, 6.2]})
        ...     z0["foo"] = {
        ...         "a": (1, (2, {3})),
        ...         "b": frozenset({1.5, 3}),
        ...         "c": 0.9 + 0.2j,
        ...     }
        ...

        Getting items from :class:`.DataZip`, like setting them uses standard Python
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

        When not used with a context manager, it should close itself automatically
        but it's not a bad idea to make sure.

        >>> z1.close()

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
            setattr(
                self,
                "_no_pqt_cols",
                md.get("no_pqt_cols", md.get("bad_cols", self._json_get("bad_cols"))),
            )
            with suppress(KeyError):
                self._recipes = {tuple(k): v for k, v in md["recipes"]}

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
        with DataZip(file, "w", **kwargs) as self:
            self._obj_meta["self"] = self._objinfo(obj)
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
            return obj_from_recipe(
                self._attributes[stem], *self._obj_meta.get(stem, (None, None, None))
            )
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
        if isinstance(
            data,
            (complex, dict, frozenset, float, int, list, tuple, set, bool, type(None)),
        ):
            try:
                return self._write_jsonable(name, data)
            except TypeError:
                return self._recursive_write(name, data)
        if hasattr(data, "write_image"):
            return self._write_image(name, data)
        if isinstance(data, np.ndarray):
            return self._write_numpy(name, data, **kwargs)
        if hasattr(data, "__getstate__") and hasattr(data, "__setstate__"):
            DataZip.dump(data, temp := BytesIO())
            self.writestr(f"{name}.zip", temp.getvalue())
            _ = self._contents[name]
            return True
        if (obj_info := self._objinfo(data)) in self.recipes:
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
                return json.loads(
                    super().read(f"{arg}.json"), object_hook=_type_hinted_hook
                )
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
        self._obj_meta.update({name: self._objinfo(data)})
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
        self._obj_meta.update({name: self._objinfo(data)})
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
        if hasattr(obj, "_asdict"):
            to_write = obj._asdict()
        _ = json_dumps(to_write)
        self._attributes.update({name: to_write})
        self._obj_meta.update({name: self._objinfo(obj)})
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
        _ = json.dumps(to_write, ensure_ascii=False)
        self._attributes.update({name: to_write})
        self._obj_meta.update({name: recipe["constructor"]})
        _ = self._contents[name]
        return True

    def _write_as_str(self, name: str, obj: Any, **kwargs) -> bool:
        """Write an object as whatever str is in the parentheses of its repr."""
        obj_info = self._objinfo(obj)
        self._attributes.update(
            {name: repr(obj).removeprefix(obj_info[1] + "(").removesuffix(")")}
        )
        self._obj_meta.update({name: obj_info})
        _ = self._contents[name]
        return True

    def _write_df(self, name: str, df: pd.DataFrame | pd.Series, **kwargs) -> bool:
        """Write a df in the ZIP as parquet."""
        self._obj_meta.update({name: self._objinfo(df)})
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
        self._obj_meta.update({name: self._objinfo(data)})
        _ = self._contents[name]
        return True

    @staticmethod
    def _str_cols(df: pd.DataFrame, *args) -> pd.DataFrame:
        return df.set_axis(list(map(str, range(df.shape[1]))), axis="columns")

    @staticmethod
    def _objinfo(obj: Any, constructor=None) -> tuple[str, ...]:
        return obj.__class__.__module__, obj.__class__.__qualname__, constructor


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
