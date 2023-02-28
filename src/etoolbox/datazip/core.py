"""Code for :class:`.DataZip`."""
from __future__ import annotations

import json
import logging
import warnings
from collections import Counter, OrderedDict, defaultdict, deque
from collections.abc import Generator
from contextlib import suppress
from datetime import datetime
from functools import partial
from io import BytesIO
from pathlib import Path, PosixPath, WindowsPath
from types import NoneType
from typing import Any
from zipfile import ZipFile
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from etoolbox import __version__
from etoolbox.datazip._types import JSONABLE, DZable, _create_engine, _Engine, _Figure
from etoolbox.datazip._utils import (
    _get_klass,
    _get_username,
    _get_version,
    _objinfo,
    _quote_strip,
    default_getstate,
    default_setstate,
)

try:
    from sqlalchemy import create_engine
    from sqlalchemy.engine import Engine
except (ModuleNotFoundError, ImportError):
    Engine = _Engine
    create_engine = _create_engine


try:
    from plotly.graph_objects import Figure
except (ModuleNotFoundError, ImportError):
    Figure = _Figure


LOGGER = logging.getLogger(__name__)


class DataZip(ZipFile):
    """A :class:`ZipFile` with methods for easier use with Python objects."""

    suffixes = {
        "pdDataFrame": ".parquet",
        "pdSeries": ".parquet",
        "ndarray": ".npy",
        # LEGACY type encoding
        ("pandas.core.frame", "DataFrame", None): ".parquet",
        ("pandas.core.series", "Series", None): ".parquet",
        ("numpy", "ndarray", None): ".npy",
    }

    def __init__(self, file: str | Path | BytesIO, mode="r", *args, **kwargs):
        """Create a DataZip.

        Args:
            file: Either the path to the file, or a file-like object.
                If it is a path, the file will be opened and closed by DataZip.
            mode: The mode can be either read 'r', or write 'w'.
            recipes: Deprecated.
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
        convenient to keep the object open. Even more unusual types that can't normally be stored in json should work.

        >>> z1 = DataZip(buffer, "r")
        >>> z1["foo"]
        {'a': (1, (2, {3})), 'b': frozenset({1.5, 3}), 'c': (0.9+0.2j)}

        Checking to see if an item is in a :class:`.DataZip` uses standard Python
        syntax.

        >>> "df" in z1
        True

        You can also check by filename. And check the number of items.

        >>> "df.parquet" in z1
        True

        >>> len(z1)
        2

        When not used with a context manager, :class:`.DataZip` should close itself
        automatically but it's not a bad idea to make sure.

        >>> z1.close()

        A :class:`.DataZip` is a write-once, read-many affair because of the way
        ``zip`` files work. Appending to a :class:`.DataZip` can be done with the
        :meth:`.DataZip.replace` method.

        >>> buffer1 = BytesIO()
        >>> with DataZip.replace(buffer1, buffer, foo=5, bar=6) as z:
        ...     z["new"] = "foo"
        ...     z["foo"]
        ...
        5
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
        self._attributes, self._metadata = {"__state__": {}}, {"__rev__": 2}
        self._ids, self._red = {}, {}
        if mode == "r":
            self._attributes = self._json_get(
                "__attributes__", "attributes", "other_attrs"
            )
            self._metadata = self._json_get("__metadata__", "metadata")
            if self._metadata.get("__rev__", 1) != 2:
                LOGGER.warning(
                    "%s was created with an older version of DataZip, all data might "
                    "not be accessible, consider using v0.1.0.",
                    file,
                )
                self._attributes = self._attributes | self._load_legacy_helper()

        self._delete_on_close = None

    @staticmethod
    def dump(obj: Any, file: Path | str | BytesIO, **kwargs) -> None:
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

        >>> from etoolbox.datazip._test_classes import _TestKlass
        >>> obj = _TestKlass(a=5, b={"c": [2, 3.5]})
        >>> obj
        _TestKlass(a=5, b={'c': [2, 3.5]})

        Save the object as a :class:`.DataZip`.

        >>> buffer = BytesIO()
        >>> DataZip.dump(obj, buffer)
        >>> del obj

        Get it back.

        >>> obj = DataZip.load(buffer)
        >>> obj
        _TestKlass(a=5, b={'c': [2, 3.5]})
        """
        with DataZip(file, "w", **kwargs) as self:
            self["state"] = obj

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
            obj = DataZip._decode_obj(self, self._attributes["state"], klass)
        return obj

    @classmethod
    def replace(cls, file_or_new_buffer, old_buffer=None, save_old=False, **kwargs):
        """Replace an old :class:`DataZip` with an editable new one.

        Note: Data and keys that are copied over by this function cannot be reliably
        mutated.``kwargs`` must be used to replace the data associated with keys that
        exist in the old :class:`DataZip`.

        Args:
            file_or_new_buffer: Either the path to the file to be replaced
                or the new buffer.
            old_buffer: only required if ``file_or_new_buffer`` is a buffer.
            save_old: if True, the old :class:`DataZip` will be
                saved with "_old" appended, if False it will be
                deleted when the new :class:`DataZip` is closed.
            kwargs: data that should be written into the new :class:`DataZip`,
                for any keys that were in the old :class:`DataZip`, the new
                value provided here will be used instead.

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
            if z._metadata.get("__rev__") != 2:
                _to_delete = None
                LOGGER.warning(
                    "%s uses an old version of DataZip so not all data may be properly "
                    "copied over. For this reason, %s will not be deleted.",
                    file_or_new_buffer,
                    old_buffer,
                )
            for k, v in z.items():
                if k in kwargs:
                    self[k] = kwargs.pop(k)
                else:
                    self[k] = v
            for k, v in kwargs.items():
                self[k] = v

        self._delete_on_close = _to_delete

        return self

    def close(self) -> None:
        """Close the file, and for mode 'w' write attributes and metadata."""
        if self.fp is None:
            return

        if self.mode == "w":
            self.writestr(
                "__attributes__.json",
                json.dumps(self._attributes, ensure_ascii=False, indent=4),
            )
            self.writestr(
                "__metadata__.json",
                json.dumps(self._metadata, ensure_ascii=False, indent=4),
            )
        self._red = {}
        super().close()
        if isinstance(self._delete_on_close, Path):
            self._delete_on_close.unlink()

    def __contains__(self, item) -> bool:
        """Provide ``in`` check."""
        return item.partition(".")[0] in self._attributes

    def __len__(self) -> int:
        """Provide for use of ``len`` builtin."""
        return len([k for k in self._attributes if k != "__state__"])

    def __getitem__(self, key: int | str) -> DZable:
        """Retrieve an item from a :class:`.DataZip`."""
        return self._decode(self._attributes[key])

    def __setitem__(self, key: int | str, value: DZable) -> None:
        """Write an item to a :class:`.DataZip`."""
        if key in ("__metadata__", "__attributes__", "__state__"):
            raise KeyError(f"{key=} is reserved, please use a different name")
        if key in self._attributes:
            raise KeyError(f"{key=} already in {self.filename}")
        if (for_attributes := self._encode(key, value)) != "__IGNORE__":
            self._attributes.update({key: for_attributes})

    def get(self, key: int | str, default=None):
        """Retrieve an item if it is there otherwise return default."""
        return self[key] if key in self else default

    def items(self):
        """Lazily read name/key valye pairs from a :class:`.DataZip`.."""
        for k in self._attributes.keys():
            if k == "__state__":
                continue
            yield k, self[k]

    def keys(self):
        """Set of names in :class:`.DataZip` as if it was a dict."""
        return {k: None for k in self._attributes if k != "__state__"}.keys()

    def _decode(self, obj: Any) -> Any:
        """Entry point for decoding anything stored :class:`DataZip`."""
        if decoder := self.DECODERS.get(type(obj), None):
            return decoder(self, obj)
        raise TypeError(f"no decoder for {type(obj)} {obj}")

    def _decode_cache_helper(self, obj: dict, func, **kwargs) -> Any:
        if obj["__loc__"] in self._red:
            return self._red[obj["__loc__"]]
        out = func(self, obj, **kwargs)
        self._red[obj["__loc__"]] = out
        return out

    def _decode_dict(self, obj: dict) -> Any:
        if "__type__" in obj:
            return self.DECODERS.get(obj["__type__"], DataZip._decode_obj)(self, obj)
        return {k: self._decode(v) for k, v in obj.items()}

    @staticmethod
    def _decode_namedtuple(_, obj):
        try:
            return _get_klass(obj["objinfo"])(**obj["items"])
        except Exception as exc:
            LOGGER.error("Namedtuple will be returned as a normal tuple, %r", exc)
            return tuple(obj["items"].values())

    def _decode_pd_df(self, obj) -> pd.DataFrame:
        out = pd.read_parquet(BytesIO(super().read(obj["__loc__"])))
        if "no_pqt_cols" not in obj:
            return out
        cols, names = obj["no_pqt_cols"]
        if isinstance(names, (tuple, list)) and len(names) > 1:
            return out.set_axis(pd.MultiIndex.from_tuples(cols, names=names), axis=1)
        return out.set_axis(pd.Index(cols, name=names[0]), axis=1)

    def _decode_pd_series(self, obj) -> pd.Series:
        out = pd.read_parquet(BytesIO(super().read(obj["__loc__"]))).squeeze()
        cols, names = obj.get("no_pqt_cols", (None, None))
        out.name = tuple(cols) if isinstance(cols, list) else cols
        return out

    def _decode_obj(self, obj, klass=None) -> Any:
        if obj["__loc__"] in self._red:
            return self._red[obj["__loc__"]]
        if klass is None:
            klass = _get_klass(obj["__type__"].split("|"))
        out_obj = klass.__new__(klass)
        state = self._decode(self._attributes["__state__"][str(obj["__loc__"])])
        if hasattr(out_obj, "__setstate__"):
            out_obj.__setstate__(state)
        else:
            default_setstate(out_obj, state)
        self._red[str(obj["__loc__"])] = out_obj
        return out_obj

    DECODERS = {
        str: lambda _, item: item,
        int: lambda _, item: item,
        bool: lambda _, item: item,
        float: lambda _, item: item,
        NoneType: lambda _, item: item,
        dict: _decode_dict,
        list: lambda self, obj: [self._decode(v) for v in obj],
        "tuple": lambda self, obj: tuple(self._decode(v) for v in obj["items"]),
        "set": lambda _, obj: set(obj["items"]),
        "frozenset": lambda _, obj: frozenset(obj["items"]),
        "complex": lambda _, obj: complex(*obj["items"]),
        "type": lambda _, obj: _get_klass(obj["items"]),
        "defaultdict": lambda self, obj: defaultdict(
            self._decode(obj["default_factory"]),
            self._decode_dict(obj["items"]),
        ),
        "Counter": lambda self, obj: Counter(self._decode_dict(obj["items"])),
        "dict_aslist": lambda self, obj: dict(
            self._decode(item) for item in obj["items"]
        ),
        "deque": lambda self, obj: deque([self._decode(v) for v in obj["items"]]),
        "OrderedDict": lambda self, obj: OrderedDict(self._decode_dict(obj["items"])),
        "datetime": lambda _, obj: datetime.fromisoformat(_quote_strip(obj["items"])),
        "pdTimestamp": lambda _, obj: pd.Timestamp(_quote_strip(obj["items"])),
        "Path": lambda _, obj: Path(_quote_strip(obj["items"])),
        "namedtuple": _decode_namedtuple,
        "pdDataFrame": partial(_decode_cache_helper, func=_decode_pd_df),
        "pdSeries": partial(_decode_cache_helper, func=_decode_pd_series),
        "ndarray": partial(
            _decode_cache_helper,
            func=lambda self, obj: np.load(BytesIO(self.read(obj["__loc__"]))),
        ),
        "saEngine": lambda _, obj: create_engine(obj["items"]["url"]),
        # LEGACY type encoding
        ("builtins", "tuple", None): lambda self, obj: tuple(
            self._decode(v) for v in obj["items"]
        ),
        ("builtins", "set", None): lambda _, obj: set(obj["items"]),
        ("builtins", "frozenset", None): lambda _, obj: frozenset(obj["items"]),
        ("builtins", "complex", None): lambda _, obj: complex(*obj["items"]),
        ("pandas.core.frame", "DataFrame", None): partial(
            _decode_cache_helper, func=_decode_pd_df
        ),
        ("pandas.core.series", "Series", None): partial(
            _decode_cache_helper, func=_decode_pd_series
        ),
        ("numpy", "ndarray", None): partial(
            _decode_cache_helper,
            func=lambda self, obj: np.load(BytesIO(self.read(obj["__loc__"]))),
        ),
    }

    def _encode(self, name, item) -> JSONABLE:
        """Entry point for encoding anything to store in :class:`DataZip`."""
        if encoder := self.ENCODERS.get(type(item), None):
            return encoder(self, name, item)
        if isinstance(item, tuple):
            if hasattr(item, "_asdict"):
                return {
                    "__type__": "namedtuple",
                    "items": {k: self._encode(k, v) for k, v in item._asdict().items()},
                    "objinfo": _objinfo(item),
                }
            return {
                "__type__": "tuple",
                "items": [self._encode(_, e) for _, e in enumerate(item)],
            }

        if id(item) in self._ids:
            return {
                "__type__": _objinfo(item),
                "__loc__": self._ids[id(item)],
            }

        return self._encode_obj(name, item)

    def _encode_loc_helper(self, name: str, data: Any, to_write: Any) -> str:
        if name in self.namelist():
            name = f"{id(data)}_{name}"
        self.writestr(name, to_write)
        self._ids[id(data)] = name
        return name

    def _encode_dict(self, _, data: dict) -> dict:
        if tuple in map(type, data.keys()):
            return {
                "__type__": "dict_aslist",
                "items": [self._encode(_, item) for _, item in enumerate(data.items())],
            }
        return {k: self._encode(k, v) for k, v in data.items()}

    def _encode_pd_df(self, name: str, df: pd.DataFrame, **kwargs) -> dict:
        """Write a df in the ZIP as parquet."""
        if loc := self._ids.get(id(df), None):
            return {"__type__": "pdDataFrame", "__loc__": loc}
        try:
            return {
                "__type__": "pdDataFrame",
                "__loc__": self._encode_loc_helper(
                    f"{name}.parquet", df, df.to_parquet()
                ),
            }
        except ValueError:
            return {
                "__type__": "pdDataFrame",
                "__loc__": self._encode_loc_helper(
                    f"{name}.parquet", df, self._str_cols(df).to_parquet()
                ),
                "no_pqt_cols": [list(df.columns), list(df.columns.names)],
            }

    def _encode_pd_series(self, name: str, df: pd.Series, **kwargs) -> dict:
        if loc := self._ids.get(id(df), None):
            return {"__type__": "pdSeries", "__loc__": loc}
        return {
            "__type__": "pdSeries",
            "__loc__": self._encode_loc_helper(
                f"{name}.parquet", df, df.to_frame(name="IGNORETHISNAME").to_parquet()
            ),
            "no_pqt_cols": [
                list(df.name) if isinstance(df.name, tuple) else df.name,
                None,
            ],
        }

    def _encode_ndarray(self, name: str, data: np.ndarray, **kwargs) -> dict:
        if loc := self._ids.get(id(data), None):
            return {"__type__": "ndarray", "__loc__": loc}
        np.save(temp := BytesIO(), data, allow_pickle=False)
        return {
            "__type__": "ndarray",
            "__loc__": self._encode_loc_helper(f"{name}.npy", data, temp.getvalue()),
        }

    def _encode_obj(self, name: str, item: Any) -> dict:
        if hasattr(item, "__getstate__"):
            state = item.__getstate__()
        elif hasattr(item, "__dict__") or hasattr(item, "__slots__"):
            state = default_getstate(item)
        else:
            raise TypeError(f"no encoder for {type(item)}")

        if name in self._attributes["__state__"]:
            name = f"{id(item)}_{name}"

        self._ids[id(item)] = name
        self._attributes["__state__"][name] = self._encode("state", state)
        return {
            "__type__": _objinfo(item),
            "__loc__": name,
            "__obj_version__": _get_version(item),
            "__io_version__": __version__,
            "__created_by__": _get_username(),
            "__file_created__": str(datetime.now(tz=ZoneInfo("UTC"))),
        }

    def _write_image(self, name: str, data: Any, **kwargs) -> str:
        data.write_image(temp := BytesIO(), format="pdf")
        self.writestr(f"{name}.pdf", temp.getvalue())
        LOGGER.warning("Only pdf of Figure is stored")
        return "__IGNORE__"

    ENCODERS = {
        str: lambda _, __, item: item,
        int: lambda _, __, item: item,
        bool: lambda _, __, item: item,
        float: lambda _, __, item: item,
        NoneType: lambda _, __, item: item,
        list: lambda self, _, item: [self._encode(i, e) for i, e in enumerate(item)],
        tuple: lambda self, _, item: {
            "__type__": "tuple",
            "items": [self._encode(i, e) for i, e in enumerate(item)],
        },
        dict: _encode_dict,
        set: lambda self, _, item: {
            "__type__": "set",
            "items": [self._encode(i, e) for i, e in enumerate(item)],
        },
        frozenset: lambda self, _, item: {
            "__type__": "frozenset",
            "items": [self._encode(i, e) for i, e in enumerate(item)],
        },
        complex: lambda _, __, item: {
            "__type__": "complex",
            "items": [item.real, item.imag],
        },
        type: lambda self, __, item: {
            "__type__": "type",
            "items": [item.__module__, item.__qualname__],
        },
        defaultdict: lambda self, __, item: {
            "__type__": "defaultdict",
            "items": {key: self._encode(key, value) for key, value in item.items()},
            "default_factory": self._encode(__, item.default_factory),
        },
        Counter: lambda self, __, item: {
            "__type__": "Counter",
            "items": self._encode_dict(__, item),
        },
        deque: lambda self, __, item: {
            "__type__": "deque",
            "items": [self._encode(_, e) for _, e in enumerate(item)],
        },
        OrderedDict: lambda self, __, item: {
            "__type__": "OrderedDict",
            "items": self._encode_dict(__, item),
        },
        datetime: lambda _, __, item: {"__type__": "datetime", "items": str(item)},
        pd.Timestamp: lambda _, __, item: {
            "__type__": "pdTimestamp",
            "items": str(item),
        },
        Path: lambda _, __, item: {"__type__": "Path", "items": str(item)},
        PosixPath: lambda _, __, item: {"__type__": "Path", "items": str(item)},
        WindowsPath: lambda _, __, item: {"__type__": "Path", "items": str(item)},
        np.ndarray: _encode_ndarray,
        np.int64: lambda _, __, item: int(item),
        pd.DataFrame: _encode_pd_df,
        pd.Series: _encode_pd_series,
        Engine: lambda _, __, item: {
            "__type__": "saEngine",
            "items": {"url": str(item.url)},
        },
        Figure: _write_image,
    }

    def _load_legacy_helper(self) -> dict:
        obj_meta = self._metadata.get("obj_meta", self._json_get("obj_meta"))

        def _make_attr_entry(k):
            _attr = {}
            if (objinfo := tuple(obj_meta.get(k, ""))) in self.DECODERS:
                _attr.update({"__type__": objinfo})
                if (file := (k + self.suffixes.get(objinfo, ""))) in self.namelist():
                    _attr.update({"__loc__": file})
                elif k in self._attributes:
                    _attr.update({"items": self._attributes[k]})
                if k in _no_pqt_cols:
                    _attr.update({"no_pqt_cols": _no_pqt_cols[k]})
            else:
                _attr = self._attributes.get(k, {})
            return _attr

        _no_pqt_cols = self._metadata.get(
            "no_pqt_cols", self._metadata.get("bad_cols", self._json_get("bad_cols"))
        )
        contents = self._metadata.get("contents", self._json_get("contents"))
        attrs = {}
        for k, sub_k in contents.items():
            if sub_k:
                LOGGER.warning(
                    "Unable to load nested structures, %s and its children will "
                    "not be accessible",
                    k,
                )

            attr = _make_attr_entry(k)
            if attr:
                attrs.update({k: attr})
        return attrs

    @staticmethod
    def _str_cols(df: pd.DataFrame, *args) -> pd.DataFrame:
        return df.set_axis(list(map(str, range(df.shape[1]))), axis="columns")

    def _json_get(self, *args):
        for arg in args:
            with suppress(Exception):
                return json.loads(super().read(f"{arg}.json"))
        return {}

    def read_dfs(self) -> Generator[tuple[str, pd.DataFrame | pd.Series]]:
        """Read all dfs lazily."""
        warnings.warn(
            "``read_dfs`` will be removed in a future version, use ``items``.",
            DeprecationWarning,
        )
        for name, *suffix in map(lambda x: x.split("."), self.namelist()):
            if "parquet" in suffix:
                yield name, self[name]

    def writed(
        self,
        name: str,
        data: Any,
    ):
        """Write dict, df, str, or some other objects to name."""
        warnings.warn(
            "``writed`` will be removed. Use `self[key] = data`", DeprecationWarning
        )
        self[name] = data

    def __repr__(self):
        return self.__class__.__qualname__ + f"(file={self.filename}, mode={self.mode})"
