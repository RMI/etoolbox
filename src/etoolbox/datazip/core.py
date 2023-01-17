"""Code for :class:`.DataZip`."""
from __future__ import annotations

import json
import logging
from collections import defaultdict
from collections.abc import Generator
from contextlib import suppress
from importlib import import_module
from io import BytesIO
from pathlib import Path
from typing import Any
from zipfile import ZipFile, ZipInfo

import numpy as np
import pandas as pd

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


def obj_from_recipe(
    thing: dict | tuple | list | str | int | float | BytesIO,
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
    if klass in ("list", "dict"):
        return thing
    if klass == "str":
        return (
            thing.removeprefix("'")
            .removeprefix('"')
            .removesuffix("'")
            .removesuffix('"')
        )

    if constructor is None and klass is not None:
        init_or_const = getattr(import_module(module), klass)
    elif constructor is not None and klass is None:
        init_or_const = getattr(import_module(module), constructor)
    elif all((klass, constructor)):
        init_or_const = getattr(getattr(import_module(module), klass), constructor)
    else:
        raise AssertionError("Must specify at least one of `klass` and `constructor`")

    if module != "builtins":
        if isinstance(thing, dict):
            return init_or_const(**thing)
        if isinstance(thing, (tuple, list)):
            return init_or_const(*thing)
    return init_or_const(thing)


class DataZip(ZipFile):
    """SubClass of :class:`ZipFile` with methods for easier use with :mod:`utils`.

    z = DataZip(file, mode="r", compression=ZIP_STORED, allowZip64=True,
                compresslevel=None)

    """

    def __init__(
        self, file: str | Path | BytesIO, mode="r", recipes=None, *args, **kwargs
    ):
        """Open the ZIP file.

        Args:
            file: Either the path to the file, or a file-like object.
                If it is a path, the file will be opened and closed by ZipFile.
            mode: The mode can be either read 'r', write 'w', exclusive create 'x',
                or append 'a'.
            recipes: add more customization on how attributes will be stored
                organized like :py:const:`etoolbox.datazip.core.RECIPES`
            compression: ZIP_STORED (no compression), ZIP_DEFLATED (requires zlib),
                ZIP_BZIP2 (requires bz2) or ZIP_LZMA (requires lzma).
            allowZip64: if True ZipFile will create files with ZIP64 extensions when
                needed, otherwise it will raise an exception when this would
                be necessary.
            compresslevel: None (default for the given compression type) or an integer
                specifying the level to pass to the compressor.
                When using ZIP_STORED or ZIP_LZMA this keyword has no effect.
                When using ZIP_DEFLATED integers 0 through 9 are accepted.
                When using ZIP_BZIP2 integers 1 through 9 are accepted.
        """
        if mode in ("a", "x"):
            raise ValueError("DataZip does not support modes 'a' or 'x'")

        if isinstance(file, str):
            file = Path(file)

        if isinstance(file, Path):
            file = file.with_suffix(".zip")
            if file.exists() and mode == "w":
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

    def read(self, name: str | ZipInfo, pwd: bytes | None = ..., super_=False) -> Any:
        """Return obj or bytes for name."""
        stem, _, suffix = name.partition(".")
        if self._contents.get(stem, False):
            return self._recursive_read(stem)
        if suffix == "parquet" or f"{stem}.parquet" in self.namelist():
            return self._read_df(stem)
        if (suffix == "zip" or f"{stem}.zip" in self.namelist()) and not super_:
            return obj_from_recipe(
                BytesIO(super().read(stem + ".zip")), *self._obj_meta[stem]
            )
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

    def readm(self, key=None):
        """Read metadata."""
        if key is None:
            return self._other_meta
        return self._other_meta[key]

    def writed(
        self,
        name: str,
        data: Any,
        **kwargs,
    ) -> bool:
        """Write dict, df, str, or some other objects to name."""
        name, _, suffix = name.partition(".")
        if name in ("__metadata__", "__attributes__"):
            raise ValueError(f"`{name}` is reserved, please use a different name")
        if data is None:
            LOGGER.info("Unable to write data %s because it is None.", name)
            return False
        if name in self._contents:
            raise FileExistsError(f"{name} already in {self.filename}")
        if isinstance(data, (pd.DataFrame, pd.Series)):
            return self._write_df(name, data, **kwargs)
        if isinstance(data, (dict, list, tuple)):
            try:
                return self._write_jsonable(name, data)
            except TypeError:
                return self._recursive_write(name, data)
        if hasattr(data, "write_image"):
            return self._write_image(name, data)
        if hasattr(data, "to_file") and hasattr(data, "from_file"):
            return self._write_obj(name, data, **kwargs)
        if isinstance(data, np.ndarray):
            return self._write_numpy(name, data, **kwargs)
        if (obj_info := self._objinfo(data)) in self.recipes:
            return self._write_using_recipe(name, data, obj_info)
        return self._write_as_str(name, data, **kwargs)

    def writem(self, key, data):
        """Write metadata."""
        if key is None and not isinstance(data, dict):
            raise TypeError("if key is None, data must be a dict")
        _ = json.dumps(data, ensure_ascii=False, indent=4)
        if key is None:
            self._other_meta = self._other_meta | data
        else:
            self._other_meta.update({key: data})

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
                json.dumps(
                    {
                        "contents": self._contents,
                        "no_pqt_cols": self._no_pqt_cols,
                        "obj_meta": self._obj_meta,
                        "recipes": list(self._recipes.items()),
                        "other_meta": self._other_meta,
                    },
                    ensure_ascii=False,
                    indent=4,
                ),
            )
        super().close()

    def __contains__(self, item):
        """Provide ``in`` check."""
        item, _, _ = item.partition(".")
        return item in self._contents

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
                return json.loads(super().read(f"{arg}.json"))
        return {}

    @property
    def recipes(self) -> dict[tuple, dict]:
        """Combination of internal and constant recipes."""
        return RECIPES | self._recipes

    def _stemlist(self) -> list[str]:
        return list(map(lambda x: x.partition(".")[0], self.namelist()))

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
        _ = json.dumps(to_write, ensure_ascii=False, indent=4)
        self._attributes.update({name: to_write})
        self._obj_meta.update({name: self._objinfo(obj)})
        _ = self._contents[name]
        return True

    def _write_using_recipe(self, name: str, data: Any, obj_info) -> bool:
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
        _ = json.dumps(to_write, ensure_ascii=False, indent=4)
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

    def _write_obj(self, name: str, obj: Any, **kwargs) -> bool:
        obj.to_file(temp := BytesIO(), **kwargs)
        self.writestr(f"{name}.zip", temp.getvalue())
        self._obj_meta.update({name: self._objinfo(obj, "from_file")})
        _ = self._contents[name]
        return True

    def _write_df(self, name: str, df: pd.DataFrame | pd.Series, **kwargs) -> bool:
        """Write a df in the ZIP as parquet."""
        if df.empty:
            LOGGER.info("Unable to write df %s because it is empty.", name)
            return False
        self._obj_meta.update({name: self._objinfo(df)})
        if isinstance(df, pd.Series):
            df = df.to_frame(name=name)
        try:
            self.writestr(f"{name}.parquet", df.to_parquet())
        except ValueError:
            self._no_pqt_cols.update({name: (list(df.columns), list(df.columns.names))})
            self.writestr(f"{name}.parquet", self._str_cols(df).to_parquet())
        # self._obj_meta.update({name: self._objinfo(df)})
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
