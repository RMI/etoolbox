"""Code for :class:`.DataZip`."""
from __future__ import annotations

import json
import logging
from collections import defaultdict
from collections.abc import Generator
from importlib import import_module
from io import BytesIO
from pathlib import Path
from typing import Any, NamedTuple
from zipfile import ZipFile, ZipInfo

import pandas as pd

LOGGER = logging.getLogger(__name__)

RECIPES = {
    ("datetime", "datetime", None): {
        "method": "as_str",
        "attributes": None,
        "keep": True,
        "constructor": ("datetime", "datetime", None),
    },
    ("pandas._libs.tslibs.timestamps", "Timestamp", None): {
        "method": "as_str",
        "attributes": None,
        "keep": True,
        "constructor": ("pandas._libs.tslibs.timestamps", "Timestamp", None),
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
    """SubClass of :class:`ZipFile` with methods for easier use with :mod:`pandas`.

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
        self.bad_cols, self.obj_meta = {}, {}
        self.other_attrs, self.contents = {}, defaultdict(list)
        self._recipes = {} if recipes is None else recipes
        if mode == "r":
            for attr in (
                "bad_cols",
                "obj_meta",
                "other_attrs",
                "contents",
            ):
                try:
                    setattr(self, attr, json.loads(super().read(attr + ".json")))
                except Exception as exc:
                    LOGGER.warning("%s missing from %s, %r", attr, file, exc)
            try:
                self._recipes = {
                    tuple(k): v for k, v in json.loads(super().read("_recipes.json"))
                }
            except Exception as exc:
                LOGGER.warning("unable to load recipes for %s, %r", file, exc)

    @property
    def recipes(self):
        """Combination of internal and constant recipes."""
        return RECIPES | self._recipes

    def _stemlist(self):
        return list(map(lambda x: x.partition(".")[0], self.namelist()))

    def read(
        self, name: str | ZipInfo, pwd: bytes | None = ..., super_=False
    ) -> bytes | pd.DataFrame | pd.Series | dict | Any:
        """Return obj or bytes for name."""
        stem, _, suffix = name.partition(".")
        if self.contents.get(stem, False):
            if "dict" in self.obj_meta[stem][1]:
                out_dict = {
                    k.rpartition("<||>")[2]: self.read(k) for k in self.contents[stem]
                }
                if self.obj_meta[stem][1] == "defaultdict":
                    return defaultdict(lambda: None, out_dict)
                return out_dict
            else:
                return [self.read(k) for k in self.contents[stem]]
        if suffix == "parquet" or f"{stem}.parquet" in self.namelist():
            return self._read_df(stem)
        if suffix == "json" or f"{stem}.json" in self.namelist():
            return self._read_jsonable(stem)
        if (suffix == "zip" or f"{stem}.zip" in self.namelist()) and not super_:
            return self._read_obj(stem)
        if stem in self.other_attrs:
            return obj_from_recipe(
                self.other_attrs[stem], *self.obj_meta.get(stem, (None, None, None))
            )
        return super().read(name)

    def read_dfs(self) -> Generator[tuple[str, pd.DataFrame | pd.Series]]:
        """Read all dfs lazily."""
        for name, *suffix in map(lambda x: x.split("."), self.namelist()):
            if "parquet" in suffix:
                yield name, self.read(name)

    def _read_obj(self, name) -> pd.DataFrame | pd.Series:
        temp = BytesIO(super().read(name + ".zip"))
        return obj_from_recipe(temp, *self.obj_meta[name])

    def _read_jsonable(self, name) -> pd.DataFrame | pd.Series:
        thing = json.loads(super().read(name + ".json"))
        return obj_from_recipe(thing, *self.obj_meta.get(name, (None, None, None)))

    def _read_df(self, name) -> pd.DataFrame | pd.Series:
        out = pd.read_parquet(BytesIO(super().read(name + ".parquet")))

        if name in self.bad_cols:
            cols, names = self.bad_cols[name]
            if isinstance(names, (tuple, list)) and len(names) > 1:
                cols = pd.MultiIndex.from_tuples(cols, names=names)
            else:
                cols = pd.Index(cols, name=names[0])
            out.columns = cols
        return out.squeeze()

    def writed(
        self,
        name: str,
        data: str | dict | pd.DataFrame | pd.Series | NamedTuple | Any,
        parent_name: str | None = None,
        **kwargs,
    ) -> None:
        """Write dict, df, str, to name."""
        if data is None:
            LOGGER.info("Unable to write data %s because it is None.", name)
            return None
        name = name.removesuffix(".json").removesuffix(".parquet").removesuffix(".zip")
        if name not in self.contents:
            if isinstance(data, (pd.DataFrame, pd.Series)):
                self._write_df(name, data, **kwargs)
            elif isinstance(data, (dict, list, tuple)):
                try:
                    self._write_jsonable(name, data)
                except TypeError:
                    self._recursive_write(name, data)
            elif hasattr(data, "to_file") and hasattr(data, "from_file"):
                self._write_obj(name, data, **kwargs)
            elif (obj_info := self._objinfo(data)) in self.recipes:
                self._write_using_recipe(name, data, obj_info)
            else:
                self._write_as_str(name, data, **kwargs)

        else:
            raise FileExistsError(f"{name} already in {self.filename}")

    def _recursive_write(self, name, data):
        if isinstance(data, dict):
            item_iter = data.items()
        elif isinstance(data, (tuple, list)):
            item_iter = enumerate(data)
        else:
            raise TypeError(f"{name} is a {data.__class__} which is not supported")
        self.obj_meta.update({name: self._objinfo(data)})
        for k, v in item_iter:
            self.writed(f"{name}<||>{k}", v, parent_name=name)
            self.contents[name].append(f"{name}<||>{k}")

    def _write_jsonable(
        self,
        name,
        obj: dict[int | str, list | tuple | dict | str | float | int] | list,
        **kwargs,
    ) -> None:
        """Write a dict in the ZIP as json."""
        to_write = obj
        if hasattr(obj, "_asdict"):
            to_write = obj._asdict()
        _ = json.dumps(to_write, ensure_ascii=False, indent=4)
        self.other_attrs.update({name: to_write})
        self.obj_meta.update({name: self._objinfo(obj)})
        _ = self.contents[name]

    def _write_using_recipe(self, name, data, obj_info):
        recipe = self.recipes[obj_info]
        if not recipe["keep"]:
            return None
        if recipe["method"] == "by_attribute":
            to_write = {
                attr: (func(getattr(data, attr)) if func else getattr(data, attr))
                for attr, func in recipe["attributes"]
            }
        elif recipe["method"] == "as_str":
            to_write = str(data)
        _ = json.dumps(to_write, ensure_ascii=False, indent=4)
        self.other_attrs.update({name: to_write})
        self.obj_meta.update({name: recipe["constructor"]})
        _ = self.contents[name]

    def _write_as_str(self, name, obj, **kwargs):
        """Write an object as whatever str is in the parentheses of its repr."""
        obj_info = self._objinfo(obj)
        self.other_attrs.update(
            {name: repr(obj).removeprefix(obj_info[1] + "(").removesuffix(")")}
        )
        self.obj_meta.update({name: obj_info})
        _ = self.contents[name]

    def _write_obj(self, name, obj, **kwargs):
        obj.to_file(temp := BytesIO(), **kwargs)
        self.writestr(f"{name}.zip", temp.getvalue())
        self.obj_meta.update({name: self._objinfo(obj, "from_file")})
        _ = self.contents[name]

    def _write_df(self, name: str, df: pd.DataFrame | pd.Series, **kwargs) -> None:
        """Write a df in the ZIP as parquet."""
        if df.empty:
            LOGGER.info("Unable to write df %s because it is empty.", name)
            return None
        if isinstance(df, pd.Series):
            df = df.to_frame(name=name)
        try:
            self.writestr(f"{name}.parquet", df.to_parquet())
        except ValueError:
            self.bad_cols.update({name: (list(df.columns), list(df.columns.names))})
            self.writestr(f"{name}.parquet", self._str_cols(df).to_parquet())
        self.obj_meta.update({name: self._objinfo(df)})
        _ = self.contents[name]

    @staticmethod
    def _str_cols(df, *args):
        return df.set_axis(list(map(str, range(df.shape[1]))), axis="columns")

    @staticmethod
    def _objinfo(obj, constructor=None):
        return obj.__class__.__module__, obj.__class__.__qualname__, constructor

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.mode == "w":
            self.writestr(
                "other_attrs.json",
                json.dumps(self.other_attrs, ensure_ascii=False, indent=4),
            )
            self.writestr(
                "contents.json", json.dumps(self.contents, ensure_ascii=False, indent=4)
            )
            self.writestr(
                "bad_cols.json", json.dumps(self.bad_cols, ensure_ascii=False, indent=4)
            )
            self.writestr(
                "obj_meta.json", json.dumps(self.obj_meta, ensure_ascii=False, indent=4)
            )
            self.writestr(
                "_recipes.json",
                json.dumps(list(self._recipes.items()), ensure_ascii=False, indent=4),
            )
        super().__exit__(exc_type, exc_val, exc_tb)

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
