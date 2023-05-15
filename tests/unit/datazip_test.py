"""Core :class:`.DataZip` tests."""
import functools
import json
from collections import Counter, OrderedDict, defaultdict, deque
from datetime import datetime
from io import BytesIO
from pathlib import Path
from traceback import TracebackException
from typing import NamedTuple
from zipfile import ZipFile

import numpy as np
import pandas as pd
import pytest
from etoolbox.datazip import DataZip
from etoolbox.datazip._test_classes import (
    ObjMeta,
    _KlassSlots,
    _TestKlass,
    _TestKlassCore,
    _TestKlassSlotsCore,
    _TestKlassSlotsDict,
)
from etoolbox.datazip._utils import default_getstate, default_setstate
from etoolbox.utils.testing import assert_equal, idfn


class TestUtils:
    """Test DataZip utilities."""

    @pytest.mark.parametrize(
        "obj, expected",
        [
            (5, None),
            (_TestKlassSlotsCore(foo=5), (None, {"foo": 5})),
            (_TestKlassCore(foo=5), {"foo": 5}),
            (
                _TestKlassSlotsDict(foo=5).add_to_dict("bar", 6),
                ({"bar": 6}, {"foo": 5}),
            ),
        ],
        ids=idfn,
    )
    def test_default_get_state(self, obj, expected):
        """Test default version of getstate."""
        assert default_getstate(obj) == expected

    @pytest.mark.parametrize(
        "obj, state, expected",
        [
            (_TestKlassSlotsCore, (None, {"foo": 5}), _TestKlassSlotsCore(foo=5)),
            (_TestKlassCore, {"foo": 5}, _TestKlassCore(foo=5)),
            (
                _TestKlassSlotsDict,
                ({"bar": 6}, {"foo": 5}),
                _TestKlassSlotsDict(foo=5).add_to_dict("bar", 6),
            ),
        ],
        ids=idfn,
    )
    def test_default_set_state(self, obj, state, expected):
        """Test default version of setstate."""
        inst = obj.__new__(obj)
        default_setstate(inst, state)
        assert inst == expected


def test_existing_name():
    """Test existing name protection."""
    with DataZip(BytesIO(), "w") as z:
        z["a"] = "a"
        with pytest.raises(KeyError):
            z["a"] = 5


def test_get():
    """Test get method and default value."""
    with DataZip(BytesIO(), "w") as z:
        z["a"] = "a"
        assert z.get("a") == "a"
        assert z.get("q", 5) == 5


def test_keys():
    """Test key method."""
    with DataZip(BytesIO(), "w") as z:
        z["a"] = "a"
        assert z.keys() == {"a": None}.keys()


def test_no_decode():
    """Test error when no decoder, though artificial."""
    z = DataZip(BytesIO(), "w")
    z._attributes["a"] = complex(3, 4)
    with pytest.raises(TypeError):
        assert z["a"] == [1, 2]
    with pytest.raises(TypeError):
        z.close()


@pytest.mark.parametrize(
    "protected, error",
    [
        ("__metadata__", KeyError),
        ("__attributes__", KeyError),
        (3, TypeError),
    ],
    ids=idfn,
)
def test_invalid_keys(temp_dir, protected, error):
    """Test :class:`.DataZip`."""
    with DataZip(BytesIO(), "w") as z:
        with pytest.raises(error):
            z[protected] = 3


def test_wrong_mode_write():
    """Test that writing to DZ opened in ``r`` mode raises error."""
    DataZip(temp := BytesIO(), "w").close()
    with DataZip(temp, "r") as z:
        with pytest.raises(ValueError):
            z["3"] = 3


def test_namedtuple_fallback(temp_dir):
    """Test named tuple fallback."""

    class NTF(NamedTuple):
        a: int
        b: int

    file = temp_dir / "test_namedtuple_fallback.zip"
    with DataZip(str(file), "w") as z0:
        z0["a"] = NTF(a=3, b=3)
    with DataZip(file, "r") as z1:
        assert z1["a"] == (3, 3)


def test_sqlalchemy(temp_dir):
    """Test sqlalchemy engine."""
    sqlalchemy = pytest.importorskip("sqlalchemy")

    a = sqlalchemy.create_engine("sqlite:///" + str(Path.home()))

    with DataZip(temp_dir / "test_sqa.zip", "w") as z0:
        z0["a"] = a
    with DataZip(temp_dir / "test_sqa.zip", "r") as z1:
        assert isinstance(z1["a"], type(a))
        assert z1["a"].url == a.url


@pytest.mark.parametrize(
    "key, expected",
    [
        ("tuple", (2, (3.5, (1, 3)))),
        ("tuple_in_dict", {"foo": (5, 4.5), "bar": 3}),
        ("set", {1, 2, 4}),
        ("frozenset", frozenset((1, 2, 4))),
        ("string", "this"),
        ("int", 3),
        ("true", True),
        ("false", False),
        ("none", None),
        ("dict_w_none", {"foo": None, "bar": 3}),
        ("complex", {"d": 2 + 3j}),
        ("datetime", datetime.now()),
        # this shouldn't really exist outside an array, right?
        pytest.param(
            "npdatetime64", np.datetime64(datetime.now()), marks=pytest.mark.xfail
        ),
        ("namedtuple_nested", [(ObjMeta("this", "that"), 5), 4, (1, 2)]),
        ("namedtuple_as_key", {ObjMeta("this", "that"): 5}),
        ("namedtuple_as_key_in_dd", defaultdict(list, {ObjMeta("this", "that"): 5})),
        ("namedtuple", ObjMeta("this", "that")),
        ("np.array", np.array([[0.0, 4.1], [3.2, 2.1]])),
        ("_TestKlass", _TestKlass(a=5, b={"c": (2, 3.5)}, c=5.5)),
        ("path", Path.home()),
        ("type", [list, tuple, DataZip]),
        ("defaultdict", defaultdict(default_factory=list, a=(1, 2, 3))),
        ("Counter", Counter({"a": 2, (1, 2, 3): 2})),
        ("deque", deque([1, 2, 3])),
        ("OrderedDict", OrderedDict({"a": 2, "b": 2})),
        ("dict_tuple_keys", {(1, 2): 5, ("a",): 3}),
        (
            "nested_TracebackException",
            {
                "a": [TracebackException.from_exception(RuntimeWarning("whops"))],
                "b": [TracebackException.from_exception(KeyError("whops"))],
            },
        ),
    ],
    ids=idfn,
)
def test_types(temp_dir, key, expected):
    """Test preservation of tuples."""
    with DataZip(temp_dir / f"test_types_{key}.zip", "w") as z0:
        z0[key] = expected
        pass
    with DataZip(temp_dir / f"test_types_{key}.zip", "r") as z1:
        read = z1[key]
        if not isinstance(read, type(expected)):
            raise AssertionError(
                f"test_types for {key} {type(read)} != {type(expected)}"
            )
        assert_equal(read, expected)


@pytest.mark.parametrize("save_old", [True, False], ids=idfn)
def test_replace(temp_dir, save_old):
    """Test file-based replace."""
    file = temp_dir / f"test_replace_{save_old}.zip"
    with DataZip(file, "w") as z0:
        z0["a"] = None
        z0["b"] = {"foo": None, "bar": 3}
    with DataZip.replace(file, save_old=save_old, a="5", c=(1, 2)) as z1:
        assert "a" in z1
        z1["d"] = {"this": "that"}
    if save_old:
        assert (temp_dir / f"test_replace_{save_old}_old.zip").exists()
    else:
        assert not (temp_dir / f"test_replace_{save_old}_old.zip").exists()
    with DataZip(file, "r") as z2:
        assert z2["a"] == "5"
        assert z2["b"] == {"foo": None, "bar": 3}
        assert z2["c"] == (1, 2)
        assert z2["d"] == {"this": "that"}


def test_replace_buffer_error():
    """Test that supplying one buffer produces an error."""
    with pytest.raises(TypeError):
        DataZip.replace(BytesIO())


def test_deep_access(temp_dir):
    """Test direct access to nested data."""
    with DataZip(temp_dir / "test_deep_access.zip", "w") as z0:
        z0["a"] = _TestKlass(a=5, b={"c": (2, 3.5)}, c={"q": 5.5})
    with DataZip(temp_dir / "test_deep_access.zip", "r") as z1:
        assert z1["a", "c", "q"] == 5.5


def test_partial(temp_dir):
    """Test that :func:`functools.partial` is not stored."""
    file = temp_dir / "test_partial.zip"
    with DataZip(file, "w") as z0:
        z0["a"] = functools.partial(int, base=2)
    with DataZip(file, "r") as z1:
        assert "a" not in z1


class TestWPDBackend:
    """Tests that involve pandas.

    These tests get run with both ``pyarrow`` and ``pandas`` backends if
    ``pandas.__version__ >= '2.0.0'``.
    """

    def test_datazip_contains_len(self, pd_backend, temp_dir):
        """Test override of ``in``."""
        file = temp_dir / f"test_datazip_contains_len_{pd_backend}.zip"
        with DataZip(file, "w") as z:
            z["a"] = pd.Series([1, 2, 3, 4])
        with DataZip(file, "r") as z1:
            assert "a" in z1
            assert "a.parquet" in z1
            assert len(z) == 1

    def test_datazip_w(self, temp_dir):
        """Test writing to existing :class:`.DataZip`."""
        file = temp_dir / "test_datazip_w.zip"
        try:
            with DataZip(file, "w") as z0:
                z0["a"] = 15
        except Exception as exc:
            raise AssertionError("Something broke") from exc
        else:
            with pytest.raises(ValueError):
                _ = DataZip(file, "a")
            with pytest.raises(ValueError):
                _ = DataZip(file, "x")
            with pytest.raises(FileExistsError):
                _ = DataZip(file, "w")
            with DataZip(file, "w", clobber=True) as z4:
                z4["b"] = 16
            with DataZip(file, "r") as z5:
                assert z5["b"] == 16

    def test_dup_names(self, pd_backend, temp_dir):
        """Test that object with the same name are both stored."""
        expected = [
            {"series": pd.Series([1, 2, 3, 4], name="series")},
            {"series": pd.Series([1, 2, 355, 4])},
        ]
        with DataZip(temp_dir / f"test_dup_names_{pd_backend}.zip", "w") as z0:
            z0["stuff"] = expected
            assert len(set(z0.namelist())) == 2
        with DataZip(temp_dir / f"test_dup_names_{pd_backend}.zip", "r") as z1:
            assert_equal(z1["stuff"], expected)

    def test_legacy(self, pd_backend, temp_dir):
        """Test that legacy DataZip can be opened and used."""
        expected_df = pd.DataFrame(
            [[0.1, 1.2], [0.5, 1.9], [1.1, 3.2]],
            columns=pd.MultiIndex.from_tuples(
                [(56391, "onshore_wind"), (60893, "solar")],
                names=["plant_id_eia", "generator_id"],
            ),
        )
        df2 = pd.DataFrame([[0, 1], [1, 0]], columns=["0", "1"])
        array = np.array([1, 2, 3, 4])
        array2 = np.array([1, 2, 555, 4])
        meta = {
            "contents": {"profs": [], "array": []},
            "no_pqt_cols": {
                "profs": [
                    [list(x) for x in list(expected_df.columns)],
                    list(expected_df.columns.names),
                ]
            },
            "obj_meta": {
                "profs": ["pandas.core.frame", "DataFrame", None],
            },
        }
        dz_file = temp_dir / f"test_legacy_{pd_backend}.zip"
        with ZipFile(dz_file, "w") as z:
            z.writestr(
                "__attributes__.json", json.dumps({}, ensure_ascii=False, indent=4)
            )
            z.writestr(
                "__metadata__.json", json.dumps(meta, ensure_ascii=False, indent=4)
            )
            z.writestr("profs.parquet", DataZip._str_cols(expected_df).to_parquet())
            z.writestr("df2.parquet", df2.to_parquet())
            np.save(temp := BytesIO(), array, allow_pickle=False)
            z.writestr("array.npy", temp.getvalue())
            np.save(temp2 := BytesIO(), array2, allow_pickle=False)
            z.writestr("array2.npy", temp2.getvalue())
            print()

        with DataZip(dz_file, "r") as z1:
            assert_equal(z1["profs"].astype(expected_df.dtypes), expected_df)
            assert_equal(z1["df2"].astype(df2.dtypes), df2)
            assert_equal(z1["array"], array)

    @pytest.mark.parametrize(
        "key, ignore_dtypes, expected",
        [
            (
                "df",
                False,
                pd.DataFrame(
                    [[0, 1], [2, 3]],
                    columns=pd.MultiIndex.from_tuples(
                        [(0, "a"), (1, "b")], names=["l1", "l2"]
                    ),
                ),
            ),
            (
                "df_arrow",
                False,
                pd.DataFrame(
                    [[0, 1], [2, 3]],
                    columns=pd.MultiIndex.from_tuples(
                        [(0, "a"), (1, "b")], names=["l1", "l2"]
                    ),
                ).astype("int64[pyarrow]"),
            ),
            ("series", False, pd.Series([1, 2, 3, 4], name="series")),
            (
                "series_arrow",
                False,
                pd.Series([1, 2, 3, 4], name="series").astype("int64[pyarrow]"),
            ),
            ("series_tp_name", False, pd.Series([1, 2, 3, 4], name=(0, "a"))),
            ("series_no_name", False, pd.Series([1, 2, 3, 4])),
            ("tuple_w_series", False, (1, pd.Series([1, 2, 3, 4], name="series"))),
            (
                "dup_series_in_dicts",
                False,
                (
                    {"series": pd.Series([1, 2, 3, 4], name="series")},
                    {"series": pd.Series([1, 2, 355, 4])},
                ),
            ),
            (
                "dup_objs",
                False,
                [
                    {
                        "a": _TestKlass(
                            a=5, b={"c": pd.Series([1, 2, 3, 4], name="series")}, c=5.5
                        )
                    },
                    {"a": _TestKlass(a=5, b={"c": pd.Series([1, 2, 355, 4])}, c=5.5)},
                ],
            ),
            ("pdTimestamp", False, pd.Timestamp(datetime.now())),
            (
                "df",
                True,
                pd.DataFrame(
                    [[0, 1], [2, 3]],
                    columns=pd.MultiIndex.from_tuples(
                        [(0, "a"), (1, "b")], names=["l1", "l2"]
                    ),
                ),
            ),
            (
                "df_arrow",
                True,
                pd.DataFrame(
                    [[0, 1], [2, 3]],
                    columns=pd.MultiIndex.from_tuples(
                        [(0, "a"), (1, "b")], names=["l1", "l2"]
                    ),
                ).astype("int64[pyarrow]"),
            ),
            ("series", True, pd.Series([1, 2, 3, 4], name="series")),
            (
                "series_arrow",
                True,
                pd.Series([1, 2, 3, 4], name="series").astype("int64[pyarrow]"),
            ),
        ],
        ids=idfn,
    )
    def test_types_w_pd(self, pd_backend, temp_dir, key, ignore_dtypes, expected):
        """Test preservation of types, dtypes, and contents."""
        file = temp_dir / f"test_types_w_pd_{key}_{pd_backend}_{ignore_dtypes}.zip"
        with DataZip(file, "w") as z0:
            z0[key] = expected
            pass
        with DataZip(file, mode="r", ignore_pd_dtypes=ignore_dtypes) as z1:
            read = z1[key]
            if not isinstance(read, type(expected)):
                raise AssertionError(
                    f"test_types for {key} {type(read)} != {type(expected)}"
                )
            # if mode.dtype_backend == 'pyarrow', the original type was not pyarrow,
            # and we ignore dtypes, what we read back will be a different type than
            # the original
            if all(
                (
                    "arrow" in pd_backend,
                    "arrow" not in key,
                    ignore_dtypes,
                    pd.__version__ < "2.0.0",
                )
            ):
                with pytest.raises(AssertionError):
                    assert_equal(read, expected)
                assert_equal(read, expected, check_pd_dtype=False)
            else:
                assert_equal(read, expected)

    def test_dup_names2(self, pd_backend, temp_dir):
        """Test what duplicate named items become."""
        file = temp_dir / f"test_dup_names2_{pd_backend}.zip"
        with DataZip(file, "w") as z0:
            z0["a"] = {
                "a": {"b": pd.Series([1, 2])},
                "b": {"b": pd.Series([1, 3])},
                "c": {"b": pd.Series([1, 4])},
                "d": {"b": pd.Series([1, 5])},
            }
            assert all(
                x in z0.namelist()
                for x in (
                    "b.parquet",
                    "0_b.parquet",
                    "1_b.parquet",
                    "2_b.parquet",
                )
            )

    @pytest.mark.parametrize(
        "name, obj, reset_ids",
        [
            pytest.param("df_reset", pd.DataFrame([[1, 2], [4, 1000]]), True),
            pytest.param("df", pd.DataFrame([[1, 2], [4, 1000]]), False),
        ],
        ids=idfn,
    )
    def test_reset_id(self, pd_backend, temp_dir, name, obj, reset_ids):
        """Test that resetting IDs stores object with same ID separately."""
        file = temp_dir / f"test_id_reset_{name}_{pd_backend}.zip"
        with DataZip(file, "w") as z0:
            z0["a"] = obj
            if reset_ids:
                z0.reset_ids()
            z0["b"] = obj
        with DataZip(file, "r") as z1:
            if reset_ids:
                assert all(
                    x in z0.namelist()
                    for x in (
                        "a.parquet",
                        "b.parquet",
                    )
                )
                assert id(z1["a"]) != id(z1["b"])
            else:
                assert id(z1["a"]) == id(z1["b"])

    @pytest.mark.parametrize(
        "name, obj, test",
        [
            pytest.param("df", pd.DataFrame([[1, 2], [4, 1000]]), "namelist"),
            pytest.param(
                "df_mi",
                pd.DataFrame({(0, "a"): [1, 2], (0, "b"): [4, 1000]}),
                "namelist",
            ),
            pytest.param(
                "df_Int", pd.DataFrame([[1, 2], [4, 1000]], dtype="Int64"), "namelist"
            ),
            pytest.param(
                "df_mi_Int",
                pd.DataFrame({(0, "a"): [1, 2], (0, "b"): [4, 1000]}, dtype="Int64"),
                "namelist",
            ),
            pytest.param("seires", pd.Series([1, 2, 4, 1000]), "namelist"),
            pytest.param("nparray", np.array([1, 2, 3]), "namelist"),
            pytest.param("obj", _TestKlass(a=5, b={"c": (2, 3.5)}, c=5.5), "state"),
        ],
        ids=idfn,
    )
    def test_dup(self, pd_backend, temp_dir, name, obj, test):
        """Test that object referenced multiple times is stored once."""
        with DataZip(temp_dir / f"test_dup_{name}_{pd_backend}.zip", "w") as z0:
            z0["a"] = obj
            z0["b"] = obj
            assert len(z0) == 2
            if test == "namelist":
                assert "b.parquet" not in z0.namelist()
            elif test == "state":
                assert len(z0["__state__"]) == 1
        with DataZip(temp_dir / f"test_dup_{name}_{pd_backend}.zip", "r") as z1:
            a = z1["a"]
            b = z1["b"]
            assert id(a) == id(b)
            assert_equal(a, obj)
            assert_equal(b, obj)

    @pytest.mark.parametrize(
        "klass",
        [
            _TestKlassSlotsCore,
            _KlassSlots,
            _TestKlassSlotsDict,
            _TestKlassCore,
            _TestKlass,
        ],
        ids=idfn,
    )
    def test_embedded_state_obj(self, pd_backend, temp_dir, klass):
        """Test creating a datazip with a :class:`typing.NamedTuple` in it."""
        obj = self.make_klass(klass)
        file = (
            temp_dir / f"test_embedded_state_obj_{klass.__qualname__}_{pd_backend}.zip"
        )
        with DataZip(file, "w") as z0:
            z0["a"] = obj
        with DataZip(file, "r") as z1:
            obj1 = z1["a"]
        assert obj == obj1

    @pytest.mark.parametrize(
        "klass",
        [
            _TestKlassSlotsCore,
            _KlassSlots,
            _TestKlassSlotsDict,
            _TestKlassCore,
            _TestKlass,
        ],
        ids=idfn,
    )
    def test_datazip_dump_load(self, pd_backend, temp_dir, klass):
        """Test dump/load on different sorts of objects."""
        obj = self.make_klass(klass)
        file = (
            temp_dir / f"test_datazip_dump_load_{klass.__qualname__}_{pd_backend}.zip"
        )
        DataZip.dump(obj, file)
        obj1 = DataZip.load(file)
        assert obj1 == obj

    def test_datazip_dump_load_buffer(self, pd_backend):
        """Test creating dump/load with slots."""
        obj = self.make_klass(_KlassSlots)
        DataZip.dump(obj, temp := BytesIO())
        obj1 = DataZip.load(temp)
        assert obj == obj1

    @staticmethod
    def make_klass(klass):
        """Create instance of ``klass``."""
        return klass(
            foo="foo",
            _dfs={
                "a": pd.DataFrame(
                    [[0, 1], [2, 3]],
                    columns=pd.MultiIndex.from_tuples([(0, "a"), (1, "b")]),
                )
            },
            tup=(0, 1),
            lis=["a", 2],
        )


@pytest.mark.parametrize("name", ["plDataFrame", "plSeries"], ids=idfn)
def test_polars(temp_dir, name):
    """Test with polars."""
    pl = pytest.importorskip("polars")

    file = temp_dir / f"polars_{name}.zip"
    if name == "plDataFrame":
        obj = pl.DataFrame({"a": [0, 1], "b": [2, 1]})
    else:
        obj = pl.Series([0, 1])
    with DataZip(file, "w") as z0:
        z0["a"] = obj
        z0["b"] = obj
        assert "b.parquet" not in z0.namelist()
    with DataZip(file, "r") as z1:
        obj1 = z1["a"]
        b = z1["b"]
    if name == "plDataFrame":
        assert obj.frame_equal(obj1)
    else:
        assert obj.series_equal(obj1)
    assert id(obj1) == id(b)


@pytest.mark.skip()
def test_d_legacy():
    """Place to test random existing DataZips."""
    with DataZip(Path.home() / "PycharmProjects/patio-model/re_data_2.zip", "r") as dz:
        print(dz["a"])


@pytest.mark.xfail(reason="Functionality removed")
def test_dfs_to_from_zip(temp_dir):
    """Dfs are same after being written and read back."""
    df_dict = {
        "a": pd.DataFrame(
            [[0, 1], [2, 3]],
            columns=pd.MultiIndex.from_tuples([(0, "a"), (1, "b")]),
        ),
        "b": pd.Series([1, 2, 3, 4]),
    }
    DataZip.dfs_to_zip(
        temp_dir / "test_dfs_to_from_zip",
        df_dict,
    )
    df_load = DataZip.dfs_from_zip(temp_dir / "test_dfs_to_from_zip")
    for a, b in zip(df_dict.values(), df_load.values()):  # noqa: B905
        assert a.compare(b).empty
