"""Core :class:`.DataZip` tests."""
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import NamedTuple

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
from etoolbox.utils.testing import assert_equal, idfn


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
    for a, b in zip(df_dict.values(), df_load.values()):
        assert a.compare(b).empty


def test_existing_name(temp_dir):
    """Test existing name protection."""
    with DataZip(temp_dir / "test_datazip.zip", "w") as z:
        z["a"] = "a"
        with pytest.raises(FileExistsError):
            z["a"] = 5


def test_datazip_contains_len(temp_dir):
    """Test override of ``in``."""
    with DataZip(temp_dir / "test_datazip_contains_len.zip", "w") as z:
        z["a"] = pd.Series([1, 2, 3, 4])
    with DataZip(temp_dir / "test_datazip_contains_len.zip", "r") as z1:
        assert "a" in z1
        assert "a.parquet" in z1
        assert len(z) == 1


@pytest.mark.parametrize("protected", ["__metadata__", "__attributes__"], ids=idfn)
def test_datazip_meta_safety(temp_dir, protected):
    """Test :class:`.DataZip`."""
    with DataZip(temp_dir / f"test_datazip_protection_{protected}.zip", "w") as z:
        with pytest.raises(ValueError):
            z[protected] = 3


def test_datazip_w(temp_dir):
    """Test writing to existing :class:`.DataZip`."""
    df_dict = {
        "a": pd.DataFrame(
            [[0, 1], [2, 3]],
            columns=pd.MultiIndex.from_tuples([(0, "a"), (1, "b")]),
        ),
    }
    try:
        with DataZip(temp_dir / "test_datazip_w.zip", "w") as z0:
            z0.writed("a", df_dict["a"])
    except Exception as exc:
        raise AssertionError("Something broke") from exc
    else:
        with DataZip(temp_dir / "test_datazip_w.zip", "r") as z1:
            assert "a" in z1._no_pqt_cols
        with pytest.raises(ValueError):
            with DataZip(temp_dir / "test_datazip_w.zip", "a") as z2a:
                z2a.namelist()
        with pytest.raises(ValueError):
            with DataZip(temp_dir / "test_datazip_w.zip", "x") as z2x:
                z2x.namelist()
        with pytest.raises(FileExistsError):
            with DataZip(temp_dir / "test_datazip_w.zip", "w") as z3:
                z3.namelist()


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
def test_datazip_dump_load(temp_dir, klass):
    """Test dump/load on different sorts of objects."""
    obj = klass(
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
    file = temp_dir / f"test_datazip_dump_load_{obj.__class__.__qualname__}.zip"
    DataZip.dump(obj, file)
    obj1 = DataZip.load(file)
    assert obj1 == obj


def test_datazip_dump_load_buffer():
    """Test creating dump/load with slots."""
    obj = _KlassSlots(
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
    DataZip.dump(obj, temp := BytesIO())
    obj1 = DataZip.load(temp)
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
def test_embedded_state_obj(temp_dir, klass):
    """Test creating a datazip with a :class:`typing.NamedTuple` in it."""
    obj = klass(
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
    file = temp_dir / f"test_embedded_state_obj_{obj.__class__.__qualname__}.zip"
    with DataZip(file, "w") as z0:
        z0["a"] = obj
    with DataZip(file, "r") as z1:
        obj1 = z1["a"]
    assert obj == obj1


@pytest.mark.parametrize(
    "obj, expected",
    [
        (5, None),
        (_TestKlassSlotsCore(foo=5), (None, {"foo": 5})),
        (_TestKlassCore(foo=5), {"foo": 5}),
        (_TestKlassSlotsDict(foo=5).add_to_dict("bar", 6), ({"bar": 6}, {"foo": 5})),
    ],
    ids=idfn,
)
def test_default_get_state(obj, expected):
    """Test default version of getstate."""
    assert DataZip.default_getstate(obj) == expected


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
def test_default_set_state(obj, state, expected):
    """Test default version of setstate."""
    inst = obj.__new__(obj)
    DataZip.default_setstate(inst, state)
    assert inst == expected


def test_none(temp_dir):
    """Test that ``None`` can be properly stored."""
    with DataZip(temp_dir / "test_none.zip", "w") as z0:
        z0["a"] = None
        z0["b"] = {"foo": None, "bar": 3}
    with DataZip(temp_dir / "test_none.zip", "r") as z1:
        assert z1["a"] is None
        assert z1["b"]["foo"] is None


def test_tuple_in_testklass(temp_dir):
    """Test preservation of tuples in a class."""
    obj = _TestKlass(a=5, b={"c": (2, 3.5)}, c=5.5)
    DataZip.dump(obj, temp_dir / "test_tuple_in_testklass.zip")
    obj1 = DataZip.load(temp_dir / "test_tuple_in_testklass.zip")
    assert obj1.b["c"] == (2, 3.5)
    assert obj1.a == 5


def test_namedtuple_fallback(temp_dir):
    """Test named tuple fallback."""
    class NTF(NamedTuple):
        a: int
        b: int

    with DataZip(temp_dir / "test_namedtuple_fallback.zip", "w") as z0:
        z0["a"] = NTF(a=3, b=3)
    with DataZip(temp_dir / "test_namedtuple_fallback.zip", "r") as z1:
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
        ("tuple_w_series", (1, pd.Series([1, 2, 3, 4], name="series"))),
        ("set", {1, 2, 4}),
        ("frozenset", frozenset((1, 2, 4))),
        ("string", "this"),
        ("int", 3),
        ("true", True),
        ("false", False),
        ("none", None),
        ("complex", {"d": 2 + 3j}),
        ("datetime", datetime.now()),
        ("pdTimestamp", pd.Timestamp(datetime.now())),
        # this shouldn't really exist outside an array, right?
        pytest.param(
            "npdatetime64", np.datetime64(datetime.now()), marks=pytest.mark.xfail
        ),
        ("namedtuple_nested", [(ObjMeta("this", "that"), 5), 4, (1, 2)]),
        ("namedtuple", ObjMeta("this", "that")),
        ("np.array", np.array([[0.0, 4.1], [3.2, 2.1]])),
        (
            "df",
            pd.DataFrame(
                [[0, 1], [2, 3]],
                columns=pd.MultiIndex.from_tuples(
                    [(0, "a"), (1, "b")], names=["l1", "l2"]
                ),
            ),
        ),
        ("series", pd.Series([1, 2, 3, 4], name="series")),
        ("series_tp_name", pd.Series([1, 2, 3, 4], name=(0, "a"))),
        ("series_no_name", pd.Series([1, 2, 3, 4])),
        ("_TestKlass", _TestKlass(a=5, b={"c": (2, 3.5)}, c=5.5)),
        ("path", Path.home()),
    ],
    ids=idfn,
)
def test_types(temp_dir, key, expected):
    """Test preservation of tuples."""
    with DataZip(temp_dir / f"test_types_{key}.zip", "w") as z0:
        z0[key] = expected
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
    with DataZip.replace(file, save_old=save_old) as z1:
        assert "a" in z1
        z1["c"] = {"this": "that"}
    if save_old:
        assert (temp_dir / f"test_replace_{save_old}_old.zip").exists()
    else:
        assert not (temp_dir / f"test_replace_{save_old}_old.zip").exists()
    with DataZip(file, "r") as z2:
        assert z2["a"] is None
        assert z2["b"] == {"foo": None, "bar": 3}
        assert z2["c"] == {"this": "that"}
