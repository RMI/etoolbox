"""Core :class:`.DataZip` tests."""
import numpy as np
import pandas as pd
import pytest

from etoolbox.datazip import DataZip
from etoolbox.datazip.test_classes import ObjMeta


def test_dfs_to_from_zip(temp_dir):
    """Dfs are same after being written and read back."""
    df_dict = {
        "a": pd.DataFrame(
            [[0, 1], [2, 3]],
            columns=pd.MultiIndex.from_tuples([(0, "a"), (1, "b")]),
        ),
        "b": pd.Series([1, 2, 3, 4]),
    }
    try:
        DataZip.dfs_to_zip(
            temp_dir / "test_dfs_to_from_zip",
            df_dict,
        )
        df_load = DataZip.dfs_from_zip(temp_dir / "test_dfs_to_from_zip")
        for a, b in zip(df_dict.values(), df_load.values()):
            assert a.compare(b).empty
    except Exception as exc:
        raise AssertionError("Something broke") from exc


def test_datazip(temp_dir):
    """Test :class:`.DataZip`."""
    df_dict = {
        "a": pd.DataFrame(
            [[0, 1], [2, 3]],
            columns=pd.MultiIndex.from_tuples([(0, "a"), (1, "b")]),
        ),
        "b": pd.Series([1, 2, 3, 4]),
    }
    try:
        with DataZip(temp_dir / "test_datazip.zip", "w") as z:
            z.writed("a", df_dict["a"])
            z.writed("b", df_dict["b"])
            z.writed("c", {1: 3, "3": "fifteen", 5: (0, 1)})
            z.writed("aa", df_dict["a"].loc[:, (0, "a")])
            z.writed("d", "hello world")
            with pytest.raises(FileExistsError):
                z.writed("c", {1: 3, "3": "fifteen", 5: (0, 1)})
            with pytest.raises(FileExistsError):
                z.writed("b", df_dict["b"])
    except Exception as exc:
        raise AssertionError("Something broke") from exc
    else:
        with DataZip(temp_dir / "test_datazip.zip", "r") as z1:
            for n in ("a", "b", "c", "d"):
                assert n in z1._contents
            assert "a" in z1._no_pqt_cols
            assert isinstance(z1.read("a"), pd.DataFrame)
            assert isinstance(z1.read("b"), pd.Series)
            assert isinstance(z1.read("aa"), pd.Series)
            assert isinstance(z1.read("c"), dict)
            assert isinstance(z1.read("d"), str)
            assert z1.read("d") == "hello world"


def test_datazip_meta_safety(temp_dir):
    """Test :class:`.DataZip`."""
    try:
        with DataZip(temp_dir / "test_datazip_meta_safety.zip", "w") as z:
            with pytest.raises(ValueError):
                z.writed("__metadata__", {1: 3, "3": "fifteen", 5: (0, 1)})
            with pytest.raises(ValueError):
                z.writed("__attributes__", {1: 3, "3": "fifteen", 5: (0, 1)})
    except Exception as exc:
        raise AssertionError("Something broke") from exc


def test_datazip_meta_readwrite(temp_dir):
    """Test read/write metadata without key."""
    try:
        with DataZip(temp_dir / "test_datazip_meta_readwrite.zip", "w") as z:
            with pytest.raises(TypeError):
                z.writem(None, 3)
            z.writem(None, {"foo": "bar"})
    except Exception as exc:
        raise AssertionError("Something broke") from exc
    else:
        with DataZip(temp_dir / "test_datazip_meta_readwrite.zip", "r") as z1:
            assert z1.readm() == {"foo": "bar"}


def test_datazip_meta_readwrite_key(temp_dir):
    """Test write metadata by key and read by key."""
    try:
        with DataZip(temp_dir / "test_datazip_meta_readwrite_key.zip", "w") as z:
            z.writem("foo", "bar")
    except Exception as exc:
        raise AssertionError("Something broke") from exc
    else:
        with DataZip(temp_dir / "test_datazip_meta_readwrite_key.zip", "r") as z1:
            assert z1.readm("foo") == "bar"


def test_datazip_numpy(temp_dir):
    """Test :class:`.DataZip`."""
    array = np.array([[0.0, 4.1], [3.2, 2.1]])
    try:
        with DataZip(temp_dir / "test_datazip_numpy.zip", "w") as z:
            z.writed("np", array)
    except Exception as exc:
        raise AssertionError("Something broke") from exc
    else:
        with DataZip(temp_dir / "test_datazip_numpy.zip", "r") as z1:
            ar = z1.read("np")
            assert isinstance(ar, np.ndarray)
            assert np.all(ar == array)


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


def test_timestamp_in_datazip(temp_dir):
    """Test creating a datazip with a :class:`utils.Timestamp` in it."""
    dm = pd.Timestamp("2022-10-21")
    df = pd.DataFrame(
        [[0, 1], [2, 3]],
        columns=pd.MultiIndex.from_tuples([(0, "a"), (1, "b")]),
    )

    with DataZip(temp_dir / "test_timestamp_in_datazip.zip", "w") as z0:
        z0.writed("df", df)
        z0.writed("dm", dm)
    with DataZip(temp_dir / "test_timestamp_in_datazip.zip", "r") as self:
        dm1 = self.read("dm")
        assert isinstance(dm1, pd.Timestamp)


def test_nested_obj_in_datazip(temp_dir):
    """Test creating a datazip with a :class:`utils.Timestamp` in it."""
    dt = {"a": pd.Timestamp("2022-10-21")}
    df = pd.DataFrame(
        [[0, 1], [2, 3]],
        columns=pd.MultiIndex.from_tuples([(0, "a"), (1, "b")]),
    )
    ddf = {
        "a": pd.DataFrame(
            [[0, 100], [2, 3]],
            columns=pd.MultiIndex.from_tuples([(0, "a"), (1, "b")]),
        )
    }

    with DataZip(temp_dir / "test_nested_obj_in_datazip.zip", "w") as z0:
        z0.writed("df", df)
        z0.writed("dt", dt)
        z0.writed("ddf", ddf)
    with DataZip(temp_dir / "test_nested_obj_in_datazip.zip", "r") as self:
        dt1 = self.read("dt")
        assert isinstance(dt1["a"], pd.Timestamp)


def test_nested2_obj_in_datazip(temp_dir):
    """Test creating a datazip with a :class:`utils.Timestamp` nested in it."""
    lt = ["a", {"time": pd.Timestamp("2022-10-21")}]

    with DataZip(temp_dir / "test_nested2_obj_in_datazip.zip", "w") as z0:
        z0.writed("lt", lt)
        pass
    with DataZip(temp_dir / "test_nested2_obj_in_datazip.zip", "r") as self:
        lt1 = self.read("lt")
        assert isinstance(lt1, list)
        assert isinstance(lt1[0], str)
        assert isinstance(lt1[1], dict)
        assert isinstance(lt1[1]["time"], pd.Timestamp)


def test_namedtuple_in_datazip(temp_dir):
    """Test creating a datazip with a :class:`typing.NamedTuple` in it."""
    df = pd.DataFrame(
        [[0, 1], [2, 3]],
        columns=pd.MultiIndex.from_tuples([(0, "a"), (1, "b")]),
    )
    om = ObjMeta("this", "that")
    with DataZip(temp_dir / "test_namedtuple_in_datazip.zip", "w") as z0:
        z0.writed("df", df)
        z0.writed("om", om)
        z0.writed("tup", (1, 2, (1, 2, 3)))
        z0.writed("l", [om, om])
        z0.writed("d", {"a": (1, 2, 3), "b": [32, 45], 3: 4})
    with DataZip(temp_dir / "test_namedtuple_in_datazip.zip", "r") as self:
        om1 = self.read("om")
        self.read("tup")
        assert isinstance(om1, ObjMeta)
