"""Core :class:`.DataZip` tests."""
import numpy as np
import pandas as pd
import pytest

from etoolbox.datazip import DataZip
from etoolbox.datazip.test_classes import ObjMeta


def test_dfs_to_from_zip(test_dir):
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
            test_dir / "df_test",
            df_dict,
        )
        df_load = DataZip.dfs_from_zip(test_dir / "df_test")
        for a, b in zip(df_dict.values(), df_load.values()):
            assert a.compare(b).empty
    except Exception as exc:
        raise AssertionError("Something broke") from exc
    finally:
        (test_dir / "df_test.zip").unlink(missing_ok=True)


def test_datazip(test_dir):
    """Test :class:`.DataZip`."""
    df_dict = {
        "a": pd.DataFrame(
            [[0, 1], [2, 3]],
            columns=pd.MultiIndex.from_tuples([(0, "a"), (1, "b")]),
        ),
        "b": pd.Series([1, 2, 3, 4]),
    }
    try:
        with DataZip(test_dir / "obj.zip", "w") as z:
            z.writed("a", df_dict["a"])
            z.writed("b", df_dict["b"])
            z.writed("c", {1: 3, "3": "fifteen", 5: (0, 1)})
            z.writed("d", "hello world")
            with pytest.raises(FileExistsError):
                z.writed("c", {1: 3, "3": "fifteen", 5: (0, 1)})
            with pytest.raises(FileExistsError):
                z.writed("b", df_dict["b"])
    except Exception as exc:
        raise AssertionError("Something broke") from exc
    else:
        with DataZip(test_dir / "obj.zip", "r") as z1:
            for n in ("a", "b", "c", "d"):
                assert n in z1._contents
            assert "a" in z1._bad_cols
            assert isinstance(z1.read("a"), pd.DataFrame)
            assert isinstance(z1.read("b"), pd.Series)
            assert isinstance(z1.read("c"), dict)
            assert isinstance(z1.read("d"), str)
            assert z1.read("d") == "hello world"
    finally:
        (test_dir / "obj.zip").unlink(missing_ok=True)


def test_datazip_meta_safety(test_dir):
    """Test :class:`.DataZip`."""
    try:
        with DataZip(test_dir / "obj.zip", "w") as z:
            with pytest.raises(ValueError):
                z.writed("__metadata__", {1: 3, "3": "fifteen", 5: (0, 1)})
            with pytest.raises(ValueError):
                z.writed("__attributes__", {1: 3, "3": "fifteen", 5: (0, 1)})
    except Exception as exc:
        raise AssertionError("Something broke") from exc
    finally:
        (test_dir / "obj.zip").unlink(missing_ok=True)


def test_datazip_numpy(test_dir):
    """Test :class:`.DataZip`."""
    array = np.array([[0.0, 4.1], [3.2, 2.1]])
    try:
        with DataZip(test_dir / "obj.zip", "w") as z:
            z.writed("np", array)
    except Exception as exc:
        raise AssertionError("Something broke") from exc
    else:
        with DataZip(test_dir / "obj.zip", "r") as z1:
            ar = z1.read("np")
            assert isinstance(ar, np.ndarray)
            assert np.all(ar == array)
    finally:
        (test_dir / "obj.zip").unlink(missing_ok=True)


def test_datazip_w(test_dir):
    """Test writing to existing :class:`.DataZip`."""
    df_dict = {
        "a": pd.DataFrame(
            [[0, 1], [2, 3]],
            columns=pd.MultiIndex.from_tuples([(0, "a"), (1, "b")]),
        ),
    }
    try:
        with DataZip(test_dir / "obj.zip", "w") as z0:
            z0.writed("a", df_dict["a"])
    except Exception as exc:
        raise AssertionError("Something broke") from exc
    else:
        with DataZip(test_dir / "obj.zip", "r") as z1:
            assert "a" in z1._bad_cols
        with pytest.raises(ValueError):
            with DataZip(test_dir / "obj.zip", "a") as z2a:
                z2a.namelist()
        with pytest.raises(ValueError):
            with DataZip(test_dir / "obj.zip", "x") as z2x:
                z2x.namelist()
        with pytest.raises(FileExistsError):
            with DataZip(test_dir / "obj.zip", "w") as z3:
                z3.namelist()
    finally:
        (test_dir / "obj.zip").unlink(missing_ok=True)


def test_timestamp_in_datazip(test_dir):
    """Test creating a datazip with a :class:`pandas.Timestamp` in it."""
    dm = pd.Timestamp("2022-10-21")
    df = pd.DataFrame(
        [[0, 1], [2, 3]],
        columns=pd.MultiIndex.from_tuples([(0, "a"), (1, "b")]),
    )

    try:
        with DataZip(test_dir / "obj.zip", "w") as z0:
            z0.writed("df", df)
            z0.writed("dm", dm)
        with DataZip(test_dir / "obj.zip", "r") as self:
            dm1 = self.read("dm")
            assert isinstance(dm1, pd.Timestamp)
    finally:
        (test_dir / "obj.zip").unlink(missing_ok=True)


def test_nested_obj_in_datazip(test_dir):
    """Test creating a datazip with a :class:`pandas.Timestamp` in it."""
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

    try:
        with DataZip(test_dir / "obj1.zip", "w") as z0:
            z0.writed("df", df)
            z0.writed("dt", dt)
            z0.writed("ddf", ddf)
            pass
        with DataZip(test_dir / "obj1.zip", "r") as self:
            dt1 = self.read("dt")
            assert isinstance(dt1["a"], pd.Timestamp)
    finally:
        (test_dir / "obj1.zip").unlink(missing_ok=True)


def test_nested2_obj_in_datazip(test_dir):
    """Test creating a datazip with a :class:`pandas.Timestamp` nested in it."""
    lt = ["a", {"time": pd.Timestamp("2022-10-21")}]

    try:
        with DataZip(test_dir / "obj.zip", "w") as z0:
            z0.writed("lt", lt)
            pass
        with DataZip(test_dir / "obj.zip", "r") as self:
            lt1 = self.read("lt")
            assert isinstance(lt1, list)
            assert isinstance(lt1[0], str)
            assert isinstance(lt1[1], dict)
            assert isinstance(lt1[1]["time"], pd.Timestamp)
    finally:
        (test_dir / "obj.zip").unlink(missing_ok=True)


def test_namedtuple_in_datazip(test_dir):
    """Test creating a datazip with a :class:`typing.NamedTuple` in it."""
    df = pd.DataFrame(
        [[0, 1], [2, 3]],
        columns=pd.MultiIndex.from_tuples([(0, "a"), (1, "b")]),
    )
    om = ObjMeta("this", "that")

    try:
        with DataZip(test_dir / "obj.zip", "w") as z0:
            z0.writed("df", df)
            z0.writed("om", om)
            z0.writed("tup", (1, 2, (1, 2, 3)))
            z0.writed("l", [om, om])
            z0.writed("d", {"a": (1, 2, 3), "b": [32, 45], 3: 4})
        with DataZip(test_dir / "obj.zip", "r") as self:
            om1 = self.read("om")
            self.read("tup")
            assert isinstance(om1, ObjMeta)
    finally:
        (test_dir / "obj.zip").unlink(missing_ok=True)
