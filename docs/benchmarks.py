"""Test read, write, and size of DataZips vs Pickles."""

import pickle
import random
import shutil
import string
import timeit
from pathlib import Path

import numpy as np
import pandas as pd
from etoolbox.datazip import DataZip
from etoolbox.datazip._test_classes import _TestKlass


def write_as_pkl(obj, path):
    with open(path, "wb") as f:
        pickle.dump(obj, f)


def read_dz(path):
    return DataZip.load(path)


def read_pkl(path):
    with open(path, "rb") as f:
        return pickle.load(f)


def main():
    path = Path(__file__).parent / "temp"
    path.mkdir(exist_ok=True)
    many_small = _TestKlass(
        **{
            str(i): pd.DataFrame(np.random.rand(100, 100)).astype(
                {i: "string" for i in range(30)} | {i: int for i in range(30, 60)}
            )
            for i in range(100)
        },
    )
    few_big = _TestKlass(
        **{
            str(i): pd.DataFrame(np.random.rand(int(1e3), 100)).astype(
                {i: "string" for i in range(30)} | {i: int for i in range(30, 60)}
            )
            for i in range(20)
        },
    )
    huge = _TestKlass(
        huge=pd.DataFrame(np.random.rand(int(1e6), 20)).astype(
            {i: "string" for i in range(6)} | {i: int for i in range(6, 12)}
        )
    )
    dicts = _TestKlass(
        data={
            **{
                ("dicts", i): {
                    str(j): "".join(
                        random.choice(string.ascii_uppercase + string.digits)
                        for _ in range(6)
                    )
                    for j in range(1000)
                }
                for i in range(25)
            },
            **{
                f"cplx{i}": {
                    str(j): complex(random.random() + random.random())
                    for j in range(1000)
                }
                for i in range(25)
            },
            **{
                f"tups{i}": {
                    str(j): tuple(random.random() for _ in range(random.randint(1, 50)))
                    for j in range(1000)
                }
                for i in range(25)
            },
        }
    )

    benchmarks = pd.Series(
        index=pd.MultiIndex.from_product(
            [["dz", "pkl"], ["small", "big", "huge"], ["read", "write", "size"]]
        )
    )

    iters = [
        (
            ("dz", "small", "write"),
            "DataZip.dump(many_small, path / 'small_dz.zip', clobber=True)",
        ),
        (("dz", "small", "read"), "DataZip.load(path / 'small_dz.zip')"),
        (("dz", "small", "size"), path / "small_dz.zip"),
        (
            ("dz", "big", "write"),
            "DataZip.dump(few_big, path / 'big_dz.zip', clobber=True)",
        ),
        (("dz", "big", "read"), "DataZip.load(path / 'big_dz.zip')"),
        (("dz", "big", "size"), path / "big_dz.zip"),
        (
            ("dz", "huge", "write"),
            "DataZip.dump(huge, path / 'huge_dz.zip', clobber=True)",
        ),
        (("dz", "huge", "read"), "DataZip.load(path / 'huge_dz.zip')"),
        (("dz", "huge", "size"), path / "huge_dz.zip"),
        (
            ("dz", "dicts", "write"),
            "DataZip.dump(dicts, path / 'dicts_dz.zip', clobber=True)",
        ),
        (("dz", "dicts", "read"), "DataZip.load(path / 'dicts_dz.zip')"),
        (("dz", "dicts", "size"), path / "dicts_dz.zip"),
        (("pkl", "small", "write"), "write_as_pkl(many_small, path / 'small_pkl.pkl')"),
        (("pkl", "small", "read"), "read_pkl(path / 'small_pkl.pkl')"),
        (("pkl", "small", "size"), path / "small_pkl.pkl"),
        (("pkl", "big", "write"), "write_as_pkl(few_big, path / 'big_pkl.pkl')"),
        (("pkl", "big", "read"), "read_pkl(path / 'big_pkl.pkl')"),
        (("pkl", "big", "size"), path / "big_pkl.pkl"),
        (("pkl", "huge", "write"), "write_as_pkl(huge, path / 'huge_pkl.pkl')"),
        (("pkl", "huge", "read"), "read_pkl(path / 'huge_pkl.pkl')"),
        (("pkl", "huge", "size"), path / "huge_pkl.pkl"),
        (("pkl", "dicts", "write"), "write_as_pkl(dicts, path / 'dicts_pkl.pkl')"),
        (("pkl", "dicts", "read"), "read_pkl(path / 'dicts_pkl.pkl')"),
        (("pkl", "dicts", "size"), path / "dicts_pkl.pkl"),
    ]
    for ix, cmd in iters:
        if "size" in ix:
            benchmarks.loc[ix] = cmd.lstat().st_size * 1e-6
        else:
            benchmarks.loc[ix] = min(
                timeit.repeat(cmd, globals=locals() | globals(), number=1, repeat=3)
            )

    benchmarks.unstack(0).to_csv("benchmarks.csv")  # noqa: PD010
    shutil.rmtree(path)


if __name__ == "__main__":
    main()
