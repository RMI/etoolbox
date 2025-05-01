"""Tools for working with RMI's Azure storage."""

import json
import logging
import os
import shutil
import subprocess
from contextlib import nullcontext
from datetime import datetime
from functools import lru_cache
from pathlib import Path

import numpy as np
import pandas as pd
from fsspec import filesystem
from fsspec.implementations.cached import WholeFileCacheFileSystem
from platformdirs import user_cache_path, user_config_path

from etoolbox.datazip import DataZip
from etoolbox.utils.misc import all_logging_disabled

try:
    import tqdm  # noqa: F401
    from fsspec.callbacks import TqdmCallback as ProgressCallback

except (ImportError, ModuleNotFoundError):
    from fsspec.callbacks import DotPrinterCallback as ProgressCallback


CONFIG_PATH = user_config_path("rmi.cloud", ensure_exists=True)
AZURE_CACHE_PATH = user_cache_path("rmi.cloud", ensure_exists=True)
RMICFEZIL_TOKEN_PATH = CONFIG_PATH / "rmicfezil_token.txt"

logger = logging.getLogger("etoolbox")


def rmi_cloud_clean(args):
    """Cleanup cache and config directories."""
    info = cache_info()
    size = info["size"].sum() * 1e-6
    print(f"Will delete the following items using {size:,.0f} MB at {AZURE_CACHE_PATH}")
    print(info[["size", "time"]])
    if not args.dry:
        shutil.rmtree(AZURE_CACHE_PATH, ignore_errors=True)
    if args.all:
        print(f"deleting config {CONFIG_PATH}")
        if not args.dry:
            shutil.rmtree(CONFIG_PATH, ignore_errors=True)


def rmi_cloud_init(args):
    """Write SAS token file to disk."""
    if RMICFEZIL_TOKEN_PATH.exists():
        if not args.clobber:
            raise FileExistsError("SAS Token already exists.")
        print(f"deleting {RMICFEZIL_TOKEN_PATH}")
        if not args.dry:
            RMICFEZIL_TOKEN_PATH.unlink()
    print(f"write {args.token} to {RMICFEZIL_TOKEN_PATH}")
    if args.dry:
        return
    with open(RMICFEZIL_TOKEN_PATH, "w") as f:
        f.write(args.token)
    print(f"SAS Token {args.token} written to {RMICFEZIL_TOKEN_PATH}.")


@lru_cache
def read_token() -> str:
    """Read SAS token from disk or environment variable."""
    if RMICFEZIL_TOKEN_PATH.exists():
        with open(RMICFEZIL_TOKEN_PATH) as f:
            return f.read()
    elif (token := os.environ.get("RMICFEZIL_SAS_TOKEN")) is not None:
        return token
    raise RuntimeError(
        "No SAS Token found, either run `rmi cloud init` or set "
        "RMICFEZIL_SAS_TOKEN environment variable."
    )


def storage_options():
    """Simplify reading from Azure using :mod:`polars`.

    When using :mod:`pandas` or writing to Azure, see :func:`.rmi_cloud_fs`.

    Examples
    --------
    >>> import polars as pl
    >>> from etoolbox.utils.cloud import storage_options

    >>> df = pl.read_parquet("az://raw-data/test_data.parquet", **storage_options())
    >>> df.select("plant_id_eia", "re_type").head()  # doctest: +NORMALIZE_WHITESPACE
    shape: (5, 2)
    ┌──────────────────────┬─────────┐
    │ plant_id_eia         ┆ re_type │
    │ ---                  ┆ ---     │
    │ i64                  ┆ str     │
    ╞══════════════════════╪═════════╡
    │ -1065799821027645681 ┆ solar   │
    │ 500701449105794732   ┆ solar   │
    │ 5264981444132581172  ┆ solar   │
    │ 8596148642566783026  ┆ solar   │
    │ 8293386810295812914  ┆ solar   │
    └──────────────────────┴─────────┘

    """
    return {"storage_options": {"account_name": "rmicfezil", "sas_token": read_token()}}


def rmi_cloud_fs(token=None) -> WholeFileCacheFileSystem:
    """Work with files on Azure.

    This can be used to read or write arbitrary files to or from Azure. And for files
    read from Azure, it will create and manage a local cache.

    Examples
    --------
    >>> import pandas as pd
    >>> from etoolbox.utils.cloud import rmi_cloud_fs

    >>> fs = rmi_cloud_fs()
    >>> df = pd.read_parquet("az://raw-data/test_data.parquet", filesystem=fs)
    >>> df[["plant_id_eia", "re_type"]].head()  # doctest: +NORMALIZE_WHITESPACE
              plant_id_eia re_type
    0 -1065799821027645681   solar
    1   500701449105794732   solar
    2  5264981444132581172   solar
    3  8596148642566783026   solar
    4  8293386810295812914   solar

    Read with :mod:`polars` using the same filecache as with :mod:`pandas`.

    >>> import polars as pl

    >>> with fs.open("az://raw-data/test_data.parquet") as f:
    ...     df = pl.read_parquet(f)
    >>> df.select("plant_id_eia", "re_type").head()  # doctest: +NORMALIZE_WHITESPACE
    shape: (5, 2)
    ┌──────────────────────┬─────────┐
    │ plant_id_eia         ┆ re_type │
    │ ---                  ┆ ---     │
    │ i64                  ┆ str     │
    ╞══════════════════════╪═════════╡
    │ -1065799821027645681 ┆ solar   │
    │ 500701449105794732   ┆ solar   │
    │ 5264981444132581172  ┆ solar   │
    │ 8596148642566783026  ┆ solar   │
    │ 8293386810295812914  ┆ solar   │
    └──────────────────────┴─────────┘

    Write a parquet, or really anything to Azure...

    >>> with fs.open("az://raw-data/file.parquet", mode="wb") as f:  # doctest: +SKIP
    ...     df.write_parquet(f)

    """
    return filesystem(
        "filecache",
        target_protocol="az",
        target_options={
            "account_name": "rmicfezil",
            "sas_token": read_token() if token is None else token,
        },
        cache_storage=str(AZURE_CACHE_PATH),
        check_files=True,
        cache_timeout=None,
    )


def cache_info():
    """Return info about cloud cache contents."""
    import pandas as pd

    with open(AZURE_CACHE_PATH / "cache", "rb") as f:
        cache_data = json.load(f)
    cdl = [
        v
        | {
            "size": (AZURE_CACHE_PATH / v["fn"]).stat().st_size,
            "time": datetime.fromtimestamp(v["time"]),
        }
        for v in cache_data.values()
        if (AZURE_CACHE_PATH / v["fn"]).exists()
    ]
    return pd.DataFrame.from_records(cdl).set_index("original")[
        ["time", "size", "fn", "uid"]
    ]


def cached_path(cloud_path: str) -> str | None:
    """Get the local cache path of a cloud file.

    Args:
        cloud_path: path on azure, eg ``az://raw-data/test_data.parquet``

    Examples
    --------
    >>> import polars as pl
    >>> from etoolbox.utils.cloud import rmi_cloud_fs, cached_path

    >>> fs = rmi_cloud_fs()
    >>> cloud_path = "az://raw-data/test_data.parquet"
    >>> with fs.open(cloud_path) as f:
    ...     df = pl.read_parquet(f)
    >>> cached_path(cloud_path)
    '2a722b95bfff23b14d1deaa81cca3b697b875934df3858159d205d20dcf1e305'

    """
    cloud_path = cloud_path.removeprefix("az://").removeprefix("abfs://")
    try:
        return cache_info().loc[cloud_path, "fn"]
    except KeyError:
        return None


def cloud_list(path: str, *, detail=False) -> list[str] | dict:
    """List cloud files in a folder.

    Args:
        path: remote folder to list contents of e.g. '<container>/...'
        detail: include detail information

    """
    fs = rmi_cloud_fs()
    return fs.ls(path, detail=detail)


AZ_MSG = (
    "azcopy not installed at ``/opt/homebrew/bin/azcopy``\n"
    "for better performance``brew install azcopy``, or set ``azcopy_path`` argument "
    "if installed elsewhere"
    "more info\nhttps://github.com/Azure/azure-storage-azcopy"
)


def get(
    to_get_path: str,
    destination: Path | str,
    fs=None,
    *,
    quiet=True,
    clobber=False,
    azcopy_path="/opt/homebrew/bin/azcopy",
) -> None:
    """Download a remote file from the cloud.

    Uses ``azcopy`` CLI if available.

    Args:
        to_get_path: remote file or folder to download of the form '<container>/...
        destination: local destination for the downloaded files
        fs: filesystem
        quiet: disable logging of adlfs output
        clobber: overwrite existing files and directories if True
        azcopy_path: path to azcopy executable
    """
    to_get_path = to_get_path.removeprefix("az://").removeprefix("abfs://")
    try:
        subprocess.run([azcopy_path], capture_output=True)  # noqa: S603
    except Exception:
        print(AZ_MSG)
        fs = rmi_cloud_fs() if fs is None else fs
        context = all_logging_disabled if quiet else nullcontext
        with context():
            ls = fs.ls(to_get_path)
            if ls[0]["name"] != to_get_path:
                raise TypeError(
                    "`to_get_path` must be a file when not using azcopy."
                ) from None
            fs.get(
                rpath="az://" + to_get_path,
                lpath=str(destination),
                recursive=False,
                callback=ProgressCallback(),
            )
    else:
        subprocess.run(  # noqa: S603
            [
                azcopy_path,
                "cp",
                f"https://rmicfezil.blob.core.windows.net/{to_get_path}?{read_token()}",
                f"{destination}",
                f"--overwrite={str(clobber).casefold()}",
                "--recursive=True",
            ],
        )


def put(
    to_put_path: Path,
    destination: str,
    fs=None,
    *,
    quiet=True,
    clobber=False,
    azcopy_path="/opt/homebrew/bin/azcopy",
) -> None:
    """Upload local files or directories to the cloud.

    Copies a specific file or tree of files. If destination
    ends with a "/", it will be assumed to be a directory, and target files
    will go within.

    Uses ``azcopy`` CLI if available.

    Args:
        to_put_path: local file or folder to copy
        destination: copy destination of the form '<container>/...
        fs: filesystem
        quiet: disable logging of adlfs output
        clobber: force overwriting of existing files (only works when azcopy is used)
        azcopy_path: path to azcopy executable
    """
    if not to_put_path.exists():
        raise FileNotFoundError(to_put_path)
    lpath = str(to_put_path)
    recursive = to_put_path.is_dir()
    try:
        subprocess.run([azcopy_path], capture_output=True)  # noqa: S603
    except Exception:
        print(AZ_MSG)
        context = all_logging_disabled if quiet else nullcontext
        fs = rmi_cloud_fs() if fs is None else fs
        with context():
            fs.put(
                lpath=lpath,
                rpath="az://"
                + destination.removeprefix("az://").removeprefix("abfs://"),
                recursive=recursive,
                callback=ProgressCallback(),
            )
    else:
        subprocess.run(  # noqa: S603
            [
                azcopy_path,
                "cp",
                lpath,
                f"https://rmicfezil.blob.core.windows.net/{destination}?{read_token()}",
                f"--overwrite={str(clobber).casefold()}",
                f"--recursive={str(recursive).casefold()}",
            ],
        )


def read_patio_resource_results(datestr: str) -> dict[str, pd.DataFrame]:
    """Reads patio resource results from Azure.

    Reads patio resource results from Azure and returns the extracted data as a
    dictionary (named list). The method handles the specific format of patio resource
    files and manages file system interactions as well as cache mechanisms.

    Args:
        datestr: Date string that identifies the model run.

    """
    return read_patio_file(datestr, f"BAs_{datestr}_results.zip")


def read_patio_file(
    datestr: str, filename: str
) -> dict[str, pd.DataFrame] | pd.DataFrame:
    """Reads patio data from Azure.

    The method handles the specific format of patio resource
    files and manages file system interactions as well as cache mechanisms.

    Args:
        datestr: Date string that identifies the model run.
        filename: Target filename for reading data.

    """
    fs = rmi_cloud_fs()

    def _zip_read(name):
        f = fs.open(f"az://patio-results/{datestr}/{name}")
        f.close()
        c_path = str(AZURE_CACHE_PATH / cached_path(f"patio-results/{datestr}/{name}"))
        with DataZip(c_path, "r") as z:
            out_dict = dict(z.items())
        return out_dict

    if ".parquet" in filename:
        return pd.read_parquet(
            f"az://patio-results/{datestr}/{filename}", filesystem=fs
        )
    if ".zip" in filename:
        return _zip_read(filename)
    for f in cloud_list(f"patio-results/{datestr}"):
        f = f.rpartition("/")[-1]
        if filename in f and ".parquet" in f:
            return pd.read_parquet(f"az://patio-results/{datestr}/{f}")
        if filename in f and ".csv" in f:
            return pd.read_csv(f"az://patio-results/{datestr}/{f}")
        if filename in f and ".zip" in f:
            return _zip_read(f)
    raise FileNotFoundError(f"patio-results/{datestr}/{filename} not found")


def write_patio_econ_results(df: pd.DataFrame, datestr: str, filename: str):
    """Writes economic results for patio data to a specified filename in Azure storage.

    This function takes a DataFrame containing economic model results and writes the
    DataFrame in the model run directory in Azure Blob Storage.

    Args:
        df: DataFrame containing financial or economic results that
            need to be stored in the Azure Blob Storage.
        datestr: Date string that identifies the model run.
        filename: Target filename for storing the results.

    """
    filename = filename.removesuffix(".parquet")
    fs = rmi_cloud_fs()
    with fs.open(f"az://patio-results/{datestr}/{filename}.parquet", mode="wb") as f:
        df.to_parquet(f)


"""
======================================= For CLI =======================================
These are wrappers to use the above functions from the CLI
"""


def _cache_info(args):
    info = (
        (
            cache_info()
            .reset_index()
            .assign(
                blob=lambda x: x["original"].str.partition("/")[0],
                original=lambda x: x["original"].str.partition("/")[2],
                time=lambda x: x["time"].dt.strftime("%Y-%m-%d %H:%M:%S"),
                fn=lambda x: x["fn"].str.slice(0, 5) + "...",
                uid=lambda x: x["uid"].str.slice(0, 5) + "...",
                size=lambda x: np.round(x["size"] * 1e-6, 1),
            )
        )
        .sort_values(["blob", "time"])
        .set_index(["blob", "original"])[["time", "size", "fn", "uid"]]
    )
    print(info)
    print(f"\nTotal size: {info['size'].sum():,.0f} MB")


def _list(args):
    import pandas as pd

    ls = cloud_list(args.to_list_path, detail=args.detail)
    if args.detail:
        cols = ["size", "creation_time", "last_modified", "type", "etag", "tags"]
        ex = ["content_settings"] if any("content_settings" in d for d in ls) else None
        info = (
            pd.DataFrame.from_records(ls, exclude=ex)
            .assign(
                name=lambda x: x.name.str.replace(
                    args.to_list_path, ""
                ).str.removeprefix("/")
            )
            .set_index("name")
        )
        print(info[[c for c in cols if c in info.columns]])
        return
    print("\n".join(a.removeprefix(args.to_list_path).removeprefix("/") for a in ls))


def _get(args):
    get(args.to_get_path, args.destination, quiet=False)


def _put(args):
    source_path = Path(args.source_path).absolute()
    if not source_path.exists():
        raise FileNotFoundError(f"{source_path}")

    put(source_path, args.destination, quiet=False)
