"""Tools for working with RMI's Azure storage."""

import json
import logging
import os
import shutil
from contextlib import nullcontext
from functools import lru_cache
from pathlib import Path

from fsspec import filesystem
from fsspec.implementations.cached import WholeFileCacheFileSystem
from platformdirs import user_cache_path, user_config_path

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
    print(f"deleting {AZURE_CACHE_PATH}")
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

    Write a parquet, or really anythin to Azure...

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
    with open(AZURE_CACHE_PATH / "cache", "rb") as f:
        cache_data = json.load(f)
    return cache_data.get(cloud_path, {}).get("fn", None)


def _get(args):
    get(args.to_get_path, args.destination, quiet=False)


def _put(args):
    to_put_path = Path(args.to_put_path).absolute()
    if not to_put_path.exists():
        raise FileNotFoundError(f"{to_put_path}")

    put(Path(args.to_put_path), args.destination, quiet=False)


def _list(args):
    from pprint import pprint

    fs = rmi_cloud_fs()
    ls = fs.ls(args.to_list_path, detail=args.detail)
    pprint(ls)


def get(to_get_path: str, destination: Path | str, fs=None, *, quiet=True) -> None:
    """Download a remote file from the cloud.

    Args:
        to_get_path: remote file or folder to download of the form '<container>/...
        destination: local destination for the downloaded files
        fs: filesystem
        quiet: disable logging of adlfs output
    """
    fs = rmi_cloud_fs() if fs is None else fs
    to_get_path = to_get_path.removeprefix("az://").removeprefix("abfs://")
    context = all_logging_disabled if quiet else nullcontext
    with context():
        ls = fs.ls(to_get_path)
        if ls[0]["name"] != to_get_path:
            raise TypeError("`to_get_path` must be a file.")
        fs.get(
            rpath="az://" + to_get_path,
            lpath=str(destination),
            recursive=False,
            callback=ProgressCallback(),
        )


def put(to_put_path: Path, destination: str, fs=None, *, quiet=True) -> None:
    """Upload local files or directories to the cloud.

    Copies a specific file or tree of files. If destination
    ends with a "/", it will be assumed to be a directory, and target files
    will go within.

    Args:
        to_put_path: local file or folder to copy
        destination: copy destination of the form '<container>/...
        fs: filesystem
        quiet: disable logging of adlfs output
    """
    fs = rmi_cloud_fs() if fs is None else fs
    context = all_logging_disabled if quiet else nullcontext
    with context():
        fs.put(
            lpath=str(to_put_path),
            rpath="az://" + destination.removeprefix("az://").removeprefix("abfs://"),
            recursive=to_put_path.is_dir(),
            callback=ProgressCallback(),
        )
