"""Tools for working with RMI's Azure storage."""

import logging
import os
from argparse import ArgumentParser
from pathlib import Path

from cachetools.func import lru_cache
from fsspec import filesystem
from fsspec.implementations.cached import WholeFileCacheFileSystem
from platformdirs import user_cache_path, user_config_path

from etoolbox.utils.misc import all_logging_disabled

CONFIG_PATH = user_config_path("rmi-cloud", ensure_exists=True)
AZURE_CACHE_PATH = user_cache_path("rmi.cloud", ensure_exists=True)
RMICFEZIL_TOKEN_PATH = CONFIG_PATH / "rmicfezil_token.txt"

logger = logging.getLogger("etoolbox")


def rmi_cloud_init():
    """Write SAS token file to disk."""
    parser = ArgumentParser(
        description="Store SAS token for reading from and writing to Azure."
    )
    parser.add_argument(
        "token",
        type=str,
        help="SAS Token.",
    )
    parser.add_argument(
        "-c,--clobber",
        action="store_true",
        default=False,
        required=False,
        help=f"Overwrite existing SAS token in {RMICFEZIL_TOKEN_PATH}.",
        dest="clobber",
    )
    args = parser.parse_args()
    if RMICFEZIL_TOKEN_PATH.exists() and not args.clobber:
        raise FileExistsError("SAS Token already exists.")
    with open(RMICFEZIL_TOKEN_PATH, "w") as f:
        f.write(args.token)
    logger.info("SAS Token written to %s.", RMICFEZIL_TOKEN_PATH)


@lru_cache
def read_token() -> str:
    """Read SAS token from disk or environment variable."""
    if RMICFEZIL_TOKEN_PATH.exists():
        with open(RMICFEZIL_TOKEN_PATH) as f:
            return f.read()
    elif (token := os.environ.get("RMICFEZIL_SAS_TOKEN")) is not None:
        return token
    raise RuntimeError(
        "No SAS Token found, either run rmi-cloud-init or set "
        "RMICFEZIL_SAS_TOKEN environment variable."
    )


def storage_options():
    """Simplify reading from/writing to Azure using :mod:`pandas` or :mod:`polars`.

    Examples
    --------
    >>> import polars as pl
    >>> df = pl.read_parquet("az://raw-data/test_data.parquet", **storage_options())
    >>> df.shape
    (46, 13)

    >>> import pandas as pd
    >>> df = pd.read_parquet("az://raw-data/test_data.parquet", **storage_options())
    >>> df.shape
    (46, 12)

    """
    return {"storage_options": {"account_name": "rmicfezil", "sas_token": read_token()}}


def rmi_cloud_fs(token=None) -> WholeFileCacheFileSystem:
    """Create a fsspec/AWS filesystem with a filecache."""
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


def get(to_get_path: str, destination: Path | str, fs=None) -> None:
    """Download a remote file from the cloud.

    Args:
        to_get_path: remote file or folder to download of the form '<container>/...
        destination: local destination for the downloaded files
        fs: filesystem
    """
    fs = rmi_cloud_fs() if fs is None else fs
    to_get_path = to_get_path.removeprefix("az://").removeprefix("abfs://")
    with all_logging_disabled():
        ls = fs.ls(to_get_path)
        if ls[0]["name"] != to_get_path:
            raise TypeError("`to_get_path` must be a file.")
        fs.get(
            rpath="az://" + to_get_path,
            lpath=str(destination),
            recursive=False,
        )


def put(to_put_path: Path, destination: str, fs=None) -> None:
    """Upload local files or directories to the cloud.

    Copies a specific file or tree of files. If destination
    ends with a "/", it will be assumed to be a directory, and target files
    will go within.

    Args:
        to_put_path: local file or folder to copy
        destination: copy destination of the form '<container>/...
        fs: filesystem
    """
    fs = rmi_cloud_fs() if fs is None else fs
    with all_logging_disabled():
        fs.put(
            lpath=str(to_put_path),
            rpath="az://" + destination.removeprefix("az://").removeprefix("abfs://"),
            recursive=to_put_path.is_dir(),
        )
