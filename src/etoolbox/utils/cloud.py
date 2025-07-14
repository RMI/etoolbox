"""Tools for working with RMI's Azure storage."""

import base64
import json
import logging
import os
import shutil
import subprocess
from contextlib import nullcontext
from datetime import datetime
from functools import lru_cache
from pathlib import Path

import click
import pandas as pd
import polars as pl
import tomllib
import yaml
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
ETB_AZURE_TOKEN_PATH = CONFIG_PATH / "etb_azure_token.txt"
ETB_AZURE_ACCOUNT_NAME_PATH = CONFIG_PATH / "etb_azure_account_name.txt"

logger = logging.getLogger("etoolbox")


def cloud_clean(*, dry: bool = False, all_: bool = False):
    """Cleanup cache and config directories."""
    info = cache_info()
    size = info["size"].sum() * 1e-6
    click.echo(
        f"Will delete the following items using {size:,.0f} MB at {AZURE_CACHE_PATH}"
    )
    click.echo(info[["size", "time"]])
    if not dry:
        shutil.rmtree(AZURE_CACHE_PATH, ignore_errors=True)
    if all_:
        click.echo(f"deleting config {CONFIG_PATH}")
        if not dry:
            shutil.rmtree(CONFIG_PATH, ignore_errors=True)


def cloud_setup():
    """Interactive cloud setup."""
    if not ETB_AZURE_ACCOUNT_NAME_PATH.exists():
        account_name = click.prompt("Enter Azure Account Name: ")
    else:
        if (
            click.prompt(
                f"Azure Account Name is currently "
                f"'{ETB_AZURE_ACCOUNT_NAME_PATH.read_text()}', "
                f"would you like to change it? [y/N]",
                default="n",
            ).casefold()
            == "y"
        ):
            account_name = click.prompt("Enter Azure Account Name: ")
        else:
            account_name = ""
    if account_name:
        with open(ETB_AZURE_ACCOUNT_NAME_PATH, "w") as f:
            f.write(account_name.strip())
        click.echo(
            f"Azure account name: {account_name} written to "
            f"{ETB_AZURE_ACCOUNT_NAME_PATH}."
        )

    if not ETB_AZURE_TOKEN_PATH.exists():
        token = click.prompt("Enter Azure Token: ", type=str)
    else:
        if (
            click.prompt(
                "Azure Token exists, would you like to change it? [y/N]", default="n"
            ).casefold()
            == "y"
        ):
            token = click.prompt("Enter Azure Token: ", type=str)
        else:
            token = ""
    if token:
        token = token.strip("'").strip('"').encode("utf-8")
        if token.startswith(b"sv="):
            token = base64.b64encode(token)
        with open(ETB_AZURE_TOKEN_PATH, "wb") as f:
            f.write(token)
        click.echo(f"SAS Token written to {ETB_AZURE_TOKEN_PATH}.")


def cloud_init(
    account_name: str,
    token: bytes | str,
    *,
    dry_run: bool = False,
    clobber: bool = False,
):
    """Write SAS token file to disk."""
    if not account_name and not token:
        return cloud_setup()
    if ETB_AZURE_ACCOUNT_NAME_PATH.exists():
        if not clobber:
            raise FileExistsError("Account name already exists.")
        click.echo(f"delete {ETB_AZURE_ACCOUNT_NAME_PATH}")
        if not dry_run:
            ETB_AZURE_ACCOUNT_NAME_PATH.unlink()
    if ETB_AZURE_TOKEN_PATH.exists() and token:
        if not clobber:
            raise FileExistsError("SAS Token already exists.")
        click.echo(f"delete {ETB_AZURE_TOKEN_PATH}")
        if not dry_run:
            ETB_AZURE_TOKEN_PATH.unlink()
    if dry_run:
        click.echo(f"write SAS Token to {ETB_AZURE_TOKEN_PATH}")
        click.echo(f"write {account_name} to {ETB_AZURE_ACCOUNT_NAME_PATH}")
        return
    if token:
        if isinstance(token, str):
            token = token.strip("'").strip('"').encode("utf-8")
        if token.startswith(b"sv="):
            token = base64.b64encode(token)
        with open(ETB_AZURE_TOKEN_PATH, "wb") as f:
            f.write(token)
        click.echo(f"SAS Token written to {ETB_AZURE_TOKEN_PATH}.")
    with open(ETB_AZURE_ACCOUNT_NAME_PATH, "w") as f:
        f.write(account_name)
    click.echo(
        f"Azure account name: {account_name} written to {ETB_AZURE_ACCOUNT_NAME_PATH}."
    )


@lru_cache
def read_token() -> str:
    """Read SAS token from disk or environment variable."""
    if ETB_AZURE_TOKEN_PATH.exists():
        return base64.b64decode(ETB_AZURE_TOKEN_PATH.read_text()).decode("utf-8")
    if (token := os.environ.get("ETB_AZURE_SAS_TOKEN")) is not None:
        return token
    if (old_path := CONFIG_PATH / "rmicfezil_token.txt").exists():
        with open(old_path) as f:
            token = f.read()
        with open(ETB_AZURE_TOKEN_PATH, "wb") as f:
            f.write(base64.b64encode(token.encode("utf-8")))
        old_path.unlink()
        return read_token()
    raise ValueError(
        "No SAS Token found, either run `etb cloud init` or set "
        "ETB_AZURE_SAS_TOKEN environment variable."
    )


@lru_cache
def read_account_name() -> str:
    """Read SAS token from disk or environment variable."""
    if ETB_AZURE_ACCOUNT_NAME_PATH.exists():
        return ETB_AZURE_ACCOUNT_NAME_PATH.read_text()
    elif (token := os.environ.get("ETB_AZURE_ACCOUNT_NAME")) is not None:
        return token
    raise ValueError(
        "No Azure account name found, either re-run `etb cloud init` "
        "or set ETB_AZURE_ACCOUNT_NAME environment variable."
    )


def storage_options():
    """Simplify reading from Azure using :mod:`polars`.

    When using :mod:`pandas` or writing to Azure, see :func:`.rmi_cloud_fs`.

    Examples
    --------
    >>> import polars as pl
    >>> from etoolbox.utils.cloud import storage_options

    >>> df = pl.read_parquet("az://patio-data/test_data.parquet", **storage_options())
    >>> df.head()  # doctest: +NORMALIZE_WHITESPACE
    shape: (5, 2)
    ┌────────────────────┬──────────────────┐
    │ energy_source_code ┆ co2_mt_per_mmbtu │
    │ ---                ┆ ---              │
    │ str                ┆ f64              │
    ╞════════════════════╪══════════════════╡
    │ AB                 ┆ 1.1817e-7        │
    │ ANT                ┆ 1.0369e-7        │
    │ BFG                ┆ 2.7432e-7        │
    │ BIT                ┆ 9.3280e-8        │
    │ BLQ                ┆ 9.4480e-8        │
    └────────────────────┴──────────────────┘

    """
    return {
        "storage_options": {
            "account_name": read_account_name(),
            "sas_token": read_token(),
        }
    }


def rmi_cloud_fs(account_name=None, token=None) -> WholeFileCacheFileSystem:
    """Work with files on Azure.

    This can be used to read or write arbitrary files to or from Azure. And for files
    read from Azure, it will create and manage a local cache.

    Examples
    --------
    >>> import pandas as pd
    >>> from etoolbox.utils.cloud import rmi_cloud_fs

    >>> fs = rmi_cloud_fs()
    >>> df = pd.read_parquet("az://patio-data/test_data.parquet", filesystem=fs)
    >>> df.head()  # doctest: +NORMALIZE_WHITESPACE
      energy_source_code  co2_mt_per_mmbtu
    0                 AB      1.181700e-07
    1                ANT      1.036900e-07
    2                BFG      2.743200e-07
    3                BIT      9.328000e-08
    4                BLQ      9.448000e-08

    Read with :mod:`polars` using the same filecache as with :mod:`pandas`.

    >>> import polars as pl

    >>> with fs.open("az://patio-data/test_data.parquet") as f:
    ...     df = pl.read_parquet(f)
    >>> df.head()  # doctest: +NORMALIZE_WHITESPACE
    shape: (5, 2)
    ┌────────────────────┬──────────────────┐
    │ energy_source_code ┆ co2_mt_per_mmbtu │
    │ ---                ┆ ---              │
    │ str                ┆ f64              │
    ╞════════════════════╪══════════════════╡
    │ AB                 ┆ 1.1817e-7        │
    │ ANT                ┆ 1.0369e-7        │
    │ BFG                ┆ 2.7432e-7        │
    │ BIT                ┆ 9.3280e-8        │
    │ BLQ                ┆ 9.4480e-8        │
    └────────────────────┴──────────────────┘

    Write a parquet file, or really anything to Azure...

    >>> with fs.open("az://patio-data/file.parquet", mode="wb") as f:  # doctest: +SKIP
    ...     df.write_parquet(f)

    """
    return filesystem(
        "filecache",
        target_protocol="az",
        target_options={
            "account_name": read_account_name()
            if account_name is None
            else account_name,
            "sas_token": read_token() if token is None else token,
        },
        cache_storage=str(AZURE_CACHE_PATH),
        check_files=True,
        cache_timeout=None,
    )


def cache_info():
    """Return info about cloud cache contents."""
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


def cached_path(cloud_path: str, *, download=False) -> str | None:
    """Get the local cache path of a cloud file.

    Args:
        cloud_path: path on azure, e.g. ``az://raw-data/test_data.parquet``
        download: download the file from Azure to create a local cache if it
            does not exist.

    Examples
    --------
    >>> import polars as pl
    >>> from etoolbox.utils.cloud import rmi_cloud_fs, cached_path

    >>> fs = rmi_cloud_fs()
    >>> cloud_path = "az://patio-data/test_data.parquet"
    >>> with fs.open(cloud_path) as f:
    ...     df = pl.read_parquet(f)
    >>> cached_path(cloud_path)
    '656706c40cb490423b652aa6d3b4903c56ab6c798ac4eb2fa3ccbab39ceebc4a'

    """
    cloud_path = cloud_path.removeprefix("az://").removeprefix("abfs://")
    if download:
        f = rmi_cloud_fs().open(cloud_path)
        f.close()
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
        to_get_path: remote file or folder to download of the form ``<container>/...``
        destination: local destination for the downloaded files
        fs: filesystem
        quiet: disable logging of adlfs output
        clobber: overwrite existing files and directories if True
        azcopy_path: path to azcopy executable
    """
    to_get_path = (
        to_get_path.removeprefix("az://")
        .removeprefix("abfs://")
        .removeprefix(f"https://{read_account_name()}.blob.core.windows.net/")
    )
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
                f"https://{read_account_name()}.blob.core.windows.net/{to_get_path}?{read_token()}",
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
        destination: copy destination of the form ``<container>/...``
        fs: filesystem
        quiet: disable logging of adlfs output
        clobber: force overwriting of existing files (only works when azcopy is used)
        azcopy_path: path to azcopy executable
    """
    if not to_put_path.exists():
        raise FileNotFoundError(to_put_path)
    lpath = str(to_put_path)
    destination = (
        destination.removeprefix("az://")
        .removeprefix("abfs://")
        .removeprefix(f"https://{read_account_name()}.blob.core.windows.net/")
    )
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
                rpath="az://" + destination,
                recursive=recursive,
                callback=ProgressCallback(),
            )
    else:
        subprocess.run(  # noqa: S603
            [
                azcopy_path,
                "cp",
                lpath,
                f"https://{read_account_name()}.blob.core.windows.net/{destination}?{read_token()}",
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
    out = read_cloud_file(f"patio-results/{datestr}/BAs_{datestr}_results.zip")
    for k, v in out.items():
        if isinstance(v, pd.DataFrame):
            out[k] = v.convert_dtypes(
                convert_boolean=True,
                convert_string=False,
                convert_floating=False,
                convert_integer=False,
            )
        elif isinstance(v, pl.DataFrame):
            out[k] = v.to_pandas()
    return out


def read_cloud_file(filename: str) -> dict[str, pd.DataFrame] | pd.DataFrame:
    """Read parquet, csv, or DataZip files from Azure.

    The method handles the specific format of patio resource
    files and manages file system interactions as well as cache mechanisms.

    Args:
        filename: the full path to the file including container and file extension.

    Examples
    --------
    >>> from etoolbox.utils.cloud import read_cloud_file

    >>> df = read_cloud_file("patio-data/20241031/utility_ids.parquet")
    >>> df.head()  # doctest: +NORMALIZE_WHITESPACE
       utility_id_ferc1  ...  public_private_unmapped
    0               1.0  ...                 unmapped
    1             342.0  ...                   public
    2             294.0  ...                   public
    3             394.0  ...                   public
    4             349.0  ...                   public
    <BLANKLINE>
    [5 rows x 37 columns]

    """
    fs = rmi_cloud_fs()
    filename = "az://" + filename.removeprefix("az://").removeprefix("abfs://")

    if ".parquet" in filename:
        try:
            return pd.read_parquet(filename, filesystem=fs)
        except Exception:
            with fs.open(filename) as fp:
                return pl.read_parquet(fp).to_pandas()
    if ".csv" in filename:
        with fs.open(filename, "rb") as fp:
            return pd.read_csv(fp)
    if ".json" in filename:
        with fs.open(filename, "rb") as fp:
            return json.load(fp)
    if ".toml" in filename:
        with fs.open(filename, "rb") as fp:
            return tomllib.load(fp)
    if ".txt" in filename:
        with fs.open(filename, "r") as fp:
            return fp.read()
    if ".yaml" in filename or ".yml" in filename:
        with fs.open(filename, "rb") as fp:
            return yaml.safe_load(fp)
    if ".zip" in filename:
        f = fs.open(filename)
        f.close()
        with DataZip(str(AZURE_CACHE_PATH / cached_path(filename)), "r") as z:
            return dict(z.items())
    raise ValueError(
        f"{filename} is not a parquet, csv, json, toml, txt, yaml/yml, or zip."
    )


def write_cloud_file(data: pd.DataFrame | str | bytes, filename: str) -> None:
    """Writes economic results for patio data to a specified filename in Azure storage.

    Args:
        data: DataFrame, or str or bytes representing
        filename: Target filename for storing the results, it must include the
            container, full path, and appropriate file extension, i.e., parquet for
            a DataFrame; csv json yaml yml toml or txt for str/bytes.

    """
    name, _, suffix = (
        filename.removeprefix("az://").removeprefix("abfs://").partition(".")
    )
    fs = rmi_cloud_fs()
    if isinstance(data, pd.DataFrame):
        if suffix != "parquet":
            raise TypeError("to write a DataFrame as csv, pass it as a str or bytes")
        with fs.open(f"az://{name}.parquet", mode="wb") as f:
            data.to_parquet(f)
    elif isinstance(data, str | bytes):
        allowed_file_types = ("csv", "json", "yaml", "yml", "toml", "txt")
        if suffix.lower() not in allowed_file_types:
            raise AssertionError(
                f"Unsupported file format {suffix}, must be one of {allowed_file_types}"
            )
        with fs.open(f"az://{name}.{suffix}", mode="wb") as f:
            f.write(data.encode("utf-8") if isinstance(data, str) else data)
    else:
        raise RuntimeError(f"Unsupported type {type(data)}")
