"""etoolbox CLI utility functions."""

from pathlib import Path

import click
import numpy as np
import pandas as pd

from etoolbox import __version__
from etoolbox.utils.cloud import (
    AZURE_CACHE_PATH,
    cache_info,
    cloud_clean,
    cloud_init,
    cloud_list,
    get,
    put,
)
from etoolbox.utils.pudl import CACHE_PATH as PUDL_CACHE_PATH
from etoolbox.utils.pudl import pl_scan_pudl, pudl_cache, pudl_list, rmi_pudl_clean
from etoolbox.utils.table_map import renamer


@click.group(help="eToolBox CLI.")
@click.version_option(__version__)
def main():
    """eToolBox CLI."""  # noqa: D403
    pass


@main.group(help="RMI cloud utilities.")
def cloud():
    """RMI cloud utilities."""
    pass


@cloud.command(name="get", help="Download a remote file from the cloud.")
@click.argument("source", type=str)
@click.argument(
    "destination", type=click.Path(path_type=Path), default=Path.cwd(), required=False
)
@click.option(
    "-c",
    "--clobber",
    is_flag=True,
    default=False,
    show_default=True,
    help="Overwrite existing files",
)
def etb_cloud_get(source, destination, clobber):
    """Download a remote file from the cloud.

    Uses ``azcopy`` CLI if available.

    SOURCE: remote file or folder to download of the form ``<container>/...``
    DESTINATION: local destination for the downloaded files
    """
    get(source, destination, quiet=False, clobber=clobber)


@cloud.command(name="put", help="Upload local files or directories to the cloud.")
@click.argument(
    "source", type=click.Path(exists=True, resolve_path=True, path_type=Path)
)
@click.argument("destination", type=str)
@click.option(
    "-c",
    "--clobber",
    is_flag=True,
    default=False,
    show_default=True,
    help="Overwrite existing files",
)
def etb_cloud_put(source: Path, destination: str, *, clobber: bool):
    """Upload local files or directories to the cloud.

    Copies a specific file or tree of files. If destination
    ends with a "/", it will be assumed to be a directory, and target files
    will go within.

    Uses ``azcopy`` CLI if available.

    SOURCE: local file or folder to copy
    DESTINATION: copy destination of the form ``<container>/...``
    """
    # source = source.absolute()
    if not source.exists():
        raise FileNotFoundError(f"{source}")

    put(source, destination, quiet=False, clobber=clobber)


@cloud.command(name="list", help="List all files in Azure container.")
@click.argument("directory", type=str)
@click.option(
    "-l",
    "--detail",
    default=False,
    is_flag=True,
    show_default=True,
    help="Show file details.",
)
def etb_cloud_list(directory: str, *, detail: bool):
    """List cloud files in a folder.

    DIRECTORY: remote folder to list contents of e.g. ``<container>/...``
    """
    ls = cloud_list(directory, detail=detail)
    if detail:
        cols = ["size", "creation_time", "last_modified", "type", "etag", "tags"]
        ex = ["content_settings"] if any("content_settings" in d for d in ls) else None
        info = (
            pd.DataFrame.from_records(ls, exclude=ex)
            .assign(
                name=lambda x: x.name.str.replace(directory, "").str.removeprefix("/")
            )
            .set_index("name")
        )
        click.echo(info[[c for c in cols if c in info.columns]])
        return
    click.echo("\n".join(a.removeprefix(directory).removeprefix("/") for a in ls))


@cloud.command(name="cache", help="Return info about cloud cache contents.")
def etb_cloud_cache():
    """Return info about cloud cache contents."""
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
    click.echo(info)
    click.echo(f"\nTotal size: {info['size'].sum():,.0f} MB")


@cloud.command(
    "clean",
    help=f"Delete local cache of Azure files in "
    f"{AZURE_CACHE_PATH.relative_to(Path.home())}.",
)
@click.option("-d", "--dry-run", default=False, is_flag=True)
@click.option(
    "-a",
    "--all",
    "all_",
    default=False,
    is_flag=True,
    show_default=True,
    help="Delete config as well as file cache.",
)
def etb_cloud_clean(*, dry_run: bool, all_: bool):
    """Cleanup cache and config directories."""
    cloud_clean(dry=dry_run, all_=all_)


@cloud.command(
    "init",
    help="Store SAS token and account name for reading from and writing to Azure.",
)
@click.argument("account_name", type=str, required=False, default="")
@click.argument("token", type=str, required=False, default="")
@click.option(
    "-d",
    "--dry-run",
    default=False,
    is_flag=True,
    show_default=True,
)
@click.option(
    "-c",
    "--clobber",
    is_flag=True,
    default=False,
    show_default=True,
    help="Overwrite existing files.",
)
def etb_cloud_init(account_name, token, *, dry_run: bool, clobber: bool):
    """Write SAS token file to disk."""
    cloud_init(account_name, token, dry_run=dry_run, clobber=clobber)


@main.group(help="PUDL utilities.")
def pudl():
    """PUDL utilities."""
    pass


@pudl.command(name="get", help="Download PUDL table from AWS as a csv.")
@click.argument("table", type=str)
@click.argument("release", type=str)
@click.argument(
    "destination", type=click.Path(path_type=Path), default=Path.cwd(), required=False
)
def etb_pudl_get(table, release, destination):
    """Download PUDL table from AWS as a csv.

    Downloads a specified PUDL table from AWS and saves it as a CSV file. This command
    retrieves the selected table for a given release version and writes it to a given
    destination directory. If no destination is provided, it defaults to the current
    working directory.

    TABLE: Name of the PUDL table to download.
    RELEASE: Release version of the PUDL data to pull the table from.
    DESTINATION: Path to the directory where the CSV file will be saved.
        Defaults to the current working directory.
    """
    pl_scan_pudl(table, release=release, use_polars=True).collect().write_csv(
        f"{destination}/{table}.csv"
    )


@pudl.command(
    name="cache",
    help=f"List files stored in ``rmi.pudl`` local cache at "
    f"``{PUDL_CACHE_PATH.relative_to(Path.home())}``.",
)
def etb_pudl_cache():
    """Return info about the contents of the PUDL cache."""
    info = pudl_cache().assign(
        time=lambda x: x["time"].dt.strftime("%Y-%m-%d %H:%M:%S"),
        fn=lambda x: x["fn"].str.slice(0, 5) + "...",
        uid=lambda x: x["uid"].str.slice(0, 5) + "...",
        size=lambda x: np.round(x["size"] * 1e-6, 1),
    )
    click.echo(info)
    click.echo(f"\nTotal size: {info['size'].sum():,.0f} MB")


@pudl.command(
    name="clean",
    help=f"Remove ``rmi.pudl`` local cache at "
    f"``{PUDL_CACHE_PATH.relative_to(Path.home())}``.",
)
@click.option("-l", "--legacy", is_flag=True, default=False, show_default=True)
@click.option("-a", "--all", "all_", is_flag=True, default=False, show_default=True)
@click.option("-d", "--dry-run", is_flag=True, default=False, show_default=True)
def etb_pudl_clean(legacy, all_, dry_run):
    """Remove rmi.pudl local cache."""
    rmi_pudl_clean(legacy=legacy, all_=all_, dry=dry_run)


@pudl.command(name="list", help="List files in PUDL release or list releases.")
@click.argument("release", type=str, default=None, required=False)
@click.option(
    "-l",
    "--detail",
    is_flag=True,
    default=False,
    show_default=True,
    help="If True, return details of each table, otherwise just names.",
)
def etb_pudl_list(release, detail):
    """List PUDL tables in AWS using ``ls`` command.

    RELEASE: ``nightly``, ``stable`` or versioned, omit to list releases.
    """
    ls = pudl_list(release, detail=detail)
    pre = release if release else ""
    if detail:
        all_cols = {k for d in ls for k in d}
        cols = [co for co in ("name", "type", "size", "LastModified") if co in all_cols]
        ex = list({"Key", "Size", "StorageClass"}.intersection(all_cols))
        info = (
            pd.DataFrame.from_records(ls, exclude=ex)
            .assign(
                name=lambda x: x.name.str.replace(
                    "pudl.catalyst.coop/", ""
                ).str.removeprefix(f"{pre}/"),
                size=lambda x: np.round(x["size"] * 1e-6, 1),
            )
            .set_index("name")
        )
        click.echo(
            info[[c for c in cols if c in info.columns]]
            .to_string()
            .replace("\n", "\n\t")
        )
        return
    click.echo(
        "\n".join(
            a.removeprefix("pudl.catalyst.coop/").removeprefix(f"{pre}/") for a in ls
        )
    )


@pudl.command(
    name="rename", help="Rename PUDL tables in files that match a provided pattern."
)
@click.argument("pattern", type=str)
@click.option("-d", "--dry-run", is_flag=True, default=False, show_default=True)
@click.option(
    "-y",
    "--yes",
    is_flag=True,
    default=False,
    show_default=True,
    help="Sets any confirmation values to 'yes' automatically. users will not be "
    "asked to confirm before tables are renamed.",
)
def etb_pudl_rename(pattern, dry_run, yes):
    """Rename PUDL tables in files that match a provided pattern.

    PATTERN: pattern for globbing files.
    """
    renamer(pattern=pattern, dry=dry_run, yes=yes)
