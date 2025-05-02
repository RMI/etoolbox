"""etoolbox CLI utility functions."""

import argparse
import os
import sys

from etoolbox import __version__
from etoolbox.utils.cloud import (
    AZURE_CACHE_PATH,
    RMICFEZIL_TOKEN_PATH,
    _cache_info,
    _get,
    _list,
    _put,
    rmi_cloud_clean,
    rmi_cloud_init,
)
from etoolbox.utils.pudl import (
    CACHE_PATH as PUDL_CACHE_PATH,
)
from etoolbox.utils.pudl import TOKEN_PATH, _pudl_cache, rmi_pudl_clean
from etoolbox.utils.pudl import _list as _pudl_list
from etoolbox.utils.table_map import renamer


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="etoolbox CLI Utilities")
    parser.add_argument(
        "-v,--version",
        action="store_true",
        default=False,
        required=False,
        help="show version",
        dest="version",
    )
    subparsers = parser.add_subparsers(required=False, title="commands")

    cloud_parser = subparsers.add_parser(
        "cloud", help="RMI cloud utilities", aliases=["azure"]
    )
    cloud_subparsers = cloud_parser.add_subparsers(required=True, title="subcommands")

    cloud_list_sp = cloud_subparsers.add_parser(
        "list",
        help="list files in Azure container or directory",
        description="List files in Azure container or directory.",
    )
    cloud_list_sp.add_argument(
        "to_list_path",
        type=str,
        help="list files in directory of the form '<container>/...'",
    )
    cloud_list_sp.add_argument(
        "-l,--detail",
        default=False,
        action="store_true",
        help="include detail",
        dest="detail",
    )
    cloud_list_sp.set_defaults(func=_list)

    cloud_get_sp = cloud_subparsers.add_parser(
        "get",
        help="download files from Azure",
        description="Download files from Azure.",
    )
    cloud_get_sp.add_argument(
        "to_get_path",
        type=str,
        help="remote file to download of the form '<container>/...'",
    )
    cloud_get_sp.add_argument(
        "-D,--destination",
        type=str,
        default=os.getcwd(),
        required=False,
        help="local destination for the downloaded files [defaults to cwd].",
        dest="destination",
    )
    cloud_get_sp.set_defaults(func=_get)

    cloud_put_sp = cloud_subparsers.add_parser(
        "put",
        help="upload files to Azure",
        description="Upload files to Azure.",
    )
    cloud_put_sp.add_argument(
        "source_path",
        type=str,
        help="local file or folder to copy",
    )
    cloud_put_sp.add_argument(
        "destination",
        type=str,
        help="copy destination of the form '<container>/...'",
    )
    cloud_put_sp.set_defaults(func=_put)

    cloud_cache_sp = cloud_subparsers.add_parser(
        "cache",
        help="show info about the local cache of cloud files",
        description="Show info about the local cache of cloud files.",
    )
    cloud_cache_sp.set_defaults(func=_cache_info)

    cloud_clean_sp = cloud_subparsers.add_parser(
        "clean",
        help=f"delete local cache in {AZURE_CACHE_PATH}",
        description="Delete local cache of Azure files.",
    )
    cloud_clean_sp.add_argument(
        "-d,--dry-run",
        action="store_true",
        default=False,
        required=False,
        help="print what would be done, but don't do anything.",
        dest="dry",
    )
    cloud_clean_sp.add_argument(
        "-a,--all",
        action="store_true",
        default=False,
        required=False,
        help="delete config including token along with cache.",
        dest="all",
    )
    cloud_clean_sp.set_defaults(func=rmi_cloud_clean)

    cloud_init_sp = cloud_subparsers.add_parser(
        "init",
        help="setup cloud access",
        description="Store SAS token for reading from and writing to Azure.",
    )
    cloud_init_sp.add_argument("token", type=str, help="SAS Token.")
    cloud_init_sp.add_argument(
        "-c,--clobber",
        action="store_true",
        default=False,
        required=False,
        help=f"overwrite existing SAS token in {RMICFEZIL_TOKEN_PATH}.",
        dest="clobber",
    )
    cloud_init_sp.add_argument(
        "-d,--dry-run",
        action="store_true",
        default=False,
        required=False,
        help="print what would be done, but don't do anything.",
        dest="dry",
    )
    cloud_init_sp.set_defaults(func=rmi_cloud_init)

    pudl_parser = subparsers.add_parser(
        "pudl",
        help="PUDL utilities",
    )
    pudl_subparsers = pudl_parser.add_subparsers(required=True, title="subcommands")

    pudl_cache_sp = pudl_subparsers.add_parser(
        "cache",
        help=f"delete local cache in {PUDL_CACHE_PATH}",
        description=f"Remove rmi.pudl local cache at {PUDL_CACHE_PATH}.",
    )
    pudl_cache_sp.set_defaults(func=_pudl_cache)

    pudl_clean_sp = pudl_subparsers.add_parser(
        "clean",
        help=f"delete local cache in {PUDL_CACHE_PATH}",
        description=f"Remove rmi.pudl local cache at {PUDL_CACHE_PATH}.",
    )
    pudl_clean_sp.add_argument(
        "-l, --legacy",
        action="store_true",
        help=f"remove legacy token at {TOKEN_PATH.parent} and legacy caches in "
        f"{PUDL_CACHE_PATH.parent} without affecting current AWS caches.",
        default=False,
        dest="legacy",
    )
    pudl_clean_sp.add_argument(
        "-a,--all",
        action="store_true",
        default=False,
        required=False,
        help="delete current AWS caches, legacy caches, and token.",
        dest="all",
    )
    pudl_clean_sp.add_argument(
        "-d,--dry-run",
        action="store_true",
        default=False,
        required=False,
        help="print what would be done, but don't do anything.",
        dest="dry",
    )
    pudl_clean_sp.set_defaults(func=rmi_pudl_clean)

    pudl_list_sp = pudl_subparsers.add_parser(
        "list",
        help="list files in pudl container or directory",
        description="List files in Azure container or directory.",
    )
    pudl_list_sp.add_argument(
        "-r,--release",
        type=str,
        required=False,
        default=None,
        dest="release",
        help="list files in release, or leave blank to see releases",
    )
    pudl_list_sp.add_argument(
        "-l,--detail",
        default=False,
        action="store_true",
        help="include detail",
        dest="detail",
    )
    pudl_list_sp.set_defaults(func=_pudl_list)

    pudl_rename_sp = pudl_subparsers.add_parser(
        "rename",
        help="rename PUDL tables in specified files",
        description="Rename PUDL tables in files that match a provided pattern.",
    )
    pudl_rename_sp.add_argument(
        type=str,
        help="pattern for globbing files.",
        dest="pattern",
    )
    pudl_rename_sp.add_argument(
        "-d,--dry-run",
        action="store_true",
        default=False,
        required=False,
        help="print what would be done, but don't do anything.",
        dest="dry",
    )
    pudl_rename_sp.add_argument(
        "-y, --yes",
        action="store_true",
        help="sets any confirmation values to 'yes' automatically. users will not be "
        " asked to confirm before tables are renamed.",
        dest="yes",
    )
    pudl_rename_sp.set_defaults(func=renamer)

    args = parser.parse_args()
    if args.version:
        print(__version__)
        sys.exit(0)
    try:
        args.func(args)
    except Exception:
        parser.print_help()


if __name__ == "__main__":
    main()
