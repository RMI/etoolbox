"""etoolbox CLI utility functions."""

import argparse

from etoolbox.utils.cloud import (
    AZURE_CACHE_PATH,
    RMICFEZIL_TOKEN_PATH,
    rmi_cloud_clean,
    rmi_cloud_init,
)
from etoolbox.utils.pudl import (
    CACHE_PATH as PUDL_CACHE_PATH,
)
from etoolbox.utils.pudl import (
    TOKEN_PATH,
    rmi_pudl_clean,
)
from etoolbox.utils.table_map import renamer


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="etoolbox CLI Utilities")
    subparsers = parser.add_subparsers(required=True, title="commands")

    cloud_parser = subparsers.add_parser(
        "cloud", help="RMI cloud utilities", aliases=["azure"]
    )
    cloud_subparsers = cloud_parser.add_subparsers(required=True, title="subcommands")

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
    args.func(args)


if __name__ == "__main__":
    main()
