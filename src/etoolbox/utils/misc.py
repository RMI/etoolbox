"""Miscellaneous helpers and utilities."""
from io import BytesIO
from pathlib import Path

import requests

from etoolbox.utils.lazy_import import lazy_import


def download(url: str, fname: Path | BytesIO) -> None:
    """Download a file with a progress bar.

    Args:
        url: location of file to download.
        fname: a file or buffer to download the file to.

    Returns: None

    """
    tqdm = lazy_import("tqdm.auto", wait_for_signal=False)
    tqdm_logging = lazy_import("tqdm.contrib.logging", wait_for_signal=False)

    resp = requests.get(url, stream=True)  # noqa: S113
    total = int(resp.headers.get("content-length", 0))
    # Can also replace 'file' with an io.BytesIO object
    with (
        tqdm_logging.logging_redirect_tqdm(),
        open(fname, "wb") as file,
        tqdm.tqdm(
            desc="Downloading " + fname.name,
            total=total,
            unit="iB",
            unit_scale=True,
            unit_divisor=1024,
        ) as bar,
    ):
        for data in resp.iter_content(chunk_size=1024):
            size = file.write(data)
            bar.update(size)
