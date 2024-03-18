"""Miscellaneous helpers and utilities."""

from io import DEFAULT_BUFFER_SIZE, BytesIO
from pathlib import Path

import requests

from etoolbox._optional import logging_redirect_tqdm, tqdm


def download(url: str, fname: Path | BytesIO) -> None:
    """Download a file with a progress bar.

    Args:
        url: location of file to download.
        fname: a file or buffer to download the file to.

    Returns: None

    """
    resp = requests.get(url, stream=True)  # noqa: S113
    total = int(resp.headers.get("content-length", 0))
    # Can also replace 'file' with an io.BytesIO object
    with (
        logging_redirect_tqdm(),
        open(fname, "wb") as file,
        tqdm(
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


def ungzip(zip_path: Path, out_path=None):
    """Un gzip a file with a progress bar.

    Args:
        zip_path: path of gzip archive.
        out_path: path of resulting output file, if not provided will be the same as
            zip_path without ``.gz`` suffix.

    Returns: None

    """
    import gzip

    if out_path is None:
        out_path = zip_path.parent / zip_path.stem

    with (
        logging_redirect_tqdm(),
        gzip.open(zip_path, "rb") as zf,
        open(out_path, "wb") as out,
        tqdm(
            desc="Ungzipping " + zip_path.name,
            unit="iB",
            unit_scale=True,
            unit_divisor=1024,
        ) as bar,
    ):
        while True:
            chunk = zf.read(DEFAULT_BUFFER_SIZE)
            if not chunk:
                break
            size = out.write(chunk)
            bar.update(size)
