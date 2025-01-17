"""Miscellaneous helpers and utilities."""

import http.client as httplib
import logging
from contextlib import contextmanager
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


def have_internet(host: str = "8.8.8.8") -> bool:
    """Check if internet is available.

    Args:
        host: address to use in check, default 8.8.8.8 (Google DNS)

    """
    conn = httplib.HTTPSConnection(host, timeout=1)
    try:
        conn.request("HEAD", "/")
        return True
    except Exception:
        return False
    finally:
        conn.close()


@contextmanager
def all_logging_disabled(highest_level=logging.CRITICAL):
    """Context manager to disable logging.

    A context manager that will prevent any logging messages
    triggered during the body from being processed.

    Args:
        highest_level: the maximum logging level in use.

    This would only need to be changed if a custom level greater than CRITICAL
    is defined.

    two kind-of hacks here:
        * can't get the highest logging level in effect => delegate to the user
        * can't get the current module-level override => use an undocumented
          (but non-private!) interface
    """
    previous_level = logging.root.manager.disable

    logging.disable(highest_level)

    try:
        yield
    finally:
        logging.disable(previous_level)
