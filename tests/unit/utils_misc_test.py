"""Test utils.misc."""
from etoolbox.utils.misc import download, ungzip


def test_download(temp_dir):
    """Test download."""
    dl_path = temp_dir / "dltest.zip"
    download("https://github.com/RMI/etoolbox/raw/0.2.0/tests/pudltabl.zip", dl_path)
    assert dl_path.exists()


def test_ungzip(gzip_test_data, temp_dir):
    """Test ungzip."""
    ugz_path = temp_dir / "ungztest.txt"
    gz_path, content = gzip_test_data
    ungzip(gz_path, ugz_path)
    with open(ugz_path, "rb") as f:
        assert f.read() == content
