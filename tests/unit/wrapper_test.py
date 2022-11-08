"""Tests for :class:`.IOWrapper`."""
from etoolbox.datazip import IOWrapper
from etoolbox.datazip.test_classes import MockPudlTabl


def test_wrapper(test_dir):
    """Test wrapping a mocked PudlTabl."""
    r = {
        ("etoolbox.datazip.test_classes", "KlassWOSlots", None): {
            "keep": False,
            "constructor": (
                "etoolbox.datazip.test_classes",
                "KlassWOSlots",
                None,
            ),
        }
    }
    a = IOWrapper(MockPudlTabl(), recipes=r)
    df = a.utils_eia860()
    try:
        a.to_file(test_dir / "wrapped")
        b = IOWrapper.from_file(test_dir / "wrapped")
        assert df.compare(b.utils_eia860()).empty
        assert not b.bga_eia860().empty
    finally:
        (test_dir / "wrapped.zip").unlink(missing_ok=True)
