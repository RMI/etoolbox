import sys
from contextlib import suppress

import pytest
from etoolbox.utils.lazy_import import lazy_import


def test_lazy_import_wrap():
    """Test that lazy_import errors on wrapped."""
    module_name = "yaml"
    with suppress(KeyError):
        del sys.modules[module_name]
    mod = lazy_import(module_name, wait_for_signal=True)
    with pytest.raises(AttributeError):
        _ = mod.__wrapped__


def test_lazy_import_wait():
    """Test that lazy_import waits for the signal."""
    module_name = "yaml"
    with suppress(KeyError):
        del sys.modules[module_name]
    mod = lazy_import(module_name, wait_for_signal=True)
    assert mod.__name__ == module_name
    assert module_name not in sys.modules
    with pytest.raises(ModuleNotFoundError):
        assert mod.dump({"this": "that"}) == "this: that\n"
    mod()
    assert mod.dump({"this": "that"}) == "this: that\n"
    del sys.modules[module_name]


def test_lazy_import_no_wait():
    """Test that lazy_import doesn't wait for the signal."""
    module_name = "yaml"
    with suppress(KeyError):
        del sys.modules[module_name]
    mod = lazy_import(module_name, wait_for_signal=False)
    assert mod.__name__ == module_name
    assert module_name not in sys.modules
    assert mod.dump({"this": "that"}) == "this: that\n"
    # second time uses different path
    assert module_name in sys.modules
    assert mod.safe_dump({"this": "that"}) == "this: that\n"
