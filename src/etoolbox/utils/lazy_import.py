"""Lazy load a module.

This lazy_import module is lightly adapted from the :mod:`polars.dependencies` module
of the `polars <https://github.com/pola-rs/polars/>`_ package written by Ritchie Vink,
Alexander Beedie, and Chitral Verma. From the original:

MIT License

Copyright (c) 2020 Ritchie Vink

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

import sys
from importlib import import_module
from types import ModuleType
from typing import Any


class _LazyModule(ModuleType):
    """Module that can act both as a lazy-loader and as a proxy."""

    __lazy__ = True

    _mod_pfx: dict[str, str] = {
        "numpy": "np.",
        "pandas": "pd.",
        "pyarrow": "pa.",
        "sqlalchemy": "sa.",
    }

    def __init__(
        self,
        module_name: str,
        *,
        wait_for_signal: bool = False,
    ) -> None:
        """Initialise lazy-loading proxy module.

        Args:
            module_name : the name of the module to lazy-load (if available).
            wait_for_signal: wait for an explicit signal to load the module. To
                enable loading, call the module.

        """
        self._module_available = False
        self._module_name = module_name
        self._wait = wait_for_signal
        self._globals = globals()
        super().__init__(module_name)

    def _import(self, attr) -> ModuleType:
        # import the referenced module, replacing the proxy in this module's globals
        try:
            module = import_module(self.__name__)
        except ModuleNotFoundError as exc:
            pfx = self._mod_pfx.get(self._module_name, "")
            raise ModuleNotFoundError(
                f"{pfx}{attr} requires '{self._module_name}' module to be installed"
            ) from exc
        else:
            self._module_available = True
            self._globals[self._module_name] = module
            self.__dict__.update(module.__dict__)
            return module

    def __call__(self, *args, **kwargs):
        self._wait = False

    def __getattr__(self, attr: Any) -> Any:
        # have "hasattr('__wrapped__')" return False without triggering import
        # (it's for decorators, not modules, but keeps "make doctest" happy)
        if attr == "__wrapped__":
            raise AttributeError(
                f"{self._module_name!r} object has no attribute {attr!r}"
            )
        # we don't have to load the modue to know the name
        if attr == "__name__":
            return self._module_name

        if self._module_available:
            return getattr(self._globals[self._module_name], attr)

        # accessing the proxy module's attributes triggers import of the real thing
        # unless we need to wait for the signal
        if not self._wait:
            # import the module and return the requested attribute
            module = self._import(attr)
            return getattr(module, attr)
        else:
            raise ModuleNotFoundError(
                f"Waiting to load '{self._module_name}' try running `stop_waiting` to "
                f"allow loading or set `wait_for_signal` to False."
            )


def lazy_import(module_name: str, *, wait_for_signal: bool) -> ModuleType:
    """Lazy import the given module; avoids up-front import costs.

    Args:
        module_name : name of the module to import, eg: "pyarrow".
        wait_for_signal: wait for an explicit signal to load the module.
            To enable loading, call the returned module.

    Returns: a lazy-loading module

    Notes
    -----
    If the requested module is not available (eg: has not been installed), a proxy
    module is created in its place, which raises an exception on any attribute
    access. This allows for import and use as normal, without requiring explicit
    guard conditions - if the module is never used, no exception occurs; if it
    is, then a helpful exception is raised.
    """
    # check if module is LOADED
    if module_name in sys.modules and not wait_for_signal:
        return sys.modules[module_name]

    # create lazy/proxy module that imports the real one on first use
    # (or raises an explanatory ModuleNotFoundError if not available)
    return _LazyModule(
        module_name=module_name,
        wait_for_signal=wait_for_signal,
    )
