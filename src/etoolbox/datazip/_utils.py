from __future__ import annotations

import getpass
import logging
from importlib import import_module
from typing import Any

LOGGER = logging.getLogger(__name__)


def _quote_strip(string: str) -> str:
    return string.replace("'", "").replace('"', "")


def obj_from_recipe(
    thing: Any,
    module: str,
    klass: str | None = None,
    constructor: str | None = None,
) -> Any:
    """Recreate an object from a recipe.

    Can be module.klass(thing), module.constructor(thing), or
    module.klass.constructor(thing).

    Args:
        thing: what will be passed to ``constructor`` / ``__init__``
        module: the module that the function or class can be found in
        klass: The __qualname__ of the class, if needed. If None, then
            ``constructor`` must be a function.
        constructor: A constructor method on a class if ``klass`` is specified,
            otherwise a function that returns the desired object. If None, the
            class ``klass`` will be created using its ``__init__``.

    """
    if klass is None and constructor is None:
        raise AssertionError("Must specify at least one of `klass` and `constructor`")

    module = import_module(module)
    if klass is None:
        klass_or_const = getattr(module, constructor)
    else:
        klass_or_const = getattr(module, klass)
        if constructor is not None:
            klass_or_const = getattr(klass_or_const, constructor)

    if isinstance(thing, str):
        return klass_or_const(_quote_strip(thing))
    if isinstance(thing, dict):
        return klass_or_const(**thing)
    if isinstance(thing, (tuple, list)):
        return klass_or_const(*thing)
    return klass_or_const(thing)


def _get_version(obj: Any) -> str:
    mod = import_module(obj.__class__.__module__.partition(".")[0])
    for v_attr in ("__version__", "version", "release"):
        if hasattr(mod, v_attr):
            return getattr(mod, v_attr)
    return "unknown"


def _get_username():
    try:
        return getpass.getuser()
    except ModuleNotFoundError as exc0:
        import os

        try:
            return os.getlogin()
        except Exception as exc1:
            LOGGER.error("No username %r from %r", exc1, exc0)
            return "unknown"


def _objinfo(obj: Any, constructor=None) -> tuple[str, ...]:
    return obj.__class__.__module__, obj.__class__.__qualname__, constructor
