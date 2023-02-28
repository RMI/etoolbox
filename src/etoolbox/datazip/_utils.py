from __future__ import annotations

import getpass
import logging
from contextlib import suppress
from importlib import import_module
from typing import Any

LOGGER = logging.getLogger(__name__)


def _quote_strip(string: str) -> str:
    return string.replace("'", "").replace('"', "")


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


def _objinfo(obj: Any) -> str:
    return obj.__class__.__module__ + "|" + obj.__class__.__qualname__


def _get_klass(mod_klass: str | list | tuple):
    with suppress(AttributeError):
        mod_klass = mod_klass.split("|")
    try:
        mod, qname, *_ = mod_klass
        klass: type = getattr(import_module(mod), qname)
    except (AttributeError, ModuleNotFoundError) as exc:
        raise ImportError(f"Unable to import {qname} from {mod}.") from exc
    else:
        return klass


def default_setstate(obj, state):
    """Called if no ``__setstate__`` implementation."""
    if state is None:
        pass
    elif isinstance(state, dict):
        obj.__dict__ = state
    elif isinstance(state, tuple):
        d_state, s_state = state
        if d_state is not None:
            obj.__dict__ = d_state
        for k, v in s_state.items():
            setattr(obj, k, v)


def default_getstate(obj):
    """Called if no ``__getstate__`` implementation."""

    def slots_dict(_slots):
        sout = {}
        for k in _slots:
            if k != "__dict__":
                with suppress(AttributeError):
                    sout.update({k: getattr(obj, k)})
        return sout

    match obj:
        case object(__dict__=d_state, __slots__=slots):
            return d_state.copy(), slots_dict(slots)
        case object(__dict__=d_state):
            return d_state.copy()
        case object(__slots__=slots):
            return None, slots_dict(slots)
        case _:
            return None
