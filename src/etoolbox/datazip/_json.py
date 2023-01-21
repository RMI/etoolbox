from __future__ import annotations

import json
import logging
from datetime import datetime
from functools import partial
from importlib import import_module
from pathlib import Path
from types import NoneType
from typing import Any

from etoolbox.datazip._utils import _objinfo, _quote_strip

LOGGER = logging.getLogger(__name__)


class _TypeHintingEncoder(json.JSONEncoder):
    def encode(self, obj):
        def hint_types(item):
            if isinstance(item, (str, int, float, NoneType)):
                return item
            if isinstance(item, tuple):
                if hasattr(item, "_asdict"):
                    return {
                        "__nt__": True,
                        "items": {k: hint_types(v) for k, v in item._asdict().items()},
                        "objinfo": _objinfo(item),
                    }
                return {"__tuple__": True, "items": [hint_types(e) for e in item]}
            if isinstance(item, dict):
                return {key: hint_types(value) for key, value in item.items()}
            if isinstance(item, list):
                return [hint_types(e) for e in item]
            if isinstance(item, set):
                return {"__set__": True, "items": [hint_types(e) for e in item]}
            if isinstance(item, frozenset):
                return {"__frozenset__": True, "items": [hint_types(e) for e in item]}
            if isinstance(item, complex):
                return {"__complex__": True, "real": item.real, "imag": item.imag}
            if isinstance(item, datetime):
                return {"__datetime__": True, "items": str(item)}
            if isinstance(item, Path):
                return {"__path__": True, "items": str(item)}
            return item

        return super().encode(hint_types(obj))


def _type_hinted_hook(obj: Any) -> Any:
    if "__tuple__" in obj:
        return tuple(obj["items"])
    if "__set__" in obj:
        return set(obj["items"])
    if "__frozenset__" in obj:
        return frozenset(obj["items"])
    if "__complex__" in obj:
        return complex(obj["real"], obj["imag"])
    if "__datetime__" in obj:
        return datetime.fromisoformat(_quote_strip(obj["items"]))
    if "__nt__" in obj:
        m, q, _ = obj["objinfo"]
        try:
            return getattr(import_module(m), q)(**obj["items"])
        except Exception as exc:
            LOGGER.error(
                "Unable to load %s from %s, so namedtuple will be returned as a normal tuple, %r",
                q,
                m,
                exc,
            )
            return tuple(obj["items"].values())
    if "__path__" in obj:
        return Path(_quote_strip(obj["items"]))
    return obj


json_dumps = partial(json.dumps, ensure_ascii=False, indent=4, cls=_TypeHintingEncoder)
json_loads = partial(json.loads, object_hook=_type_hinted_hook)
