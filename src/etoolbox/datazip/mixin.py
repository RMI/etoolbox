"""A mixin for adding :class:`.DataZip` functionality to another class.

The goal is to build this out so that most custom classes can be stored and recovered by
inheriting this mixin.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any
from zipfile import ZIP_STORED

from etoolbox.datazip.core import DataZip

if TYPE_CHECKING:
    from io import BytesIO
    from pathlib import Path


LOGGER = logging.getLogger("etoolbox")


class IOMixin:
    """Mixin for adding :class:`.DataZip` IO.

    Examples
    --------
    >>> from io import BytesIO
    >>> import numpy as np
    >>> import pandas as pd

    Create a class that inherits from :class:`IOMixin`.

    >>> class MyClass(IOMixin):
    ...     pass

    Create an instance of the class with many kinds of things in it.
    As nested as you like.

    >>> inst = MyClass()
    >>> inst.foo = {"a": 1, "b": (1, 2, 3), "c": np.array([1, 2, 4])}
    >>> inst.bar = pd.Series([1, 2, 3])

    The object can now be saved to a file, or buffer for this example.

    >>> buffer = BytesIO()
    >>> inst.to_file(buffer)
    >>> del inst

    And we can bring it back, as if it was pickled. Usually.

    >>> inst = MyClass.from_file(buffer)
    >>> type(inst.bar)
    <class 'pandas.core.series.Series'>

    >>> inst.foo
    {'a': 1, 'b': (1, 2, 3), 'c': array([1, 2, 4])}

    If your class has funny things in it like lambdas or unserializable objects,
    you will need to define ``__getstate__`` and ``__setstate__``. If you don't use
    ``__slots__`` they can be very simple.

    >>> from collections import defaultdict
    >>> class MyFunnyClass(IOMixin):
    ...     def __init__(self):
    ...         self.stuff = defaultdict(lambda: None)
    ...
    ...     def __getstate__(self):
    ...         return self.__dict__ | {"stuff": dict(self.__dict__["stuff"])}
    ...
    ...     def __setstate__(self, state):
    ...         self.__dict__ = state | {
    ...             "stuff": defaultdict(lambda: None, state["stuff"])
    ...         }

    Instantiate the class and use the :class:`defaultdict`.

    >>> inst = MyFunnyClass()
    >>> inst.stuff["bar"] = 3
    >>> print(inst.stuff["foo"])
    None

    Dump the object into the buffer and delete the original instance.

    >>> buffer = BytesIO()
    >>> inst.to_file(buffer)
    >>> del inst

    Recreate the object from the buffer and confirm it is as it should be.

    >>> inst = MyFunnyClass.from_file(buffer)
    >>> type(inst.stuff)
    <class 'collections.defaultdict'>

    >>> print(inst.stuff["foobar"])
    None

    >>> dict(inst.stuff)
    {'bar': 3, 'foo': None, 'foobar': None}
    """

    # in case child uses __slots__
    __slots__ = ()

    @classmethod
    def from_file(cls, path: Path | str | BytesIO, **kwargs) -> Any:
        """Recreate object fom file or buffer."""
        return DataZip.load(path, klass=cls)

    def to_file(
        self,
        path: Path | str | BytesIO,
        compression=ZIP_STORED,
        clobber=False,  # noqa: FBT002
        **kwargs,
    ) -> None:
        """Write object to file or buffer."""
        DataZip.dump(self, path, compression=compression, clobber=clobber)
