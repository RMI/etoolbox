"""Dummy modules and classes when optional packages are not installed.

Could do something more like this:
https://github.com/pola-rs/polars/blob/master/py-polars/polars/dependencies.py
"""

import warnings

try:
    import sqlalchemy
except (ModuleNotFoundError, ImportError):

    class sqlalchemy:  # noqa: N801
        """Dummy for :mod:`sqlalchemy` when not installed."""

        class engine:  # noqa: N801
            """Dummy for :mod:`sqlalchemy.engine` when not installed."""

            class Engine:
                """Dummy for :mod:`sqlalchemy.engine.Engine` when not installed."""

                pass

        @staticmethod
        def create_engine(*args, **kwargs):
            """Dummy for :func:`sqlalchemy.create_engine` when not installed."""
            pass


try:
    import plotly
except (ModuleNotFoundError, ImportError):

    class plotly:  # noqa: N801
        """Dummy for :mod:`plotly` when not installed."""

        class graph_objects:  # noqa: N801
            """Dummy for :mod:`plotly.graph_objects` when not installed."""

            class Figure:
                """Dummy for :mod:`plotly.graph_objects.Figure` when not installed."""

                pass


try:
    from tqdm.auto import tqdm
    from tqdm.contrib.logging import logging_redirect_tqdm
except (ImportError, ModuleNotFoundError):
    from contextlib import nullcontext

    logging_redirect_tqdm = nullcontext

    class tqdm(nullcontext):  # noqa: N801
        """Dummy for :class:`tqdm` when not installed."""

        def __init__(self, *args, **kwargs):
            """Dummy init for :class:`.tqdm` when not installed."""
            warnings.warn(
                "tqdm is not installed, progress bar will be disabled",
                UserWarning,
                stacklevel=2,
            )
            super().__init__()

        def update(self, *args, **kwargs):
            """Dummy update for :class:`.tqdm` when not installed."""
            pass
