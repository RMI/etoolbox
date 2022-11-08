"""Utilities for working with :mod:`pandas` objects."""
from __future__ import annotations

import logging
from collections import Counter

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def compare_dfs(
    self: pd.DataFrame,
    other: pd.DataFrame,
    align_col: str | list | None = None,
    decimals: int | dict = 5,
    **kwargs,
) -> pd.DataFrame:
    """Compare dataframes without perfect index/column alignment.

    Compare two dataframes on the intersection of their columns and
    aligned rows, roughly equivalent to pd.DataFrame.compare(self, other)

    Args:
        self: the first Dataframe (self in outputs)
        other: the second Dataframe (other in outputs)
        align_col: the column to align the dfs on, values should be unique, if a list,
            the columns will be cast as strings and concatenated. If the resulting
            values are not unique, duplicated values and values not in both
            dfs will be shown separately
        decimals: Number of decimal places to round each column to.
            If an int is given, round each column to the same number of places.
            Otherwise dict and Series round to variable numbers of places.
            Column names should be in the keys if decimals is a dict-like. Any columns not
            included in decimals will be left as is. Elements of decimals
            which are not columns of the input will be ignored.

    Returns: a Comparison of the two dataframes
    """
    # determine which columns we can actually use to compare the dfs
    cols = list(set(self.columns).intersection(other.columns))
    # generally don't care about small differences in values so round
    # the numeric values in both dfs
    self, other = self.round(decimals), other.round(decimals)
    # if the indexes match just do a straight compare
    if len(self) == len(other) and np.all(self.index == other.index):
        return self[cols].compare(other[cols], **kwargs)
    # if they don't match, there is more work to do
    if align_col is None:
        raise ValueError("with mismatched index, must specify `align_col`")
    if isinstance(align_col, list):
        # cat together columns for alignment
        assert (
            len(align_col) > 1
        ), "if `align_col` is a list, it must represent more than one column"
        _align_col = "+".join(align_col)
        self[_align_col] = self[align_col].astype(str).apply("___".join, axis=1)
        other[_align_col] = other[align_col].astype(str).apply("___".join, axis=1)
        align_col = _align_col
    # determine common values in align column, get just those values
    # sort the dfs and pull out the right columns
    vals = set(self[align_col]).intersection(other[align_col])
    self_ = (
        self[self[align_col].isin(vals)]
        .sort_values([align_col])
        .reset_index(drop=True)[cols + [align_col]]
    )
    other_ = (
        other[other[align_col].isin(vals)]
        .sort_values([align_col])
        .reset_index(drop=True)[cols + [align_col]]
    )

    def _add_ix(df, name):
        return pd.concat(
            [df.set_index([align_col])],
            keys=[name],
            axis=1,
        ).reorder_levels([1, 0], axis=1)

    out = pd.concat(
        {
            "both": self_.drop_duplicates(subset=align_col)
            .set_index(align_col)
            .compare(
                other_.drop_duplicates(subset=align_col).set_index(align_col),
                **kwargs,
            ),
            "self_dup": _add_ix(
                self_[self_[align_col].duplicated(keep=False)],
                "self",
            ),
            "other_dup": _add_ix(
                other_[other_[align_col].duplicated(keep=False)], "other"
            ),
            "self_only": _add_ix(self[~self[align_col].isin(vals)], "self"),
            "other_only": _add_ix(other[~other[align_col].isin(vals)], "other"),
        },
        names=["merge"],
    )
    out = out.reset_index()
    al_cols = out.iloc[:, 1].str.split("___", expand=True)
    al_cols.columns = pd.MultiIndex.from_product([align_col.split("+"), ("aligned",)])
    out = pd.concat([al_cols, out], axis=1).set_index(
        [("merge", "")] + list(al_cols.columns)
    )
    out.index.names = ["merge"] + align_col.split("+")
    logger.warning("category counts: %s", Counter(out.index.get_level_values(0)))
    return out.iloc[:, 1:]
