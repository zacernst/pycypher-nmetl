"""Tests for the _init_null_result utility and NullMaskResult class."""

import numpy as np
import pandas as pd
import pytest
from pycypher.constants import NullMaskResult, _init_null_result


class TestNullMaskResult:
    """Tests for NullMaskResult properties."""

    def test_all_null_true_when_non_null_vals_is_none(self):
        nr = NullMaskResult(pd.Series([None]), pd.Series([False]), None)
        assert nr.all_null is True

    def test_all_null_false_when_non_null_vals_present(self):
        nr = NullMaskResult(
            pd.Series([None, None]),
            pd.Series([False, True]),
            pd.Series([42]),
        )
        assert nr.all_null is False

    def test_slots(self):
        nr = NullMaskResult(pd.Series([]), pd.Series([], dtype=bool), None)
        assert hasattr(nr, "__slots__")
        with pytest.raises(AttributeError):
            nr.extra_attr = 1  # type: ignore[attr-defined]


class TestInitNullResult:
    """Tests for _init_null_result."""

    def test_all_null_input(self):
        s = pd.Series([None, None, None])
        nr = _init_null_result(s)
        assert nr.all_null is True
        assert len(nr.result) == 3
        assert nr.result.isna().all()
        assert nr.non_null_vals is None

    def test_no_null_input(self):
        s = pd.Series([1, 2, 3])
        nr = _init_null_result(s)
        assert nr.all_null is False
        assert len(nr.result) == 3
        assert nr.non_null_mask.all()
        assert list(nr.non_null_vals) == [1, 2, 3]

    def test_mixed_nulls(self):
        s = pd.Series([10, None, 30, None, 50])
        nr = _init_null_result(s)
        assert nr.all_null is False
        assert len(nr.result) == 5
        assert nr.result.isna().all()  # pre-allocated as all null
        assert list(nr.non_null_mask) == [True, False, True, False, True]
        assert list(nr.non_null_vals) == [10, 30, 50]

    def test_empty_series(self):
        s = pd.Series([], dtype=object)
        nr = _init_null_result(s)
        # Empty series has no non-null values
        assert nr.all_null is True
        assert len(nr.result) == 0

    def test_preserves_index(self):
        s = pd.Series([None, 42, None], index=[10, 20, 30])
        nr = _init_null_result(s)
        assert list(nr.result.index) == [10, 20, 30]

    def test_single_value(self):
        s = pd.Series([7])
        nr = _init_null_result(s)
        assert nr.all_null is False
        assert list(nr.non_null_vals) == [7]

    def test_single_null(self):
        s = pd.Series([None])
        nr = _init_null_result(s)
        assert nr.all_null is True

    def test_assignment_pattern(self):
        """Test the typical usage pattern: compute on non-nulls, assign back."""
        s = pd.Series([1, None, 3, None, 5])
        nr = _init_null_result(s)
        if not nr.all_null:
            nr.result[nr.non_null_mask] = nr.non_null_vals * 2
        expected = pd.Series([2.0, None, 6.0, None, 10.0], dtype=object)
        # Check non-null positions
        assert nr.result.iloc[0] == 2
        assert nr.result.iloc[2] == 6
        assert nr.result.iloc[4] == 10
        # Check null positions
        assert pd.isna(nr.result.iloc[1])
        assert pd.isna(nr.result.iloc[3])

    def test_nan_treated_as_null(self):
        s = pd.Series([1.0, np.nan, 3.0])
        nr = _init_null_result(s)
        assert nr.all_null is False
        assert list(nr.non_null_mask) == [True, False, True]
        assert list(nr.non_null_vals) == [1.0, 3.0]
