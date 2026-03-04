"""
Tests for the 5 changes from the MultiTable Source Fixes handover document.
Designed to run before AND after fixes to confirm bugs exist and are resolved.

Usage:
    python -m pytest tests/test_handover_fixes.py -v
"""

import sys
import os
import io
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import polars as pl
import pandas as pd
from multitable import MultiTable


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def polars_mt():
    """MultiTable wrapping a Polars LazyFrame with mixed types."""
    df = pl.DataFrame({
        "name":   ["Alice", "Bob", "Charlie"],
        "age":    [30, 25, 35],
        "salary": [50000.0, 60000.0, 70000.0],
        "joined": [
            pl.Series([None]).cast(pl.Date).to_list()[0],  # null placeholder
            pl.Series([None]).cast(pl.Date).to_list()[0],
            pl.Series([None]).cast(pl.Date).to_list()[0],
        ],
    })
    # Create proper date column
    df = pl.DataFrame({
        "name":   ["Alice", "Bob", "Charlie"],
        "age":    [30, 25, 35],
        "salary": [50000.0, 60000.0, 70000.0],
    })
    lazy = df.lazy()
    return MultiTable(lazy, src_path="test.csv", table_name="test", frame_type="polars")


@pytest.fixture
def polars_mt_with_dates():
    """MultiTable wrapping a Polars LazyFrame with date columns."""
    from datetime import date
    df = pl.DataFrame({
        "name":   ["Alice", "Bob", "Charlie"],
        "event_date": [date(2024, 1, 1), date(2024, 6, 15), date(2024, 12, 31)],
        "score": [95.5, 87.3, 92.1],
    })
    lazy = df.lazy()
    return MultiTable(lazy, src_path="test.csv", table_name="test_dates", frame_type="polars")


@pytest.fixture
def polars_mt_with_decimals():
    """MultiTable with a Polars Decimal column."""
    from decimal import Decimal as PyDecimal
    df = pl.DataFrame({
        "amount": [PyDecimal("10.50"), PyDecimal("20.75"), PyDecimal("30.00")],
        "label":  ["a", "b", "c"],
    })
    lazy = df.lazy()
    return MultiTable(lazy, src_path="test.csv", table_name="test_dec", frame_type="polars")


@pytest.fixture
def pandas_mt():
    """MultiTable wrapping a Pandas DataFrame."""
    df = pd.DataFrame({
        "name":   ["Charlie", "Alice", "Bob"],
        "age":    [35, 30, 25],
        "salary": [70000.0, 50000.0, 60000.0],
    })
    return MultiTable(df, src_path="test.csv", table_name="test", frame_type="pandas")


@pytest.fixture
def pandas_mt_with_dates():
    """MultiTable wrapping a Pandas DataFrame with datetime column."""
    df = pd.DataFrame({
        "name":   ["Alice", "Bob", "Charlie"],
        "event_date": pd.to_datetime(["2024-01-01", "2024-06-15", "2024-12-31"]),
        "score":  [95.5, 87.3, 92.1],
    })
    return MultiTable(df, src_path="test.csv", table_name="test_dates", frame_type="pandas")


# ══════════════════════════════════════════════════════════════════════════════
# Change 1: .schema property (native types)
# ══════════════════════════════════════════════════════════════════════════════

class TestSchemaProperty:
    """Change 1: .schema should return native type objects, not strings."""

    def test_schema_property_exists(self, polars_mt):
        """The .schema property should exist on MultiTable."""
        assert hasattr(polars_mt, 'schema'), \
            "BUG CONFIRMED: MultiTable has no .schema property"

    def test_schema_returns_native_polars_types(self, polars_mt):
        """Schema should return native pl.DataType objects, not strings."""
        if not hasattr(polars_mt, 'schema'):
            pytest.skip("schema property not yet added")
        schema = polars_mt.schema
        assert schema["age"] == pl.Int64, \
            f"Expected pl.Int64, got {schema['age']}"
        assert schema["name"] == pl.Utf8, \
            f"Expected pl.Utf8, got {schema['name']}"

    def test_schema_returns_native_pandas_types(self, pandas_mt):
        """Schema should return numpy dtype objects for pandas."""
        if not hasattr(pandas_mt, 'schema'):
            pytest.skip("schema property not yet added")
        import numpy as np
        schema = pandas_mt.schema
        assert schema["age"] == np.dtype('int64'), \
            f"Expected int64 dtype, got {schema['age']}"

    def test_dtypes_vs_schema_difference(self, polars_mt):
        """dtypes returns strings; schema should return native types."""
        if not hasattr(polars_mt, 'schema'):
            pytest.skip("schema property not yet added")
        dtypes = polars_mt.dtypes
        schema = polars_mt.schema
        # dtypes values are strings
        assert isinstance(dtypes["age"], str)
        # schema values should NOT be strings
        assert not isinstance(schema["age"], str), \
            "schema should return native types, not strings"


# ══════════════════════════════════════════════════════════════════════════════
# Change 2: Fix get_values_to_list (sort + error msg + warning)
# ══════════════════════════════════════════════════════════════════════════════

class TestGetValuesToList:

    # 2a. Missing sort
    def test_values_are_sorted(self, polars_mt):
        """Docstring says 'sorted ascending' but values may not be sorted."""
        values = polars_mt.get_values_to_list("name")
        assert values == sorted(values), \
            f"BUG CONFIRMED: get_values_to_list returns unsorted values: {values}"

    def test_numeric_values_sorted(self, pandas_mt):
        """Numeric values should also be returned sorted."""
        values = pandas_mt.get_values_to_list("age")
        assert values == sorted(values), \
            f"BUG CONFIRMED: get_values_to_list returns unsorted values: {values}"

    # 2b. Error message references TopBottomNCoding
    def test_error_message_correct(self):
        """Error on unsupported backend should reference get_values_to_list, not TopBottomNCoding."""
        # Create a MT with a fake frame_type to trigger the error
        df = pd.DataFrame({"a": [1]})
        mt = MultiTable(df, src_path="test.csv", table_name="test", frame_type="pandas")
        mt.frame_type = "fake_backend"  # force unsupported
        with pytest.raises(NotImplementedError) as exc_info:
            mt.get_values_to_list("a")
        error_msg = str(exc_info.value)
        assert "TopBottomNCoding" not in error_msg, \
            f"BUG CONFIRMED: Error message references 'TopBottomNCoding': {error_msg}"
        assert "get_values_to_list" in error_msg, \
            f"Error message should reference 'get_values_to_list': {error_msg}"


# ══════════════════════════════════════════════════════════════════════════════
# Change 3: validate_string_column method
# ══════════════════════════════════════════════════════════════════════════════

class TestValidateStringColumn:

    def test_method_exists(self, polars_mt):
        """validate_string_column should exist."""
        assert hasattr(polars_mt, 'validate_string_column'), \
            "BUG CONFIRMED: validate_string_column not yet added"

    def test_string_column_returns_true(self, polars_mt):
        """String column should validate as True."""
        if not hasattr(polars_mt, 'validate_string_column'):
            pytest.skip("validate_string_column not yet added")
        assert polars_mt.validate_string_column("name") is True

    def test_numeric_column_returns_false(self, polars_mt):
        """Numeric column should validate as False for string check."""
        if not hasattr(polars_mt, 'validate_string_column'):
            pytest.skip("validate_string_column not yet added")
        assert polars_mt.validate_string_column("age") is False

    def test_pandas_string_column(self, pandas_mt):
        """Pandas string column should validate correctly."""
        if not hasattr(pandas_mt, 'validate_string_column'):
            pytest.skip("validate_string_column not yet added")
        assert pandas_mt.validate_string_column("name") is True
        assert pandas_mt.validate_string_column("age") is False


# ══════════════════════════════════════════════════════════════════════════════
# Change 4: validate_datetime_column method
# ══════════════════════════════════════════════════════════════════════════════

class TestValidateDatetimeColumn:

    def test_method_exists(self, polars_mt_with_dates):
        """validate_datetime_column should exist."""
        assert hasattr(polars_mt_with_dates, 'validate_datetime_column'), \
            "BUG CONFIRMED: validate_datetime_column not yet added"

    def test_date_column_returns_true(self, polars_mt_with_dates):
        """Date column should validate as True."""
        if not hasattr(polars_mt_with_dates, 'validate_datetime_column'):
            pytest.skip("validate_datetime_column not yet added")
        assert polars_mt_with_dates.validate_datetime_column("event_date") is True

    def test_non_date_column_returns_false(self, polars_mt_with_dates):
        """Non-date column should validate as False."""
        if not hasattr(polars_mt_with_dates, 'validate_datetime_column'):
            pytest.skip("validate_datetime_column not yet added")
        assert polars_mt_with_dates.validate_datetime_column("name") is False

    def test_pandas_datetime_column(self, pandas_mt_with_dates):
        """Pandas datetime column should validate correctly."""
        if not hasattr(pandas_mt_with_dates, 'validate_datetime_column'):
            pytest.skip("validate_datetime_column not yet added")
        assert pandas_mt_with_dates.validate_datetime_column("event_date") is True
        assert pandas_mt_with_dates.validate_datetime_column("name") is False


# ══════════════════════════════════════════════════════════════════════════════
# Change 5: pl.Decimal missing from validate_numeric_column
# ══════════════════════════════════════════════════════════════════════════════

class TestValidateNumericDecimal:

    def test_decimal_column_is_numeric(self, polars_mt_with_decimals):
        """pl.Decimal columns should be recognized as numeric."""
        result = polars_mt_with_decimals.validate_numeric_column("amount")
        assert result is True, \
            "BUG CONFIRMED: pl.Decimal not recognized as numeric by validate_numeric_column"

    def test_string_column_not_numeric(self, polars_mt_with_decimals):
        """String column should still not be numeric."""
        result = polars_mt_with_decimals.validate_numeric_column("label")
        assert result is False
