from __future__ import annotations

import datetime as dt
import re
from re import DOTALL
from typing import TYPE_CHECKING, Any, cast

import pytest
from hypothesis import assume, given
from hypothesis.strategies import none
from numpy import array, nan
from numpy.testing import assert_equal
from pandas import (
    NA,
    DataFrame,
    Index,
    NaT,
    RangeIndex,
    Series,
    Timestamp,
    concat,
    to_datetime,
)
from pandas.testing import assert_index_equal, assert_series_equal

from utilities.datetime import TODAY_UTC, UTC
from utilities.hypothesis import int_indexes, text_ascii, timestamps
from utilities.numpy import datetime64ns
from utilities.pandas import (
    TIMESTAMP_MAX_AS_DATE,
    TIMESTAMP_MAX_AS_DATETIME,
    TIMESTAMP_MIN_AS_DATE,
    TIMESTAMP_MIN_AS_DATETIME,
    CheckIndexError,
    CheckPandasDataFrameError,
    CheckRangeIndexError,
    EmptyPandasConcatError,
    IndexI,
    Int64,
    ReindexToSetError,
    ReindexToSubSetError,
    ReindexToSuperSetError,
    SeriesA,
    SeriesMinMaxError,
    TimestampToDateTimeError,
    UnionIndexesError,
    astype,
    boolean,
    check_index,
    check_pandas_dataframe,
    check_range_index,
    redirect_empty_pandas_concat,
    reindex_to_set,
    reindex_to_subset,
    reindex_to_superset,
    rename_index,
    series_max,
    series_min,
    sort_index,
    string,
    timestamp_to_date,
    timestamp_to_datetime,
    to_numpy,
    union_indexes,
)

if TYPE_CHECKING:
    from collections.abc import Callable


class TestAsType:
    def test_main(self) -> None:
        df = DataFrame(0, index=RangeIndex(1), columns=["value"], dtype=int)
        check_pandas_dataframe(df, dtypes={"value": int})
        result = astype(df, float)
        check_pandas_dataframe(result, dtypes={"value": float})


class TestCheckIndex:
    def test_main(self) -> None:
        check_index(RangeIndex(1))

    def test_length_pass(self) -> None:
        check_index(RangeIndex(1), length=1)

    def test_length_error(self) -> None:
        with pytest.raises(
            CheckIndexError, match=r"Index .* must satisfy the length requirements\."
        ):
            check_index(RangeIndex(1), length=2)

    def test_min_length_pass(self) -> None:
        check_index(RangeIndex(2), min_length=1)

    def test_min_length_error(self) -> None:
        with pytest.raises(
            CheckIndexError, match=r"Index .* must satisfy the length requirements\."
        ):
            check_index(RangeIndex(0), min_length=1)

    def test_max_length_pass(self) -> None:
        check_index(RangeIndex(0), max_length=1)

    def test_max_length_error(self) -> None:
        with pytest.raises(
            CheckIndexError, match=r"Index .* must satisfy the length requirements\."
        ):
            check_index(RangeIndex(2), max_length=1)

    def test_name_pass(self) -> None:
        check_index(RangeIndex(0), name=None)

    def test_name_error(self) -> None:
        with pytest.raises(
            CheckIndexError, match=r"Index .* must satisfy the name requirement\."
        ):
            check_index(RangeIndex(0), name="name")

    def test_sorted_pass(self) -> None:
        check_index(Index(["A", "B"]), sorted=True)

    def test_sorted_error(self) -> None:
        with pytest.raises(CheckIndexError, match=r"Index .* must be sorted\."):
            check_index(Index(["B", "A"]), sorted=True)

    def test_unique_pass(self) -> None:
        check_index(Index(["A", "B"]), unique=True)

    def test_unique_error(self) -> None:
        with pytest.raises(CheckIndexError, match=r"Index .* must be unique\."):
            check_index(Index(["A", "A"]), unique=True)


class TestCheckPandasDataFrame:
    def test_main(self) -> None:
        check_pandas_dataframe(DataFrame())

    def test_columns_pass(self) -> None:
        df = DataFrame(0.0, index=RangeIndex(0), columns=[])
        check_pandas_dataframe(df, columns=[])

    def test_columns_error(self) -> None:
        df = DataFrame(0.0, index=RangeIndex(0), columns=["value"])
        with pytest.raises(
            CheckPandasDataFrameError,
            match=re.compile(
                r"DataFrame must have columns .*; got .*\n\n.*\.", flags=DOTALL
            ),
        ):
            check_pandas_dataframe(df, columns=["other"])

    def test_dtypes_pass(self) -> None:
        df = DataFrame(0.0, index=RangeIndex(0), columns=[])
        check_pandas_dataframe(df, dtypes={})

    def test_dtypes_error_set_of_columns(self) -> None:
        df = DataFrame(0.0, index=RangeIndex(0), columns=[])
        with pytest.raises(
            CheckPandasDataFrameError,
            match=re.compile(
                r"DataFrame must have dtypes .*; got .*\n\n.*\.", flags=DOTALL
            ),
        ):
            check_pandas_dataframe(df, dtypes={"value": int})

    def test_dtypes_error_order_of_columns(self) -> None:
        df = DataFrame(0.0, index=RangeIndex(0), columns=["a", "b"])
        with pytest.raises(
            CheckPandasDataFrameError,
            match=re.compile(
                r"DataFrame must have dtypes .*; got .*\n\n.*\.", flags=DOTALL
            ),
        ):
            check_pandas_dataframe(df, dtypes={"b": float, "a": float})

    def test_length_pass(self) -> None:
        df = DataFrame(0.0, index=RangeIndex(1), columns=["value"])
        check_pandas_dataframe(df, length=1)

    def test_length_error(self) -> None:
        df = DataFrame(0.0, index=RangeIndex(1), columns=["value"])
        with pytest.raises(
            CheckPandasDataFrameError,
            match=re.compile(
                r"DataFrame must satisfy the length requirements; got .*\n\n.*\.",
                flags=DOTALL,
            ),
        ):
            check_pandas_dataframe(df, length=2)

    def test_min_length_pass(self) -> None:
        df = DataFrame(0.0, index=RangeIndex(2), columns=["value"])
        check_pandas_dataframe(df, min_length=1)

    def test_min_length_error(self) -> None:
        df = DataFrame(0.0, index=RangeIndex(0), columns=["value"])
        with pytest.raises(
            CheckPandasDataFrameError,
            match=re.compile(
                r"DataFrame must satisfy the length requirements; got .*\n\n.*\.",
                flags=DOTALL,
            ),
        ):
            check_pandas_dataframe(df, min_length=1)

    def test_max_length_pass(self) -> None:
        df = DataFrame(0.0, index=RangeIndex(0), columns=["value"])
        check_pandas_dataframe(df, max_length=1)

    def test_max_length_error(self) -> None:
        df = DataFrame(0.0, index=RangeIndex(2), columns=["value"])
        with pytest.raises(
            CheckPandasDataFrameError,
            match=re.compile(
                r"DataFrame must satisfy the length requirements; got .*\n\n.*\.",
                flags=DOTALL,
            ),
        ):
            check_pandas_dataframe(df, max_length=1)

    def test_sorted_pass(self) -> None:
        df = DataFrame([[0.0], [1.0]], index=RangeIndex(2), columns=["value"])
        check_pandas_dataframe(df, sorted="value")

    def test_sorted_error(self) -> None:
        df = DataFrame([[1.0], [0.0]], index=RangeIndex(2), columns=["value"])
        with pytest.raises(
            CheckPandasDataFrameError,
            match=re.compile(r"DataFrame must be sorted on .*\n\n.*\.", flags=DOTALL),
        ):
            check_pandas_dataframe(df, sorted="value")

    def test_standard_pass(self) -> None:
        check_pandas_dataframe(DataFrame(index=RangeIndex(0)), standard=True)

    @pytest.mark.parametrize(
        "df",
        [
            pytest.param(DataFrame(0.0, index=Index(["A"]), columns=Index(["value"]))),
            pytest.param(
                DataFrame(0.0, index=RangeIndex(1, 2), columns=Index(["value"]))
            ),
            pytest.param(
                DataFrame(0.0, index=RangeIndex(1, step=2), columns=Index(["value"]))
            ),
            pytest.param(
                DataFrame(
                    0.0, index=RangeIndex(1, name="name"), columns=Index(["value"])
                )
            ),
        ],
    )
    def test_standard_errors_index(self, *, df: DataFrame) -> None:
        with pytest.raises(
            CheckPandasDataFrameError,
            match=re.compile(
                r"DataFrame must have a standard index; got .*\n\n.*\.", flags=DOTALL
            ),
        ):
            check_pandas_dataframe(df, standard=True)

    @pytest.mark.parametrize(
        "df",
        [
            pytest.param(
                DataFrame(
                    0.0, index=RangeIndex(1), columns=Index(["value"], name="name")
                )
            ),
            pytest.param(
                DataFrame(0.0, index=RangeIndex(1), columns=Index(["value", "value"]))
            ),
        ],
    )
    def test_standard_errors(self, *, df: DataFrame) -> None:
        with pytest.raises(
            CheckPandasDataFrameError,
            match=re.compile(
                r"DataFrame must have standard columns; got .*\n\n.*\.", flags=DOTALL
            ),
        ):
            check_pandas_dataframe(df, standard=True)

    def test_unique_pass(self) -> None:
        df = DataFrame([[0.0], [1.0]], index=RangeIndex(2), columns=["value"])
        check_pandas_dataframe(df, unique="value")

    def test_unique_error(self) -> None:
        df = DataFrame(0.0, index=RangeIndex(2), columns=["value"])
        with pytest.raises(
            CheckPandasDataFrameError,
            match=re.compile(r"DataFrame must be unique on .*\n\n.*\.", flags=DOTALL),
        ):
            check_pandas_dataframe(df, unique="value")

    def test_width_pass(self) -> None:
        df = DataFrame()
        check_pandas_dataframe(df, width=0)

    def test_width_error(self) -> None:
        df = DataFrame()
        with pytest.raises(
            CheckPandasDataFrameError,
            match=re.compile(
                r"DataFrame must have width .*; got .*\n\n.*\.", flags=DOTALL
            ),
        ):
            check_pandas_dataframe(df, width=1)


class TestCheckRangeIndex:
    def test_main(self) -> None:
        check_range_index(RangeIndex(0))

    def test_start_pass(self) -> None:
        check_range_index(RangeIndex(0), start=0)

    def test_start_error(self) -> None:
        with pytest.raises(CheckRangeIndexError):
            check_range_index(RangeIndex(0), start=1)

    def test_stop_pass(self) -> None:
        check_range_index(RangeIndex(0), stop=0)

    def test_stop_error(self) -> None:
        with pytest.raises(CheckRangeIndexError):
            check_range_index(RangeIndex(0), stop=1)

    def test_step_pass(self) -> None:
        check_range_index(RangeIndex(0), step=1)

    def test_step_error(self) -> None:
        with pytest.raises(CheckRangeIndexError):
            check_range_index(RangeIndex(0), step=2)

    def test_length_pass(self) -> None:
        check_range_index(RangeIndex(1), length=1)

    def test_length_error(self) -> None:
        with pytest.raises(CheckRangeIndexError):
            check_range_index(RangeIndex(1), length=2)

    def test_min_length_pass(self) -> None:
        check_range_index(RangeIndex(2), min_length=1)

    def test_min_length_error(self) -> None:
        with pytest.raises(CheckRangeIndexError):
            check_range_index(RangeIndex(0), min_length=1)

    def test_max_length_pass(self) -> None:
        check_range_index(RangeIndex(0), max_length=1)

    def test_max_length_error(self) -> None:
        with pytest.raises(CheckRangeIndexError):
            check_range_index(RangeIndex(2), max_length=1)

    def test_name_pass(self) -> None:
        check_range_index(RangeIndex(0), name=None)

    def test_name_error(self) -> None:
        with pytest.raises(CheckRangeIndexError):
            check_range_index(RangeIndex(0), name="name")


class TestDTypes:
    @pytest.mark.parametrize(
        "dtype", [pytest.param(Int64), pytest.param(boolean), pytest.param(string)]
    )
    def test_main(self, *, dtype: Any) -> None:
        assert isinstance(Series([], dtype=dtype), Series)


class TestReindexToSet:
    @given(name=text_ascii() | none())
    def test_main(self, *, name: str | None) -> None:
        index = Index([1, 2, 3], name=name)
        target = [3, 2, 1]
        result = reindex_to_set(index, target)
        expected = Index([3, 2, 1], name=name)
        assert_index_equal(result, expected)

    def test_error(self) -> None:
        index = Index([1, 2, 3])
        target = [2, 3, 4]
        with pytest.raises(
            ReindexToSetError, match=r"Index .* and .* must be equal as sets\."
        ):
            _ = reindex_to_set(index, target)


class TestReindexToSubSet:
    @given(name=text_ascii() | none())
    def test_main(self, *, name: str | None) -> None:
        index = Index([1, 2, 3], name=name)
        target = [1]
        result = reindex_to_subset(index, target)
        expected = Index([1], name=name)
        assert_index_equal(result, expected)

    def test_error(self) -> None:
        index = Index([1])
        target = [1, 2, 3]
        with pytest.raises(
            ReindexToSubSetError, match=r"Index .* must be a superset of .*\."
        ):
            _ = reindex_to_subset(index, target)


class TestReindexToSuperSet:
    @given(name=text_ascii() | none())
    def test_main(self, *, name: str | None) -> None:
        index = Index([1], name=name)
        target = [1, 2, 3]
        result = reindex_to_superset(index, target)
        expected = Index([1, 2, 3], name=name)
        assert_index_equal(result, expected)

    def test_error(self) -> None:
        index = Index([1, 2, 3])
        target = [1]
        with pytest.raises(
            ReindexToSuperSetError, match=r"Index .* must be a subset of .*\."
        ):
            _ = reindex_to_superset(index, target)


class TestRedirectEmptyPandasConcat:
    def test_main(self) -> None:
        with pytest.raises(EmptyPandasConcatError), redirect_empty_pandas_concat():
            _ = concat([])


class TestRenameIndex:
    @given(index=int_indexes(), name=text_ascii())
    def test_main(self, *, index: IndexI, name: str) -> None:
        renamed = rename_index(index, name)
        assert renamed.name == name


class TestSeriesMinMax:
    @pytest.mark.parametrize(
        ("x_v", "y_v", "dtype", "expected_min_v", "expected_max_v"),
        [
            pytest.param(0.0, 1.0, float, 0.0, 1.0),
            pytest.param(0.0, nan, float, 0.0, 0.0),
            pytest.param(nan, 1.0, float, 1.0, 1.0),
            pytest.param(nan, nan, float, nan, nan),
            pytest.param(0, 1, Int64, 0, 1),
            pytest.param(0, NA, Int64, 0, 0),
            pytest.param(NA, 1, Int64, 1, 1),
            pytest.param(NA, NA, Int64, NA, NA),
            pytest.param(
                TIMESTAMP_MIN_AS_DATE,
                TIMESTAMP_MAX_AS_DATE,
                datetime64ns,
                TIMESTAMP_MIN_AS_DATE,
                TIMESTAMP_MAX_AS_DATE,
            ),
            pytest.param(
                TIMESTAMP_MIN_AS_DATE,
                NaT,
                datetime64ns,
                TIMESTAMP_MIN_AS_DATE,
                TIMESTAMP_MIN_AS_DATE,
            ),
            pytest.param(
                NaT,
                TIMESTAMP_MAX_AS_DATE,
                datetime64ns,
                TIMESTAMP_MAX_AS_DATE,
                TIMESTAMP_MAX_AS_DATE,
            ),
            pytest.param(NaT, NaT, datetime64ns, NaT, NaT),
        ],
    )
    def test_main(
        self,
        *,
        x_v: Any,
        y_v: Any,
        dtype: Any,
        expected_min_v: Any,
        expected_max_v: Any,
    ) -> None:
        x = Series(data=[x_v], dtype=dtype)
        y = Series(data=[y_v], dtype=dtype)
        result_min = series_min(x, y)
        expected_min = Series(data=[expected_min_v], dtype=dtype)
        assert_series_equal(result_min, expected_min)
        result_max = series_max(x, y)
        expected_max = Series(data=[expected_max_v], dtype=dtype)
        assert_series_equal(result_max, expected_max)

    @pytest.mark.parametrize(
        "func", [pytest.param(series_min), pytest.param(series_max)]
    )
    def test_different_index(
        self, *, func: Callable[[SeriesA, SeriesA], SeriesA]
    ) -> None:
        x = Series(data=nan, index=Index([0], dtype=int))
        y = Series(data=nan, index=Index([1], dtype=int))
        with pytest.raises(AssertionError):
            _ = func(x, y)

    @pytest.mark.parametrize(
        "func", [pytest.param(series_min), pytest.param(series_max)]
    )
    def test_error(self, *, func: Callable[[SeriesA, SeriesA], SeriesA]) -> None:
        x = Series(data=nan, dtype=float)
        y = Series(data=NA, dtype=Int64)  # type: ignore[]
        with pytest.raises(
            SeriesMinMaxError,
            match=re.compile(
                r"Series .* and .* must have the same dtype; got .* and .*\.",
                flags=DOTALL,
            ),
        ):
            _ = func(x, y)


class TestSortIndex:
    @given(index=int_indexes())
    def test_main(self, *, index: IndexI) -> None:
        sorted_ = sort_index(index)
        assert_index_equal(sorted_, cast(Any, index.sort_values()))


class TestTimestampMinMaxAsDate:
    def test_min(self) -> None:
        date = TIMESTAMP_MIN_AS_DATE
        assert isinstance(to_datetime(cast(Timestamp, date)), Timestamp)
        with pytest.raises(ValueError, match="Out of bounds nanosecond timestamp"):
            _ = to_datetime(cast(Timestamp, date - dt.timedelta(days=1)))

    def test_max(self) -> None:
        date = TIMESTAMP_MAX_AS_DATE
        assert isinstance(to_datetime(cast(Timestamp, date)), Timestamp)
        with pytest.raises(ValueError, match="Out of bounds nanosecond timestamp"):
            _ = to_datetime(cast(Timestamp, date + dt.timedelta(days=1)))


class TestTimestampMinMaxAsDateTime:
    def test_min(self) -> None:
        date = TIMESTAMP_MIN_AS_DATETIME
        assert isinstance(to_datetime(date), Timestamp)
        with pytest.raises(ValueError, match="Out of bounds nanosecond timestamp"):
            _ = to_datetime(date - dt.timedelta(microseconds=1))

    def test_max(self) -> None:
        date = TIMESTAMP_MAX_AS_DATETIME
        assert isinstance(to_datetime(date), Timestamp)
        with pytest.raises(ValueError, match="Out of bounds nanosecond timestamp"):
            _ = to_datetime(date + dt.timedelta(microseconds=1))


class TestTimestampToDate:
    @pytest.mark.parametrize(
        ("timestamp", "expected"),
        [
            pytest.param(to_datetime("2000-01-01"), dt.date(2000, 1, 1)),
            pytest.param(to_datetime("2000-01-01 12:00:00"), dt.date(2000, 1, 1)),
        ],
    )
    def test_main(self, *, timestamp: Any, expected: dt.date) -> None:
        assert timestamp_to_date(timestamp) == expected

    def test_error(self) -> None:
        with pytest.raises(TimestampToDateTimeError):
            _ = timestamp_to_date(NaT)


class TestTimestampToDateTime:
    @pytest.mark.parametrize(
        ("timestamp", "expected"),
        [
            pytest.param(
                to_datetime("2000-01-01"), dt.datetime(2000, 1, 1, tzinfo=UTC)
            ),
            pytest.param(
                to_datetime("2000-01-01 12:00:00"),
                dt.datetime(2000, 1, 1, 12, tzinfo=UTC),
            ),
            pytest.param(
                to_datetime("2000-01-01 12:00:00+00:00"),
                dt.datetime(2000, 1, 1, 12, tzinfo=UTC),
            ),
        ],
    )
    def test_main(self, *, timestamp: Any, expected: dt.datetime) -> None:
        assert timestamp_to_datetime(timestamp) == expected

    @given(timestamp=timestamps(allow_nanoseconds=True))
    def test_warn(self, *, timestamp: Timestamp) -> None:
        _ = assume(cast(Any, timestamp).nanosecond != 0)
        with pytest.raises(
            UserWarning, match="Discarding nonzero nanoseconds in conversion"
        ):
            _ = timestamp_to_datetime(timestamp)

    def test_error(self) -> None:
        with pytest.raises(TimestampToDateTimeError):
            _ = timestamp_to_datetime(NaT)


class TestToNumpy:
    @pytest.mark.parametrize(
        ("series_v", "series_d", "array_v", "array_d"),
        [
            pytest.param(True, bool, True, bool),
            pytest.param(False, bool, False, bool),
            pytest.param(True, boolean, True, object),
            pytest.param(False, boolean, False, object),
            pytest.param(NA, boolean, None, object),
            pytest.param(TODAY_UTC, datetime64ns, TODAY_UTC, datetime64ns),
            pytest.param(0, int, 0, int),
            pytest.param(0, Int64, 0, object),
            pytest.param(NA, Int64, None, object),
            pytest.param(nan, float, nan, float),
            pytest.param("", string, "", object),
            pytest.param(NA, string, None, object),
        ],
    )
    def test_main(
        self, *, series_v: Any, series_d: Any, array_v: Any, array_d: Any
    ) -> None:
        series = Series([series_v], dtype=series_d)
        result = to_numpy(series)
        expected = array([array_v], dtype=array_d)
        assert_equal(result, expected)


class TestUnionIndexes:
    @given(name=text_ascii() | none())
    def test_first_named(self, *, name: str | None) -> None:
        left = Index([1, 2, 3], name=name)
        right = Index([2, 3, 4])
        result1 = union_indexes(left, right)
        result2 = union_indexes(right, left)
        expected = Index([1, 2, 3, 4], name=name)
        assert_index_equal(result1, expected)
        assert_index_equal(result2, expected)

    @given(lname=text_ascii(), rname=text_ascii())
    def test_both_named_taking_first(self, *, lname: str, rname: str) -> None:
        left = Index([1, 2, 3], name=lname)
        right = Index([2, 3, 4], name=rname)
        result = union_indexes(left, right, names="first")
        expected = Index([1, 2, 3, 4], name=lname)
        assert_index_equal(result, expected)

    @given(lname=text_ascii(), rname=text_ascii())
    def test_both_named_taking_last(self, *, lname: str, rname: str) -> None:
        left = Index([1, 2, 3], name=lname)
        right = Index([2, 3, 4], name=rname)
        result = union_indexes(left, right, names="last")
        expected = Index([1, 2, 3, 4], name=rname)
        assert_index_equal(result, expected)

    @given(lname=text_ascii(), rname=text_ascii())
    def test_both_named_error(self, *, lname: str, rname: str) -> None:
        _ = assume(lname != rname)
        left = Index([1, 2, 3], name=lname)
        right = Index([2, 3, 4], name=rname)
        with pytest.raises(
            UnionIndexesError,
            match=r"Indexes .* and .* must have the same name; got .* and .*\.",
        ):
            _ = union_indexes(left, right, names="raise")
