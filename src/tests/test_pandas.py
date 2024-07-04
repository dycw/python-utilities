from __future__ import annotations

import datetime as dt
import re
from re import DOTALL
from typing import TYPE_CHECKING, Any, Literal, cast

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
from pandas.testing import assert_frame_equal, assert_index_equal, assert_series_equal
from pytest import mark, param, raises

from utilities.datetime import TODAY_UTC
from utilities.hypothesis import text_ascii, timestamps
from utilities.numpy import datetime64ns
from utilities.pandas import (
    TIMESTAMP_MAX_AS_DATE,
    TIMESTAMP_MAX_AS_DATETIME,
    TIMESTAMP_MIN_AS_DATE,
    TIMESTAMP_MIN_AS_DATETIME,
    AssignBeforeOrAfterError,
    AssignBetweenIndexError,
    AssignBetweenIndicesError,
    CheckIndexError,
    CheckPandasDataFrameError,
    CheckRangeIndexError,
    EmptyPandasConcatError,
    Int64,
    ReindexToSetError,
    ReindexToSubSetError,
    ReindexToSuperSetError,
    SeriesA,
    SeriesMinMaxError,
    TimestampToDateTimeError,
    UnionIndexesError,
    assign_after,
    assign_before,
    assign_between,
    boolean,
    check_index,
    check_pandas_dataframe,
    check_range_index,
    redirect_empty_pandas_concat,
    reindex_to_set,
    reindex_to_subset,
    reindex_to_superset,
    series_max,
    series_min,
    string,
    timestamp_to_date,
    timestamp_to_datetime,
    to_numpy,
    union_indexes,
)
from utilities.zoneinfo import UTC

if TYPE_CHECKING:
    from collections.abc import Callable


class TestAssignBeforeOrAfter:
    def test_after(self) -> None:
        df = DataFrame([[0, 1, 2]], columns=Index(["a", "b", "c"]))
        result = assign_after(df, "b", Series(9, name="z"))
        expected = DataFrame([[0, 1, 9, 2]], columns=Index(["a", "b", "z", "c"]))
        assert_frame_equal(result, expected)

    def test_before(self) -> None:
        df = DataFrame([[0, 1, 2]], columns=Index(["a", "b", "c"]))
        result = assign_before(df, "b", Series(9, name="z"))
        expected = DataFrame([[0, 9, 1, 2]], columns=Index(["a", "z", "b", "c"]))
        assert_frame_equal(result, expected)

    @mark.parametrize("mode", [param("before"), param("after")])
    def test_error(self, *, mode: Literal["before", "after"]) -> None:
        df = DataFrame([[0, 1, 2]], columns=Index(["a", "b", "c"]))
        match mode:
            case "before":
                func = assign_before
            case "after":
                func = assign_after
        with raises(
            AssignBeforeOrAfterError,
            match="DataFrame must contain exactly one column named 'd'",
        ):
            _ = func(df, "d", Series(9, name="z"))


class TestAssignBetween:
    def test_main(self) -> None:
        df = DataFrame([[0, 1, 2, 3]], columns=Index(["a", "b", "c", "d"]))
        result = assign_between(df, "b", "c", Series(9, name="z"))
        expected = DataFrame(
            [[0, 1, 9, 2, 3]], columns=Index(["a", "b", "z", "c", "d"])
        )
        assert_frame_equal(result, expected)

    def test_error_index(self) -> None:
        df = DataFrame([[0, 1, 2]], columns=Index(["a", "b", "c"]))
        with raises(
            AssignBetweenIndexError,
            match="DataFrame must contain exactly one column named 'd'",
        ):
            _ = assign_between(df, "d", "e", Series(9, name="z"))

    def test_error_indices(self) -> None:
        df = DataFrame([[0, 1, 2]], columns=Index(["a", "b", "c"]))
        with raises(
            AssignBetweenIndicesError,
            match="DataFrame must specify consecutive indices; got 0 and 2",
        ):
            _ = assign_between(df, "a", "c", Series(9, name="z"))


class TestCheckIndex:
    def test_main(self) -> None:
        check_index(RangeIndex(1))

    def test_length_pass(self) -> None:
        check_index(RangeIndex(1), length=1)

    def test_length_error(self) -> None:
        with raises(
            CheckIndexError, match=r"Index .* must satisfy the length requirements\."
        ):
            check_index(RangeIndex(1), length=2)

    def test_min_length_pass(self) -> None:
        check_index(RangeIndex(2), min_length=1)

    def test_min_length_error(self) -> None:
        with raises(
            CheckIndexError, match=r"Index .* must satisfy the length requirements\."
        ):
            check_index(RangeIndex(0), min_length=1)

    def test_max_length_pass(self) -> None:
        check_index(RangeIndex(0), max_length=1)

    def test_max_length_error(self) -> None:
        with raises(
            CheckIndexError, match=r"Index .* must satisfy the length requirements\."
        ):
            check_index(RangeIndex(2), max_length=1)

    def test_name_pass(self) -> None:
        check_index(RangeIndex(0), name=None)

    def test_name_error(self) -> None:
        with raises(
            CheckIndexError, match=r"Index .* must satisfy the name requirement\."
        ):
            check_index(RangeIndex(0), name="name")

    def test_sorted_pass(self) -> None:
        check_index(Index(["A", "B"]), sorted=True)

    def test_sorted_error(self) -> None:
        with raises(CheckIndexError, match=r"Index .* must be sorted\."):
            check_index(Index(["B", "A"]), sorted=True)

    def test_unique_pass(self) -> None:
        check_index(Index(["A", "B"]), unique=True)

    def test_unique_error(self) -> None:
        with raises(CheckIndexError, match=r"Index .* must be unique\."):
            check_index(Index(["A", "A"]), unique=True)


class TestCheckPandasDataFrame:
    def test_main(self) -> None:
        check_pandas_dataframe(DataFrame())

    def test_columns_pass(self) -> None:
        df = DataFrame(0.0, index=RangeIndex(0), columns=Index([]))
        check_pandas_dataframe(df, columns=[])

    def test_columns_error(self) -> None:
        df = DataFrame(0.0, index=RangeIndex(0), columns=Index(["value"]))
        with raises(
            CheckPandasDataFrameError,
            match=re.compile(
                r"DataFrame must have columns .*; got .*\n\n.*\.", flags=DOTALL
            ),
        ):
            check_pandas_dataframe(df, columns=["other"])

    def test_dtypes_pass(self) -> None:
        df = DataFrame(0.0, index=RangeIndex(0), columns=Index([]))
        check_pandas_dataframe(df, dtypes={})

    def test_dtypes_error_set_of_columns(self) -> None:
        df = DataFrame(0.0, index=RangeIndex(0), columns=Index([]))
        with raises(
            CheckPandasDataFrameError,
            match=re.compile(
                r"DataFrame must have dtypes .*; got .*\n\n.*\.", flags=DOTALL
            ),
        ):
            check_pandas_dataframe(df, dtypes={"value": int})

    def test_dtypes_error_order_of_columns(self) -> None:
        df = DataFrame(0.0, index=RangeIndex(0), columns=Index(["a", "b"]))
        with raises(
            CheckPandasDataFrameError,
            match=re.compile(
                r"DataFrame must have dtypes .*; got .*\n\n.*\.", flags=DOTALL
            ),
        ):
            check_pandas_dataframe(df, dtypes={"b": float, "a": float})

    def test_length_pass(self) -> None:
        df = DataFrame(0.0, index=RangeIndex(1), columns=Index(["value"]))
        check_pandas_dataframe(df, length=1)

    def test_length_error(self) -> None:
        df = DataFrame(0.0, index=RangeIndex(1), columns=Index(["value"]))
        with raises(
            CheckPandasDataFrameError,
            match=re.compile(
                r"DataFrame must satisfy the length requirements; got .*\n\n.*\.",
                flags=DOTALL,
            ),
        ):
            check_pandas_dataframe(df, length=2)

    def test_min_length_pass(self) -> None:
        df = DataFrame(0.0, index=RangeIndex(2), columns=Index(["value"]))
        check_pandas_dataframe(df, min_length=1)

    def test_min_length_error(self) -> None:
        df = DataFrame(0.0, index=RangeIndex(0), columns=Index(["value"]))
        with raises(
            CheckPandasDataFrameError,
            match=re.compile(
                r"DataFrame must satisfy the length requirements; got .*\n\n.*\.",
                flags=DOTALL,
            ),
        ):
            check_pandas_dataframe(df, min_length=1)

    def test_max_length_pass(self) -> None:
        df = DataFrame(0.0, index=RangeIndex(0), columns=Index(["value"]))
        check_pandas_dataframe(df, max_length=1)

    def test_max_length_error(self) -> None:
        df = DataFrame(0.0, index=RangeIndex(2), columns=Index(["value"]))
        with raises(
            CheckPandasDataFrameError,
            match=re.compile(
                r"DataFrame must satisfy the length requirements; got .*\n\n.*\.",
                flags=DOTALL,
            ),
        ):
            check_pandas_dataframe(df, max_length=1)

    def test_sorted_pass(self) -> None:
        df = DataFrame([[0.0], [1.0]], index=RangeIndex(2), columns=Index(["value"]))
        check_pandas_dataframe(df, sorted="value")

    def test_sorted_error(self) -> None:
        df = DataFrame([[1.0], [0.0]], index=RangeIndex(2), columns=Index(["value"]))
        with raises(
            CheckPandasDataFrameError,
            match=re.compile(r"DataFrame must be sorted on .*\n\n.*\.", flags=DOTALL),
        ):
            check_pandas_dataframe(df, sorted="value")

    def test_standard_pass(self) -> None:
        check_pandas_dataframe(DataFrame(index=RangeIndex(0)), standard=True)

    @mark.parametrize(
        "df",
        [
            param(DataFrame(0.0, index=Index(["A"]), columns=Index(["value"]))),
            param(DataFrame(0.0, index=RangeIndex(1, 2), columns=Index(["value"]))),
            param(
                DataFrame(0.0, index=RangeIndex(1, step=2), columns=Index(["value"]))
            ),
            param(
                DataFrame(
                    0.0, index=RangeIndex(1, name="name"), columns=Index(["value"])
                )
            ),
        ],
    )
    def test_standard_errors_index(self, *, df: DataFrame) -> None:
        with raises(
            CheckPandasDataFrameError,
            match=re.compile(
                r"DataFrame must have a standard index; got .*\n\n.*\.", flags=DOTALL
            ),
        ):
            check_pandas_dataframe(df, standard=True)

    @mark.parametrize(
        "df",
        [
            param(
                DataFrame(
                    0.0, index=RangeIndex(1), columns=Index(["value"], name="name")
                )
            ),
            param(
                DataFrame(0.0, index=RangeIndex(1), columns=Index(["value", "value"]))
            ),
        ],
    )
    def test_standard_errors(self, *, df: DataFrame) -> None:
        with raises(
            CheckPandasDataFrameError,
            match=re.compile(
                r"DataFrame must have standard columns; got .*\n\n.*\.", flags=DOTALL
            ),
        ):
            check_pandas_dataframe(df, standard=True)

    def test_unique_pass(self) -> None:
        df = DataFrame([[0.0], [1.0]], index=RangeIndex(2), columns=Index(["value"]))
        check_pandas_dataframe(df, unique="value")

    def test_unique_error(self) -> None:
        df = DataFrame(0.0, index=RangeIndex(2), columns=Index(["value"]))
        with raises(
            CheckPandasDataFrameError,
            match=re.compile(r"DataFrame must be unique on .*\n\n.*\.", flags=DOTALL),
        ):
            check_pandas_dataframe(df, unique="value")

    def test_width_pass(self) -> None:
        df = DataFrame()
        check_pandas_dataframe(df, width=0)

    def test_width_error(self) -> None:
        df = DataFrame()
        with raises(
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
        with raises(CheckRangeIndexError):
            check_range_index(RangeIndex(0), start=1)

    def test_stop_pass(self) -> None:
        check_range_index(RangeIndex(0), stop=0)

    def test_stop_error(self) -> None:
        with raises(CheckRangeIndexError):
            check_range_index(RangeIndex(0), stop=1)

    def test_step_pass(self) -> None:
        check_range_index(RangeIndex(0), step=1)

    def test_step_error(self) -> None:
        with raises(CheckRangeIndexError):
            check_range_index(RangeIndex(0), step=2)

    def test_length_pass(self) -> None:
        check_range_index(RangeIndex(1), length=1)

    def test_length_error(self) -> None:
        with raises(CheckRangeIndexError):
            check_range_index(RangeIndex(1), length=2)

    def test_min_length_pass(self) -> None:
        check_range_index(RangeIndex(2), min_length=1)

    def test_min_length_error(self) -> None:
        with raises(CheckRangeIndexError):
            check_range_index(RangeIndex(0), min_length=1)

    def test_max_length_pass(self) -> None:
        check_range_index(RangeIndex(0), max_length=1)

    def test_max_length_error(self) -> None:
        with raises(CheckRangeIndexError):
            check_range_index(RangeIndex(2), max_length=1)

    def test_name_pass(self) -> None:
        check_range_index(RangeIndex(0), name=None)

    def test_name_error(self) -> None:
        with raises(CheckRangeIndexError):
            check_range_index(RangeIndex(0), name="name")


class TestDTypes:
    @mark.parametrize("dtype", [param(Int64), param(boolean), param(string)])
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
        with raises(
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
        with raises(ReindexToSubSetError, match=r"Index .* must be a superset of .*\."):
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
        with raises(ReindexToSuperSetError, match=r"Index .* must be a subset of .*\."):
            _ = reindex_to_superset(index, target)


class TestRedirectEmptyPandasConcat:
    def test_main(self) -> None:
        with raises(EmptyPandasConcatError), redirect_empty_pandas_concat():
            _ = concat([])


class TestSeriesMinMax:
    @mark.parametrize(
        ("x_v", "y_v", "dtype", "expected_min_v", "expected_max_v"),
        [
            param(0.0, 1.0, float, 0.0, 1.0),
            param(0.0, nan, float, 0.0, 0.0),
            param(nan, 1.0, float, 1.0, 1.0),
            param(nan, nan, float, nan, nan),
            param(0, 1, Int64, 0, 1),
            param(0, NA, Int64, 0, 0),
            param(NA, 1, Int64, 1, 1),
            param(NA, NA, Int64, NA, NA),
            param(
                TIMESTAMP_MIN_AS_DATE,
                TIMESTAMP_MAX_AS_DATE,
                datetime64ns,
                TIMESTAMP_MIN_AS_DATE,
                TIMESTAMP_MAX_AS_DATE,
            ),
            param(
                TIMESTAMP_MIN_AS_DATE,
                NaT,
                datetime64ns,
                TIMESTAMP_MIN_AS_DATE,
                TIMESTAMP_MIN_AS_DATE,
            ),
            param(
                NaT,
                TIMESTAMP_MAX_AS_DATE,
                datetime64ns,
                TIMESTAMP_MAX_AS_DATE,
                TIMESTAMP_MAX_AS_DATE,
            ),
            param(NaT, NaT, datetime64ns, NaT, NaT),
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

    @mark.parametrize("func", [param(series_min), param(series_max)])
    def test_different_index(
        self, *, func: Callable[[SeriesA, SeriesA], SeriesA]
    ) -> None:
        x = Series(data=nan, index=Index([0], dtype=int))
        y = Series(data=nan, index=Index([1], dtype=int))
        with raises(AssertionError):
            _ = func(x, y)

    @mark.parametrize("func", [param(series_min), param(series_max)])
    def test_error(self, *, func: Callable[[SeriesA, SeriesA], SeriesA]) -> None:
        x = Series(data=nan, dtype=float)
        y = Series(data=NA, dtype=Int64)
        with raises(
            SeriesMinMaxError,
            match=re.compile(
                r"Series .* and .* must have the same dtype; got .* and .*\.",
                flags=DOTALL,
            ),
        ):
            _ = func(x, y)


class TestTimestampMinMaxAsDate:
    def test_min(self) -> None:
        date = TIMESTAMP_MIN_AS_DATE
        assert isinstance(to_datetime(cast(Timestamp, date)), Timestamp)
        with raises(ValueError, match="Out of bounds nanosecond timestamp"):
            _ = to_datetime(cast(Timestamp, date - dt.timedelta(days=1)))

    def test_max(self) -> None:
        date = TIMESTAMP_MAX_AS_DATE
        assert isinstance(to_datetime(cast(Timestamp, date)), Timestamp)
        with raises(ValueError, match="Out of bounds nanosecond timestamp"):
            _ = to_datetime(cast(Timestamp, date + dt.timedelta(days=1)))


class TestTimestampMinMaxAsDateTime:
    def test_min(self) -> None:
        date = TIMESTAMP_MIN_AS_DATETIME
        assert isinstance(to_datetime(date), Timestamp)
        with raises(ValueError, match="Out of bounds nanosecond timestamp"):
            _ = to_datetime(date - dt.timedelta(microseconds=1))

    def test_max(self) -> None:
        date = TIMESTAMP_MAX_AS_DATETIME
        assert isinstance(to_datetime(date), Timestamp)
        with raises(ValueError, match="Out of bounds nanosecond timestamp"):
            _ = to_datetime(date + dt.timedelta(microseconds=1))


class TestTimestampToDate:
    @mark.parametrize(
        ("timestamp", "expected"),
        [
            param(to_datetime("2000-01-01"), dt.date(2000, 1, 1)),
            param(to_datetime("2000-01-01 12:00:00"), dt.date(2000, 1, 1)),
        ],
    )
    def test_main(self, *, timestamp: Any, expected: dt.date) -> None:
        assert timestamp_to_date(timestamp) == expected

    def test_error(self) -> None:
        with raises(TimestampToDateTimeError):
            _ = timestamp_to_date(NaT)


class TestTimestampToDateTime:
    @mark.parametrize(
        ("timestamp", "expected"),
        [
            param(to_datetime("2000-01-01"), dt.datetime(2000, 1, 1, tzinfo=UTC)),
            param(
                to_datetime("2000-01-01 12:00:00"),
                dt.datetime(2000, 1, 1, 12, tzinfo=UTC),
            ),
            param(
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
        with raises(UserWarning, match="Discarding nonzero nanoseconds in conversion"):
            _ = timestamp_to_datetime(timestamp)

    def test_error(self) -> None:
        with raises(TimestampToDateTimeError):
            _ = timestamp_to_datetime(NaT)


class TestToNumpy:
    @mark.parametrize(
        ("series_v", "series_d", "array_v", "array_d"),
        [
            param(True, bool, True, bool),
            param(False, bool, False, bool),
            param(True, boolean, True, object),
            param(False, boolean, False, object),
            param(NA, boolean, None, object),
            param(TODAY_UTC, datetime64ns, TODAY_UTC, datetime64ns),
            param(0, int, 0, int),
            param(0, Int64, 0, object),
            param(NA, Int64, None, object),
            param(nan, float, nan, float),
            param("", string, "", object),
            param(NA, string, None, object),
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
        with raises(
            UnionIndexesError,
            match=r"Indexes .* and .* must have the same name; got .* and .*\.",
        ):
            _ = union_indexes(left, right, names="raise")
