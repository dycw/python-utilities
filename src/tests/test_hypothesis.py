from __future__ import annotations

from itertools import pairwise
from pathlib import Path
from re import search
from subprocess import PIPE, check_output
from typing import TYPE_CHECKING, Any, cast

from hypothesis import HealthCheck, Phase, assume, given, settings
from hypothesis.errors import InvalidArgument
from hypothesis.extra.numpy import array_shapes
from hypothesis.strategies import (
    DataObject,
    DrawFn,
    booleans,
    composite,
    data,
    dates,
    datetimes,
    floats,
    integers,
    just,
    none,
    sets,
)
from luigi import Task
from numpy import (
    iinfo,
    inf,
    int32,
    int64,
    isfinite,
    isinf,
    isnan,
    ravel,
    rint,
    uint32,
    uint64,
    zeros,
)
from pandas import Timestamp
from pandas.testing import assert_index_equal
from pytest import mark, param, raises
from semver import Version
from sqlalchemy import Column, Engine, Integer, MetaData, Table, select
from sqlalchemy.orm import declarative_base

from tests.conftest import FLAKY
from utilities.git import _GET_BRANCH_NAME
from utilities.hypothesis import (
    Shape,
    _merge_into_dict_of_indexes,
    assume_does_not_raise,
    bool_arrays,
    bool_data_arrays,
    concatenated_arrays,
    dates_pd,
    datetimes_pd,
    datetimes_utc,
    dicts_of_indexes,
    float_arrays,
    float_data_arrays,
    floats_extra,
    git_repos,
    hashables,
    indexes,
    int32s,
    int64s,
    int_arrays,
    int_data_arrays,
    int_indexes,
    lists_fixed_length,
    namespace_mixins,
    settings_with_reduced_examples,
    setup_hypothesis_profiles,
    slices,
    sqlite_engines,
    str_arrays,
    str_data_arrays,
    str_indexes,
    temp_dirs,
    temp_paths,
    text_ascii,
    text_clean,
    text_printable,
    timestamps,
    uint32s,
    uint64s,
    versions,
)
from utilities.os import temp_environ
from utilities.pandas import (
    TIMESTAMP_MAX_AS_DATE,
    TIMESTAMP_MAX_AS_DATETIME,
    TIMESTAMP_MIN_AS_DATE,
    TIMESTAMP_MIN_AS_DATETIME,
    IndexA,
    string,
)
from utilities.pathvalidate import valid_path
from utilities.platform import maybe_yield_lower_case
from utilities.sqlalchemy import get_table, insert_items
from utilities.zoneinfo import UTC

if TYPE_CHECKING:
    import datetime as dt
    from collections.abc import Hashable, Mapping
    from collections.abc import Set as AbstractSet

    from utilities.tempfile import TemporaryDirectory


class TestAssumeDoesNotRaise:
    @given(x=booleans())
    def test_no_match_and_suppressed(self, *, x: bool) -> None:
        with assume_does_not_raise(ValueError):
            if x is True:
                msg = "x is True"
                raise ValueError(msg)
        assert x is False

    @given(x=booleans())
    def test_no_match_and_not_suppressed(self, *, x: bool) -> None:
        msg = "x is True"
        if x is True:
            with raises(ValueError, match=msg), assume_does_not_raise(RuntimeError):
                raise ValueError(msg)

    @given(x=booleans())
    def test_with_match_and_suppressed(self, *, x: bool) -> None:
        msg = "x is True"
        if x is True:
            with assume_does_not_raise(ValueError, match=msg):
                raise ValueError(msg)
        assert x is False

    @given(x=just(value=True))
    def test_with_match_and_not_suppressed(self, *, x: bool) -> None:
        msg = "x is True"
        if x is True:
            with (
                raises(ValueError, match=msg),
                assume_does_not_raise(ValueError, match="wrong"),
            ):
                raise ValueError(msg)


class TestBoolArrays:
    @given(data=data(), shape=array_shapes())
    def test_main(self, *, data: DataObject, shape: Shape) -> None:
        array = data.draw(bool_arrays(shape=shape))
        assert array.dtype == bool
        assert array.shape == shape


class TestBoolDataArrays:
    @given(data=data(), indexes=dicts_of_indexes(), name=text_ascii() | none())
    def test_main(
        self, *, data: DataObject, indexes: Mapping[str, IndexA], name: str | None
    ) -> None:
        array = data.draw(bool_data_arrays(indexes, name=name))
        assert set(array.coords) == set(indexes)
        assert array.dims == tuple(indexes)
        assert array.dtype == bool
        assert array.name == name
        for arr, exp in zip(array.indexes.values(), indexes.values(), strict=True):
            assert_index_equal(arr, exp, check_names=False)


class TestConcatenatedArrays:
    @given(data=data(), m=integers(0, 10), n=integers(0, 10))
    def test_1d(self, *, data: DataObject, m: int, n: int) -> None:
        arrays = just(zeros(n, dtype=float))
        array = data.draw(concatenated_arrays(arrays, m, n))
        assert array.shape == (m, n)

    @given(data=data(), m=integers(0, 10), n=integers(0, 10), p=integers(0, 10))
    def test_2d(self, *, data: DataObject, m: int, n: int, p: int) -> None:
        arrays = just(zeros((n, p), dtype=float))
        array = data.draw(concatenated_arrays(arrays, m, (n, p)))
        assert array.shape == (m, n, p)


class TestDatesPd:
    @given(
        data=data(),
        min_value=dates(min_value=TIMESTAMP_MIN_AS_DATE),
        max_value=dates(max_value=TIMESTAMP_MAX_AS_DATE),
    )
    @settings(suppress_health_check={HealthCheck.filter_too_much})
    def test_main(
        self, *, data: DataObject, min_value: dt.date, max_value: dt.date
    ) -> None:
        _ = assume(min_value <= max_value)
        date = data.draw(dates_pd(min_value=min_value, max_value=max_value))
        _ = Timestamp(date)
        assert min_value <= date <= max_value


class TestDatetimesPd:
    @given(
        data=data(),
        min_value=datetimes_utc(min_value=TIMESTAMP_MIN_AS_DATETIME),
        max_value=datetimes_utc(max_value=TIMESTAMP_MAX_AS_DATETIME),
    )
    @settings(suppress_health_check={HealthCheck.filter_too_much})
    def test_main(
        self, *, data: DataObject, min_value: dt.datetime, max_value: dt.datetime
    ) -> None:
        _ = assume(min_value <= max_value)
        datetime = data.draw(datetimes_pd(min_value=min_value, max_value=max_value))
        _ = Timestamp(datetime)
        assert min_value <= datetime <= max_value


class TestDatetimesUTC:
    @given(data=data(), min_value=datetimes(), max_value=datetimes())
    def test_main(
        self, *, data: DataObject, min_value: dt.datetime, max_value: dt.datetime
    ) -> None:
        min_value, max_value = (v.replace(tzinfo=UTC) for v in [min_value, max_value])
        _ = assume(min_value <= max_value)
        datetime = data.draw(datetimes_utc(min_value=min_value, max_value=max_value))
        assert min_value <= datetime <= max_value


class TestDictsOfIndexes:
    @given(
        data=data(),
        min_dims=integers(1, 3),
        max_dims=integers(1, 3) | none(),
        min_side=integers(1, 10),
        max_side=integers(1, 10) | none(),
    )
    def test_main(
        self,
        *,
        data: DataObject,
        min_dims: int,
        max_dims: int | None,
        min_side: int,
        max_side: int | None,
    ) -> None:
        with assume_does_not_raise(InvalidArgument):
            indexes = data.draw(
                dicts_of_indexes(
                    min_dims=min_dims,
                    max_dims=max_dims,
                    min_side=min_side,
                    max_side=max_side,
                )
            )
        ndims = len(indexes)
        assert ndims >= min_dims
        if max_dims is not None:
            assert ndims <= max_dims
        for index in indexes.values():
            length = len(index)
            assert length >= min_side
            if max_side is not None:
                assert length <= max_side


class TestFloatArrays:
    @given(
        data=data(),
        shape=array_shapes(),
        min_value=floats() | none(),
        max_value=floats() | none(),
        allow_nan=booleans(),
        allow_inf=booleans(),
        allow_pos_inf=booleans(),
        allow_neg_inf=booleans(),
        integral=booleans(),
        unique=booleans(),
    )
    def test_main(
        self,
        *,
        data: DataObject,
        shape: Shape,
        min_value: float | None,
        max_value: float | None,
        allow_nan: bool,
        allow_inf: bool,
        allow_pos_inf: bool,
        allow_neg_inf: bool,
        integral: bool,
        unique: bool,
    ) -> None:
        with assume_does_not_raise(InvalidArgument):
            array = data.draw(
                float_arrays(
                    shape=shape,
                    min_value=min_value,
                    max_value=max_value,
                    allow_nan=allow_nan,
                    allow_inf=allow_inf,
                    allow_pos_inf=allow_pos_inf,
                    allow_neg_inf=allow_neg_inf,
                    integral=integral,
                    unique=unique,
                )
            )
        assert array.dtype == float
        assert array.shape == shape
        if min_value is not None:
            assert ((isfinite(array) & (array >= min_value)) | ~isfinite(array)).all()
        if max_value is not None:
            assert ((isfinite(array) & (array <= max_value)) | ~isfinite(array)).all()
        if not allow_nan:
            assert (~isnan(array)).all()
        if not allow_inf:
            if not (allow_pos_inf or allow_neg_inf):
                assert (~isinf(array)).all()
            if not allow_pos_inf:
                assert (array != inf).all()
            if not allow_neg_inf:
                assert (array != -inf).all()
        if integral:
            assert ((array == rint(array)) | isnan(array)).all()
        if unique:
            flat = ravel(array)
            assert len(set(flat)) == len(flat)


class TestFloatDataArrays:
    @given(
        data=data(),
        indexes=dicts_of_indexes(),
        min_value=floats() | none(),
        max_value=floats() | none(),
        allow_nan=booleans(),
        allow_inf=booleans(),
        allow_pos_inf=booleans(),
        allow_neg_inf=booleans(),
        integral=booleans(),
        unique=booleans(),
        name=text_ascii() | none(),
    )
    def test_main(
        self,
        *,
        data: DataObject,
        indexes: Mapping[str, IndexA],
        min_value: float | None,
        max_value: float | None,
        allow_nan: bool,
        allow_inf: bool,
        allow_pos_inf: bool,
        allow_neg_inf: bool,
        integral: bool,
        unique: bool,
        name: str | None,
    ) -> None:
        with assume_does_not_raise(InvalidArgument):
            array = data.draw(
                float_data_arrays(
                    indexes,
                    min_value=min_value,
                    max_value=max_value,
                    allow_nan=allow_nan,
                    allow_inf=allow_inf,
                    allow_pos_inf=allow_pos_inf,
                    allow_neg_inf=allow_neg_inf,
                    integral=integral,
                    unique=unique,
                    name=name,
                )
            )
        assert set(array.coords) == set(indexes)
        assert array.dims == tuple(indexes)
        assert array.dtype == float
        assert array.name == name
        for arr, exp in zip(array.indexes.values(), indexes.values(), strict=True):
            assert_index_equal(arr, exp, check_names=False)


class TestFloatsExtra:
    @given(
        data=data(),
        min_value=floats() | none(),
        max_value=floats() | none(),
        allow_nan=booleans(),
        allow_inf=booleans(),
        allow_pos_inf=booleans(),
        allow_neg_inf=booleans(),
        integral=booleans(),
    )
    def test_main(
        self,
        *,
        data: DataObject,
        min_value: float | None,
        max_value: float | None,
        allow_nan: bool,
        allow_inf: bool,
        allow_pos_inf: bool,
        allow_neg_inf: bool,
        integral: bool,
    ) -> None:
        with assume_does_not_raise(InvalidArgument):
            x = data.draw(
                floats_extra(
                    min_value=min_value,
                    max_value=max_value,
                    allow_nan=allow_nan,
                    allow_inf=allow_inf,
                    allow_pos_inf=allow_pos_inf,
                    allow_neg_inf=allow_neg_inf,
                    integral=integral,
                )
            )
        if min_value is not None:
            assert (isfinite(x) and x >= min_value) or not isfinite(x)
        if max_value is not None:
            assert (isfinite(x) and x <= max_value) or not isfinite(x)
        if not allow_nan:
            assert not isnan(x)
        if not allow_inf:
            if not (allow_pos_inf or allow_neg_inf):
                assert not isinf(x)
            if not allow_pos_inf:
                assert x != inf
            if not allow_neg_inf:
                assert x != -inf
        if integral:
            assert (isfinite(x) and x == round(x)) or not isfinite(x)

    @given(data=data(), min_value=floats() | none(), max_value=floats() | none())
    def test_finite_and_integral(
        self, *, data: DataObject, min_value: float | None, max_value: float | None
    ) -> None:  # hard to reach
        with assume_does_not_raise(InvalidArgument):
            x = data.draw(
                floats_extra(
                    min_value=min_value,
                    max_value=max_value,
                    allow_nan=False,
                    allow_inf=False,
                    allow_pos_inf=False,
                    allow_neg_inf=False,
                    integral=True,
                )
            )
        assert isfinite(x)
        if min_value is not None:
            assert x >= min_value
        if max_value is not None:
            assert x <= max_value
        assert x == round(x)


class TestGitRepos:
    @given(data=data())
    @settings_with_reduced_examples(suppress_health_check={HealthCheck.filter_too_much})
    def test_main(self, *, data: DataObject) -> None:
        branch = data.draw(text_ascii(min_size=1) | none())
        path = data.draw(git_repos(branch=branch))
        assert set(path.iterdir()) == {Path(path, ".git")}
        if branch is not None:
            output = check_output(_GET_BRANCH_NAME, stderr=PIPE, cwd=path, text=True)
            assert output.strip("\n") == branch


class TestHashables:
    @given(data=data())
    def test_main(self, *, data: DataObject) -> None:
        x = data.draw(hashables())
        _ = hash(x)


class TestIndexes:
    @given(
        data=data(),
        n=integers(0, 10),
        unique=booleans(),
        name=hashables(),
        sort=booleans(),
    )
    def test_generic(
        self, *, data: DataObject, n: int, unique: bool, name: Hashable, sort: bool
    ) -> None:
        index = data.draw(
            indexes(
                elements=int64s(), dtype=int64, n=n, unique=unique, name=name, sort=sort
            )
        )
        assert len(index) == n
        if unique:
            assert not index.duplicated().any()
        assert index.name == name
        if sort:
            assert_index_equal(index, index.sort_values())

    @given(
        data=data(),
        n=integers(0, 10),
        unique=booleans(),
        name=hashables(),
        sort=booleans(),
    )
    def test_int(
        self, *, data: DataObject, n: int, unique: bool, name: Hashable, sort: bool
    ) -> None:
        index = data.draw(int_indexes(n=n, unique=unique, name=name, sort=sort))
        assert index.dtype == int64
        assert len(index) == n
        if unique:
            assert not index.duplicated().any()
        assert index.name == name
        if sort:
            assert_index_equal(index, index.sort_values())

    @given(
        data=data(),
        n=integers(0, 10),
        unique=booleans(),
        name=hashables(),
        sort=booleans(),
    )
    def test_str(
        self, *, data: DataObject, n: int, unique: bool, name: Hashable, sort: bool
    ) -> None:
        index = data.draw(str_indexes(n=n, unique=unique, name=name, sort=sort))
        assert index.dtype == string
        assert len(index) == n
        if unique:
            assert not index.duplicated().any()
        assert index.name == name
        if sort:
            assert_index_equal(index, index.sort_values())


class TestIntArrays:
    @given(
        data=data(),
        shape=array_shapes(),
        min_value=int64s() | none(),
        max_value=int64s() | none(),
        unique=booleans(),
    )
    def test_main(
        self,
        *,
        data: DataObject,
        shape: Shape,
        min_value: int | None,
        max_value: int | None,
        unique: bool,
    ) -> None:
        with assume_does_not_raise(InvalidArgument):
            array = data.draw(
                int_arrays(
                    shape=shape, min_value=min_value, max_value=max_value, unique=unique
                )
            )
        assert array.dtype == int64
        assert array.shape == shape
        if unique:
            flat = ravel(array)
            assert len(set(flat)) == len(flat)


class TestInt32s:
    @given(data=data(), min_value=int32s() | none(), max_value=int32s() | none())
    def test_main(
        self, *, data: DataObject, min_value: int | None, max_value: int | None
    ) -> None:
        with assume_does_not_raise(InvalidArgument):
            x = data.draw(int32s(min_value=min_value, max_value=max_value))
        info = iinfo(int32)
        assert info.min <= x <= info.max
        if min_value is not None:
            assert x >= min_value
        if max_value is not None:
            assert x <= max_value


class TestInt64s:
    @given(data=data(), min_value=int64s() | none(), max_value=int64s() | none())
    def test_main(
        self, *, data: DataObject, min_value: int | None, max_value: int | None
    ) -> None:
        with assume_does_not_raise(InvalidArgument):
            x = data.draw(int64s(min_value=min_value, max_value=max_value))
        info = iinfo(int64)
        assert info.min <= x <= info.max
        if min_value is not None:
            assert x >= min_value
        if max_value is not None:
            assert x <= max_value


class TestIntDataArrays:
    @given(
        data=data(),
        indexes=dicts_of_indexes(),
        min_value=int64s() | none(),
        max_value=int64s() | none(),
        unique=booleans(),
        name=text_ascii() | none(),
    )
    def test_main(
        self,
        *,
        data: DataObject,
        indexes: Mapping[str, IndexA],
        min_value: int | None,
        max_value: int | None,
        unique: bool,
        name: str | None,
    ) -> None:
        with assume_does_not_raise(InvalidArgument):
            array = data.draw(
                int_data_arrays(
                    indexes,
                    min_value=min_value,
                    max_value=max_value,
                    unique=unique,
                    name=name,
                )
            )
        assert set(array.coords) == set(indexes)
        assert array.dims == tuple(indexes)
        assert array.dtype == int64
        assert array.name == name
        for arr, exp in zip(array.indexes.values(), indexes.values(), strict=True):
            assert_index_equal(arr, exp, check_names=False)


class TestLiftDraw:
    @given(data=data(), x=booleans())
    def test_fixed(self, *, data: DataObject, x: bool) -> None:
        @composite
        def func(_draw: DrawFn, /) -> bool:
            _ = _draw(booleans())
            return x

        result = data.draw(func())
        assert result is x

    @given(data=data())
    def test_strategy(self, *, data: DataObject) -> None:
        @composite
        def func(_draw: DrawFn, /) -> bool:
            return _draw(booleans())

        result = data.draw(func())
        assert isinstance(result, bool)


class TestListsFixedLength:
    @given(data=data(), size=integers(1, 10))
    @mark.parametrize(
        "unique", [param(True, id="unique"), param(False, id="no unique")]
    )
    @mark.parametrize(
        "sorted_", [param(True, id="sorted"), param(False, id="no sorted")]
    )
    def test_main(
        self, *, data: DataObject, size: int, unique: bool, sorted_: bool
    ) -> None:
        result = data.draw(
            lists_fixed_length(integers(), size, unique=unique, sorted=sorted_)
        )
        assert isinstance(result, list)
        assert len(result) == size
        if unique:
            assert len(set(result)) == len(result)
        if sorted_:
            assert sorted(result) == result


class TestNamespaceMixins:
    @given(data=data())
    def test_main(self, *, data: DataObject) -> None:
        _ = data.draw(namespace_mixins())

    @given(namespace_mixin=namespace_mixins())
    def test_first(self, *, namespace_mixin: Any) -> None:
        class Example(namespace_mixin, Task): ...

        _ = Example()

    @given(namespace_mixin=namespace_mixins())
    def test_second(self, *, namespace_mixin: Any) -> None:
        class Example(namespace_mixin, Task): ...

        _ = Example()


class TestReducedExamples:
    @given(frac=floats(0.0, 10.0))
    def test_main(self, *, frac: float) -> None:
        @settings_with_reduced_examples(frac)
        def test() -> None:
            pass

        result = cast(Any, test)._hypothesis_internal_use_settings.max_examples  # noqa: SLF001
        expected = max(round(frac * settings().max_examples), 1)
        assert result == expected


class TestSlices:
    @given(data=data(), iter_len=integers(0, 10))
    def test_main(self, *, data: DataObject, iter_len: int) -> None:
        slice_len = data.draw(integers(0, iter_len) | none())
        slice_ = data.draw(slices(iter_len, slice_len=slice_len))
        range_slice = range(iter_len)[slice_]
        assert all(i + 1 == j for i, j in pairwise(range_slice))
        if slice_len is not None:
            assert len(range_slice) == slice_len

    @given(data=data(), iter_len=integers(0, 10))
    def test_error(self, *, data: DataObject, iter_len: int) -> None:
        with raises(
            InvalidArgument, match=r"Slice length \d+ exceeds iterable length \d+"
        ):
            _ = data.draw(slices(iter_len, slice_len=iter_len + 1))


class TestMergeIntoDictOfIndexes:
    @given(data=data())
    def test_empty(self, *, data: DataObject) -> None:
        _ = data.draw(_merge_into_dict_of_indexes())

    @given(
        data=data(), indexes1=dicts_of_indexes() | none(), indexes2=dicts_of_indexes()
    )
    def test_non_empty(
        self,
        *,
        data: DataObject,
        indexes1: Mapping[str, IndexA] | None,
        indexes2: Mapping[str, IndexA],
    ) -> None:
        indexes_ = data.draw(_merge_into_dict_of_indexes(indexes1, **indexes2))
        expected = (set() if indexes1 is None else set(indexes1)) | set(indexes2)
        assert set(indexes_) == expected


class TestSetupHypothesisProfiles:
    def test_main(self) -> None:
        setup_hypothesis_profiles()
        curr = settings()
        assert Phase.shrink in curr.phases
        assert curr.max_examples in {10, 100, 1000}

    def test_no_shrink(self) -> None:
        with temp_environ({"HYPOTHESIS_NO_SHRINK": "1"}):
            setup_hypothesis_profiles()
        assert Phase.shrink not in settings().phases

    @given(max_examples=integers(1, 100))
    def test_max_examples(self, *, max_examples: int) -> None:
        with temp_environ({"HYPOTHESIS_MAX_EXAMPLES": str(max_examples)}):
            setup_hypothesis_profiles()
        assert settings().max_examples == max_examples


class TestSQLiteEngines:
    @given(engine=sqlite_engines())
    def test_main(self, *, engine: Engine) -> None:
        assert isinstance(engine, Engine)
        database = engine.url.database
        assert database is not None
        assert not valid_path(database).exists()

    @given(data=data(), ids=sets(integers(0, 10)))
    def test_table(self, *, data: DataObject, ids: set[int]) -> None:
        metadata = MetaData()
        table = Table("example", metadata, Column("id_", Integer, primary_key=True))
        engine = data.draw(sqlite_engines(metadata=metadata))
        self._run_test(engine, table, ids)

    @given(data=data(), ids=sets(integers(0, 10)))
    def test_mapped_class(self, *, data: DataObject, ids: set[int]) -> None:
        Base = declarative_base()  # noqa: N806

        class Example(Base):
            __tablename__ = "example"

            id_ = Column(Integer, primary_key=True)

        engine = data.draw(sqlite_engines(base=Base))
        self._run_test(engine, Example, ids)

    def _run_test(
        self, engine: Engine, table_or_mapped_class: Table | type[Any], ids: set[int], /
    ) -> None:
        insert_items(engine, ([(id_,) for id_ in ids], table_or_mapped_class))
        sel = select(get_table(table_or_mapped_class).c["id_"])
        with engine.begin() as conn:
            res = conn.execute(sel).scalars().all()
        assert set(res) == ids


class TestStrArrays:
    @given(
        data=data(),
        shape=array_shapes(),
        min_size=integers(0, 100),
        max_size=integers(0, 100) | none(),
        allow_none=booleans(),
        unique=booleans(),
    )
    def test_main(
        self,
        *,
        data: DataObject,
        shape: Shape,
        min_size: int,
        max_size: int | None,
        allow_none: bool,
        unique: bool,
    ) -> None:
        with assume_does_not_raise(InvalidArgument):
            array = data.draw(
                str_arrays(
                    shape=shape,
                    min_size=min_size,
                    max_size=max_size,
                    allow_none=allow_none,
                    unique=unique,
                )
            )
        assert array.dtype == object
        assert array.shape == shape
        flat = ravel(array)
        flat_text = [i for i in flat if i is not None]
        assert all(len(t) >= min_size for t in flat_text)
        if max_size is not None:
            assert all(len(t) <= max_size for t in flat_text)
        if not allow_none:
            assert len(flat_text) == array.size
        if unique:
            flat = ravel(array)
            assert len(set(flat)) == len(flat)


class TestStrDataArrays:
    @given(
        data=data(),
        indexes=dicts_of_indexes(),
        min_size=integers(0, 100),
        max_size=integers(0, 100) | none(),
        allow_none=booleans(),
        unique=booleans(),
        name=text_ascii() | none(),
    )
    def test_main(
        self,
        *,
        data: DataObject,
        indexes: Mapping[str, IndexA],
        min_size: int,
        max_size: int | None,
        allow_none: bool,
        unique: bool,
        name: str | None,
    ) -> None:
        with assume_does_not_raise(InvalidArgument):
            array = data.draw(
                str_data_arrays(
                    indexes,
                    min_size=min_size,
                    max_size=max_size,
                    allow_none=allow_none,
                    unique=unique,
                    name=name,
                )
            )
        assert set(array.coords) == set(indexes)
        assert array.dims == tuple(indexes)
        assert array.dtype == object
        assert array.name == name
        for arr, exp in zip(array.indexes.values(), indexes.values(), strict=True):
            assert_index_equal(arr, exp, check_names=False)


class TestTempDirs:
    @given(temp_dir=temp_dirs())
    def test_main(self, *, temp_dir: TemporaryDirectory) -> None:
        path = temp_dir.path
        assert path.is_dir()
        assert len(set(path.iterdir())) == 0

    @FLAKY
    @given(temp_dir=temp_dirs(), contents=sets(text_ascii(min_size=1), max_size=10))
    def test_writing_files(
        self, *, temp_dir: TemporaryDirectory, contents: AbstractSet[str]
    ) -> None:
        path = temp_dir.path
        assert len(set(path.iterdir())) == 0
        as_set = set(maybe_yield_lower_case(contents))
        for content in as_set:
            Path(path, content).touch()
        assert len(set(path.iterdir())) == len(as_set)


class TestTempPaths:
    @given(path=temp_paths())
    def test_main(self, *, path: Path) -> None:
        assert path.is_dir()
        assert len(set(path.iterdir())) == 0

    @given(path=temp_paths(), contents=sets(text_ascii(min_size=1), max_size=10))
    def test_writing_files(self, *, path: Path, contents: AbstractSet[str]) -> None:
        assert len(set(path.iterdir())) == 0
        as_set = set(maybe_yield_lower_case(contents))
        for content in as_set:
            Path(path, content).touch()
        assert len(set(path.iterdir())) == len(as_set)


class TestTextAscii:
    @given(
        data=data(),
        min_size=integers(0, 100),
        max_size=integers(0, 100) | none(),
        disallow_na=booleans(),
    )
    def test_main(
        self,
        *,
        data: DataObject,
        min_size: int,
        max_size: int | None,
        disallow_na: bool,
    ) -> None:
        with assume_does_not_raise(InvalidArgument, AssertionError):
            text = data.draw(
                text_ascii(
                    min_size=min_size, max_size=max_size, disallow_na=disallow_na
                )
            )
        assert search("^[A-Za-z]*$", text)
        assert len(text) >= min_size
        if max_size is not None:
            assert len(text) <= max_size
        if disallow_na:
            assert text != "NA"


class TestTextClean:
    @given(
        data=data(),
        min_size=integers(0, 100),
        max_size=integers(0, 100) | none(),
        disallow_na=booleans(),
    )
    def test_main(
        self,
        *,
        data: DataObject,
        min_size: int,
        max_size: int | None,
        disallow_na: bool,
    ) -> None:
        with assume_does_not_raise(InvalidArgument, AssertionError):
            text = data.draw(
                text_clean(
                    min_size=min_size, max_size=max_size, disallow_na=disallow_na
                )
            )
        assert search("^\\S[^\\r\\n]*$|^$", text)
        assert len(text) >= min_size
        if max_size is not None:
            assert len(text) <= max_size
        if disallow_na:
            assert text != "NA"


class TestTextPrintable:
    @given(
        data=data(),
        min_size=integers(0, 100),
        max_size=integers(0, 100) | none(),
        disallow_na=booleans(),
    )
    def test_main(
        self,
        *,
        data: DataObject,
        min_size: int,
        max_size: int | None,
        disallow_na: bool,
    ) -> None:
        with assume_does_not_raise(InvalidArgument, AssertionError):
            text = data.draw(
                text_printable(
                    min_size=min_size, max_size=max_size, disallow_na=disallow_na
                )
            )
        assert search(r"^[0-9A-Za-z!\"#$%&'()*+,-./:;<=>?@\[\\\]^_`{|}~\s]*$", text)
        assert len(text) >= min_size
        if max_size is not None:
            assert len(text) <= max_size
        if disallow_na:
            assert text != "NA"


class TestTimestamps:
    @given(
        data=data(),
        min_value=datetimes_utc(min_value=TIMESTAMP_MIN_AS_DATETIME),
        max_value=datetimes_utc(max_value=TIMESTAMP_MAX_AS_DATETIME),
        allow_nanoseconds=booleans(),
    )
    @settings(suppress_health_check={HealthCheck.filter_too_much})
    def test_main(
        self,
        *,
        data: DataObject,
        min_value: dt.datetime,
        max_value: dt.datetime,
        allow_nanoseconds: bool,
    ) -> None:
        _ = assume(min_value <= max_value)
        timestamp = data.draw(
            timestamps(
                min_value=min_value,
                max_value=max_value,
                allow_nanoseconds=allow_nanoseconds,
            )
        )
        assert min_value <= timestamp <= max_value
        if not allow_nanoseconds:
            assert cast(Any, timestamp).nanosecond == 0


class TestUInt32s:
    @given(data=data(), min_value=uint32s() | none(), max_value=uint32s() | none())
    def test_main(
        self, *, data: DataObject, min_value: int | None, max_value: int | None
    ) -> None:
        with assume_does_not_raise(InvalidArgument):
            x = data.draw(uint32s(min_value=min_value, max_value=max_value))
        info = iinfo(uint32)
        assert info.min <= x <= info.max
        if min_value is not None:
            assert x >= min_value
        if max_value is not None:
            assert x <= max_value


class TestUInt64s:
    @given(data=data(), min_value=uint64s() | none(), max_value=uint64s() | none())
    def test_main(
        self, *, data: DataObject, min_value: int | None, max_value: int | None
    ) -> None:
        with assume_does_not_raise(InvalidArgument):
            x = data.draw(uint64s(min_value=min_value, max_value=max_value))
        info = iinfo(uint64)
        assert info.min <= x <= info.max
        if min_value is not None:
            assert x >= min_value
        if max_value is not None:
            assert x <= max_value


class TestVersions:
    @given(data=data())
    def test_main(self, data: DataObject) -> None:
        version = data.draw(versions())
        assert isinstance(version, Version)

    @given(data=data())
    def test_min_version(self, data: DataObject) -> None:
        min_version = data.draw(versions())
        version = data.draw(versions(min_version=min_version))
        assert version >= min_version

    @given(data=data())
    def test_max_version(self, data: DataObject) -> None:
        max_version = data.draw(versions())
        version = data.draw(versions(max_version=max_version))
        assert version <= max_version

    @given(data=data())
    def test_min_and_max_version(self, data: DataObject) -> None:
        version1, version2 = data.draw(lists_fixed_length(versions(), 2))
        min_version = min(version1, version2)
        max_version = max(version1, version2)
        version = data.draw(versions(min_version=min_version, max_version=max_version))
        assert min_version <= version <= max_version

    @given(data=data())
    def test_error(self, data: DataObject) -> None:
        version1, version2 = data.draw(lists_fixed_length(versions(), 2))
        _ = assume(version1 != version2)
        with raises(InvalidArgument):
            _ = data.draw(
                versions(
                    min_version=max(version1, version2),
                    max_version=min(version1, version2),
                )
            )
