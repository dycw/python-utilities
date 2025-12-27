from __future__ import annotations

from typing import TYPE_CHECKING

from hypothesis import HealthCheck, Phase, given, reproduce_failure, settings
from pytest import RaisesGroup, approx, fixture, mark, param, raises, skip

from utilities.contextvars import set_global_breakpoint
from utilities.permissions import Permissions

if TYPE_CHECKING:
    from pytest_benchmark.fixture import BenchmarkFixture
    from pytest_lazy_fixtures import lf
    from pytest_regressions.dataframe_regression import DataFrameRegressionFixture

_CASES: list[tuple[Permissions, int, int, str]] = [
    (Permissions(), 0, 1, "u=,g=,o="),
    (Permissions(user_read=True), 400, 1, "u=r,g=,o="),
    (Permissions(group_write=True), 20, 1, "u=,g=w,o="),
    (Permissions(others_execute=True), 1, 1, "u=,g=,o=x"),
    (Permissions(user_read=True, user_write=True), 600, 2, "u=rw,g=,o="),
    (Permissions(user_read=True, group_execute=True), 410, 2, "u=rw,g=,o="),
]


class TestPermissions:
    @mark.parametrize(
        ("perms", "expected"),
        [param(perms, expected) for perms, expected, _, _ in _CASES],
    )
    def test_to_and_from_int(self, *, perms: Permissions, expected: str) -> None:
        result = int(perms)
        assert result == expected
        assert Permissions.from_int(result) == perms

    @mark.parametrize(
        ("perms", "expected"),
        [param(perms, expected) for perms, _, _, expected in _CASES],
    )
    def test_to_and_from_str(self, *, perms: Permissions, expected: str) -> None:
        result = str(perms)
        assert result == expected
        assert Permissions.from_str(result) == perms
