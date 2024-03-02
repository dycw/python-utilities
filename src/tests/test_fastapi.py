from __future__ import annotations

from pytest import mark, param, raises
from typing_extensions import Self

from utilities.fastapi import APIRouter


class TestAPIRouter:
    @mark.parametrize("route", [param("/"), param("/home")])
    def test_main(self: Self, route: str) -> None:
        router = APIRouter()

        @router.get(route)
        def _() -> None:
            return None

    def test_error(self: Self) -> None:
        router = APIRouter()

        with raises(ValueError, match="Invalid route"):

            @router.get("/home/")
            def _() -> None:
                return None
