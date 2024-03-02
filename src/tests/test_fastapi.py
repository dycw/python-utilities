from __future__ import annotations

import pytest

from utilities.fastapi import APIRouter


class TestAPIRouter:
    @pytest.mark.parametrize("route", [pytest.param("/"), pytest.param("/home")])
    def test_main(self, route: str) -> None:
        router = APIRouter()

        @router.get(route)
        def _() -> None:
            return None

    def test_error(self) -> None:
        router = APIRouter()

        with pytest.raises(ValueError, match="Invalid route"):

            @router.get("/home/")
            def _() -> None:
                return None
