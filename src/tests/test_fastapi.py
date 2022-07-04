from pytest import mark
from pytest import param
from pytest import raises

from utilities.fastapi import APIRouter


class TestAPIRouter:
    @mark.parametrize("route", [param("/"), param("/home")])
    def test_main(self, route: str) -> None:
        router = APIRouter()

        @router.get(route)
        def _() -> None:
            return None

    def test_error(self) -> None:
        router = APIRouter()

        with raises(ValueError, match="Invalid route"):

            @router.get("/home/")
            def _() -> None:
                return None
