from pytest import raises

from utilities.text import ensure_str


class TestEnsureStr:
    def test_str(self) -> None:
        assert isinstance(ensure_str(""), str)

    def test_not_str(self) -> None:
        with raises(TypeError, match="x=None"):
            _ = ensure_str(None)
