from pytest import raises

from utilities.text import ensure_str
from utilities.text import strip_and_dedent


class TestEnsureStr:
    def test_str(self) -> None:
        assert isinstance(ensure_str(""), str)

    def test_not_str(self) -> None:
        with raises(TypeError, match="x=None"):
            _ = ensure_str(None)


class TestStripAndDedent:
    def test_main(self) -> None:
        text = """
               This is line 1.
               This is line 2.
               """
        result = strip_and_dedent(text)
        expected = "This is line 1.\nThis is line 2."
        assert result == expected
