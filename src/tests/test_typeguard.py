from inspect import signature

from utilities.typeguard import typeguard_ignore


class TestTypeguardIgnore:
    def test_main(self) -> None:
        def func() -> None:
            pass

        decorated = typeguard_ignore(func)
        assert signature(decorated) == signature(func)
