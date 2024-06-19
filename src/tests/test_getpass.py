from utilities.getpass import USER


class TestUser:
    def test_main(self) -> None:
        assert isinstance(USER, str)
