from utilities.sys import PYTHON_AT_LEAST_3_11, VERSION_MAJOR_MINOR


class TestPythonAtLeast311:
    def test_main(self) -> None:
        assert isinstance(PYTHON_AT_LEAST_3_11, bool)


class TestVersionMajorMinor:
    def test_main(self) -> None:
        assert isinstance(VERSION_MAJOR_MINOR, tuple)
        assert len(VERSION_MAJOR_MINOR) == 2
