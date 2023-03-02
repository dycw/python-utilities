from pytest import mark, param, raises

from utilities.re import (
    MultipleCaptureGroupsError,
    MultipleMatchesError,
    NoCaptureGroupsError,
    NoMatchesError,
    extract_group,
    extract_groups,
)


class TestExtractGroup:
    def test_success(self) -> None:
        assert extract_group(r"(\d)", "A0A") == "0"

    def test_no_groups(self) -> None:
        with raises(NoCaptureGroupsError):
            _ = extract_group(r"\d", "0")

    def test_multiple_groups(self) -> None:
        with raises(MultipleCaptureGroupsError):
            _ = extract_group(r"(\d)(\w)", "0A")

    def test_no_matches(self) -> None:
        with raises(NoMatchesError, match="pattern='.*', text='.*'"):
            _ = extract_group(r"(\d)", "A")

    def test_multiple_matches(self) -> None:
        with raises(MultipleMatchesError, match="pattern='.*', text='.*'"):
            _ = extract_group(r"(\d)", "0A0")


class TestExtractGroups:
    @mark.parametrize(
        ("pattern", "text", "expected"),
        [param(r"(\d)", "A0A", ["0"]), param(r"(\d)(\w)", "A0A0", ["0", "A"])],
    )
    def test_success(self, pattern: str, text: str, expected: list[str]) -> None:
        assert extract_groups(pattern, text) == expected

    def test_no_groups(self) -> None:
        with raises(NoCaptureGroupsError):
            _ = extract_groups(r"\d", "0")

    @mark.parametrize(
        ("pattern", "text"), [param(r"(\d)", "A"), param(r"(\d)(\w)", "A0")]
    )
    def test_no_matches(self, pattern: str, text: str) -> None:
        with raises(NoMatchesError, match="pattern='.*', text='.*'"):
            _ = extract_groups(pattern, text)

    @mark.parametrize(
        ("pattern", "text"), [param(r"(\d)", "0A0"), param(r"(\d)(\w)", "0A0A")]
    )
    def test_multiple_matches(self, pattern: str, text: str) -> None:
        with raises(MultipleMatchesError, match="pattern='.*', text='.*'"):
            _ = extract_groups(pattern, text)
