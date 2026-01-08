from __future__ import annotations

from packaging.requirements import InvalidRequirement
from pytest import mark, param, raises

from utilities.packaging import SortedRequirement, format_requirement


class TestFormatRequirement:
    @mark.parametrize(
        ("requirement", "expected"),
        [
            param("package", "package"),
            param("package>=1.2.3", "package>=1.2.3"),
            param("package<1.3", "package<1.3"),
            param("package>=1.2.3,<1.3", "package>=1.2.3, <1.3"),
            param("package<1.3,>=1.2.3", "package>=1.2.3, <1.3"),
            param("package[extra]>=1.2.3", "package[extra]>=1.2.3"),
            param("package[extra1,extra2]>=1.2.3", "package[extra1,extra2]>=1.2.3"),
            param("package@https://www.github.com", "package@ https://www.github.com"),
            param("package;python_version>='3'", 'package; python_version >= "3"'),
            param(
                "package[extra1,extra2]@https://www.github.com>=1.2.3,<1.3;python_version>='3'",
                "package[extra1,extra2]@ https://www.github.com>=1.2.3,<1.3;python_version>='3'",
            ),
        ],
    )
    def test_main(self, *, requirement: str, expected: str) -> None:
        assert format_requirement(requirement) == expected


class TestSortedRequirement:
    def test_error(self) -> None:
        with raises(InvalidRequirement, match="Expected end or semicolon"):
            _ = SortedRequirement("invalid >> 1.2.3")
