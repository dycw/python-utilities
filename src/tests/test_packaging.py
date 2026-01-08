from __future__ import annotations

from pytest import mark, param

from utilities.iterables import one
from utilities.packaging import Requirement


class TestRequirement:
    def test_extra(self) -> None:
        extra = "extra"
        requirement = Requirement.new(f"package[{extra}]")
        assert requirement.extras == [extra]

    def test_marker(self) -> None:
        requirement = Requirement.new('package; python_version >= "3"')
        assert requirement.marker is not None
        variable, op, value = one(requirement.marker)
        assert str(variable) == "python_version"
        assert str(op) == ">="
        assert str(value) == "3"

    def test_name(self) -> None:
        requirement = Requirement.new("package")
        assert requirement.name == "package"

    def test_specifier(self) -> None:
        requirement = Requirement.new("package>=1.2.3, <1.3")
        assert requirement.specifier == ">=1.2.3,<1.3"

    def test_specifier_set(self) -> None:
        requirement = Requirement.new("package>=1.2.3, <1.3")
        assert requirement.specifier_set == ">=1.2.3,<1.3"

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
    def test_str(self, *, requirement: str, expected: str) -> None:
        assert str(Requirement.new(requirement)) == expected

    def test_url(self) -> None:
        url = "https://www.github.com"
        requirement = Requirement.new(f"package@{url}")
        assert requirement.url == url
