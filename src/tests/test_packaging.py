from __future__ import annotations

from pytest import mark, param, raises

from utilities.iterables import one
from utilities.packaging import Requirement, _CustomSpecifierSet


class TestRequirement:
    def test_extra(self) -> None:
        extra = "extra"
        requirement = Requirement(f"package[{extra}]")
        assert requirement.extras == [extra]

    def test_get(self) -> None:
        requirement = Requirement("package>=1.2.3, <1.3")
        assert requirement.get(">=") == "1.3"
        assert requirement.get("<") == "1.3"
        assert requirement.get(">") is None

    def test_get_item(self) -> None:
        requirement = Requirement("package>=1.2.3, <1.3")
        assert requirement[">="] == "1.2.3"
        assert requirement["<"] == "1.3"
        with raises(KeyError):
            _ = requirement[">"]

    def test_marker(self) -> None:
        requirement = Requirement('package; python_version >= "3"')
        assert requirement.marker is not None
        variable, op, value = one(requirement.marker)
        assert str(variable) == "python_version"
        assert str(op) == ">="
        assert str(value) == "3"

    def test_name(self) -> None:
        requirement = Requirement("package")
        assert requirement.name == "package"

    def test_specifier(self) -> None:
        requirement = Requirement("package>=1.2.3, <1.3")
        assert requirement.specifier == ">=1.2.3,<1.3"

    def test_specifier_set(self) -> None:
        requirement = Requirement("package>=1.2.3, <1.3")
        assert requirement.specifier_set == ">=1.2.3,<1.3"

    @mark.parametrize(
        ("requirement", "expected"),
        [
            param("package", "package"),
            param("package>=1.2.3", "package>=1.2.3"),
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
        assert str(Requirement(requirement)) == expected

    def test_url(self) -> None:
        url = "https://www.github.com"
        requirement = Requirement(f"package@{url}")
        assert requirement.url == url


class TestCustomSpecifierSet:
    def test_get(self) -> None:
        set_ = _CustomSpecifierSet(">=1.2.3, <1.3")
        assert set_.get(">=") == "1.3"
        assert set_.get("<") == "1.3"
        assert set_.get(">") is None

    def test_get_item(self) -> None:
        set_ = _CustomSpecifierSet(">=1.2.3, <1.3")
        assert set_[">="] == "1.2.3"
        assert set_["<"] == "1.3"
        with raises(KeyError):
            _ = set_[">"]
        assert set_.get("<") == "1.3"
        assert set_.get(">") is None

    def test_replace_existing(self) -> None:
        set_ = _CustomSpecifierSet(">=1.2.3, <1.3").replace(">=", "1.2.4")
        expected = _CustomSpecifierSet(">=1.2.4, <1.3")
        assert set_ == expected

    def test_replace_new(self) -> None:
        set_ = _CustomSpecifierSet(">=1.2.3").replace("<", "1.3")
        expected = _CustomSpecifierSet(">=1.2.3, <1.3")
        assert set_ == expected

    @mark.parametrize(
        ("specifier", "expected"),
        [
            param("", ""),
            param(">=1.2.3", ">=1.2.3"),
            param("<1.3", "<1.3"),
            param(">=1.2.3,<1.3", ">=1.2.3, <1.3"),
            param("<1.3,>=1.2.3", ">=1.2.3, <1.3"),
        ],
    )
    def test_str(self, *, specifier: str, expected: str) -> None:
        set_ = _CustomSpecifierSet(specifier)
        assert str(set_) == expected
