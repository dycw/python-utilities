from __future__ import annotations

import re
from re import DOTALL

import pytest
from bs4 import BeautifulSoup
from polars import DataFrame, Utf8
from polars.testing import assert_frame_equal

from utilities.bs4 import TableTagToDataFrameError, yield_tables
from utilities.text import strip_and_dedent


class TestYieldTables:
    def test_with_header(self) -> None:
        html = """
            <table>
                <tr>
                    <th>Company</th>
                    <th>Contact</th>
                    <th>Country</th>
                </tr>
                <tr>
                    <td>Alfreds Futterkiste</td>
                    <td>Maria Anders</td>
                    <td>Germany</td>
                </tr>
                <tr>
                    <td>Centro comercial Moctezuma</td>
                    <td>Francisco Chang</td>
                    <td>Mexico</td>
                </tr>
                <tr>
                    <td>Ernst Handel</td>
                    <td>Roland Mendel</td>
                    <td>Austria</td>
                </tr>
                <tr>
                    <td>Island Trading</td>
                    <td>Helen Bennett</td>
                    <td>UK</td>
                </tr>
            </table>
        """
        soup = BeautifulSoup(strip_and_dedent(html), features="html.parser")
        (df,) = list(yield_tables(soup))
        expected = DataFrame(
            {
                "Company": [
                    "Alfreds Futterkiste",
                    "Centro comercial Moctezuma",
                    "Ernst Handel",
                    "Island Trading",
                ],
                "Contact": [
                    "Maria Anders",
                    "Francisco Chang",
                    "Roland Mendel",
                    "Helen Bennett",
                ],
                "Country": ["Germany", "Mexico", "Austria", "UK"],
            },
            schema={"Company": Utf8, "Contact": Utf8, "Country": Utf8},
        )
        assert_frame_equal(df, expected)

    def test_without_header(self) -> None:
        html = """
            <table>
                <tr>
                    <td>Alfreds Futterkiste</td>
                    <td>Maria Anders</td>
                    <td>Germany</td>
                </tr>
                <tr>
                    <td>Centro comercial Moctezuma</td>
                    <td>Francisco Chang</td>
                    <td>Mexico</td>
                </tr>
                <tr>
                    <td>Ernst Handel</td>
                    <td>Roland Mendel</td>
                    <td>Austria</td>
                </tr>
                <tr>
                    <td>Island Trading</td>
                    <td>Helen Bennett</td>
                    <td>UK</td>
                </tr>
            </table>
        """
        soup = BeautifulSoup(strip_and_dedent(html), features="html.parser")
        (df,) = list(yield_tables(soup))
        expected = DataFrame(
            [
                (
                    "Alfreds Futterkiste",
                    "Centro comercial Moctezuma",
                    "Ernst Handel",
                    "Island Trading",
                ),
                ("Maria Anders", "Francisco Chang", "Roland Mendel", "Helen Bennett"),
                ("Germany", "Mexico", "Austria", "UK"),
            ]
        )
        assert_frame_equal(df, expected)

    def test_multiple_th_rows_error(self) -> None:
        html = """
            <table>
                <tr>
                    <th>Company</th>
                    <th>Contact</th>
                    <th>Country</th>
                </tr>
                <tr>
                    <th>Company</th>
                    <th>Contact</th>
                    <th>Country</th>
                </tr>
                <tr>
                    <td>Alfreds Futterkiste</td>
                    <td>Maria Anders</td>
                    <td>Germany</td>
                </tr>
                <tr>
                    <td>Centro comercial Moctezuma</td>
                    <td>Francisco Chang</td>
                    <td>Mexico</td>
                </tr>
                <tr>
                    <td>Ernst Handel</td>
                    <td>Roland Mendel</td>
                    <td>Austria</td>
                </tr>
                <tr>
                    <td>Island Trading</td>
                    <td>Helen Bennett</td>
                    <td>UK</td>
                </tr>
                <tr>
                    <td>Laughing Bacchus Winecellars</td>
                    <td>Yoshi Tannamuri</td>
                    <td>Canada</td>
                </tr>
                <tr>
                    <td>Magazzini Alimentari Riuniti</td>
                    <td>Giovanni Rovelli</td>
                    <td>Italy</td>
                </tr>
            </table>
        """
        soup = BeautifulSoup(strip_and_dedent(html), features="html.parser")
        with pytest.raises(
            TableTagToDataFrameError,
            match=re.compile(
                r"Table .* must contain exactly one `th` tag; got .*, .* and perhaps more\.",
                flags=DOTALL,
            ),
        ):
            _ = list(yield_tables(soup))
