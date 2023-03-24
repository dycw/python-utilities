from typing import Any, Optional, Union

from pytest import mark, param
from tqdm import tqdm as _tqdm

from utilities.tqdm import _get_total, tqdm


class TestGetTotal:
    @mark.parametrize(
        ("total", "iterable", "expected"),
        [
            param(None, (), 0),
            param(0, (), 0),
            param(1, (), 1),
            param(0.0, (), 0.0),
            param(1.0, (), 1.0),
            param(None, range(3), 3),
            param(None, iter(range(3)), None),
        ],
    )
    def test_main(
        self,
        total: Optional[Union[int, float]],
        iterable: Any,
        expected: Optional[Union[int, float]],
    ) -> None:
        assert _get_total(total, iterable) == expected

    def test_custom(self) -> None:
        class Custom:
            def __len__(self) -> int:
                return 1

        custom = Custom()
        assert _get_total(None, custom) == 1


class TestTqdm:
    def test_disable_tqdm(self, capsys: Any) -> None:
        _ = list(tqdm(range(10)))
        assert not capsys.readouterr().err

    def test_disable_native(self, capsys: Any) -> None:
        _ = list(_tqdm(range(10)))
        assert capsys.readouterr().err
