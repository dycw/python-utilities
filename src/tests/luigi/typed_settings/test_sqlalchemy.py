from __future__ import annotations

from sqlalchemy import Engine

from utilities.luigi import EngineParameter
from utilities.luigi.typed_settings import _map_annotation


class TestMapAnnotation:
    def test_main(self) -> None:
        result = _map_annotation(Engine)
        param = result()
        assert isinstance(param, EngineParameter)
