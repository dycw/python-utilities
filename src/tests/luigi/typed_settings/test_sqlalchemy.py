from __future__ import annotations

from sqlalchemy import Engine

from utilities.luigi import EngineParameter, annotation_to_class


class TestAnnotationToClass:
    def test_main(self) -> None:
        result = annotation_to_class(Engine)
        param = result()
        assert isinstance(param, EngineParameter)
