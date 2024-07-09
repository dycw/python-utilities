from __future__ import annotations

from ast import Assign, Attribute, Call, Import, Load, Module, Name, alias

from utilities.astor import module_to_source


class TestModuleToSource:
    def test_main(self) -> None:
        import_dt = Import(names=[alias(name="datetime", asname="dt")])
        assign = Assign(
            targets=[Name(id="now")],
            value=Call(
                func=Attribute(
                    value=Attribute(value=Name(id="dt", ctx=Load()), attr="datetime"),
                    attr="now",
                ),
                args=[],
                keywords=[],
            ),
        )
        module = Module(body=[import_dt, assign], type_ignores=[])
        result = module_to_source(module)
        expected_with_ruff = """\
import datetime as dt

now = dt.datetime.now()
"""
        expected_without_ruff = """\
import datetime as dt
now = dt.datetime.now()
"""
        assert result in {expected_with_ruff, expected_without_ruff}
