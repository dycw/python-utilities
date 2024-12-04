from dataclasses import dataclass

from utilities.beartype import beartype_cond


@beartype_cond
@dataclass(kw_only=True)
class Outer:
    inner: "Inner"


@beartype_cond
@dataclass(kw_only=True)
class Inner:
    x: None = None
