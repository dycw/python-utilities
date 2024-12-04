from dataclasses import dataclass


@dataclass(kw_only=True)
class Outer:
    inner: "Inner"


@dataclass(kw_only=True)
class Inner:
    x: None = None
