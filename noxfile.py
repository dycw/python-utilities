from __future__ import annotations

from pathlib import Path

from nox import Session, session


@session
def ruff(session: Session) -> None:
    """Run `ruff`."""
    session.install("ruff")
    _ = session.run("ruff", "--fix", ".")


@session(python=["3.11", "3.12"])
def tests(session: Session, /) -> None:
    """Run the tests."""
    session.install("--upgrade", "pip-tools")
    requirements = set(Path(__file__).parent.glob("requirements*.txt"))
    _ = session.run("pip-sync", *(map(str, requirements)))
    _ = session.run("pytest", "--cov-report=term-missing:skip-covered", "-n=auto")
