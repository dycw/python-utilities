from pathlib import Path

from beartype import beartype
from nox import Session, session


@session
@beartype
def ruff(session: Session, /) -> None:
    """Run `ruff`."""
    session.install("ruff")
    _ = session.run("ruff", "--fix", ".")


@session(python=["3.9", "3.10", "3.11"])
@beartype
def tests(session: Session, /) -> None:
    """Run the tests."""
    session.install("--upgrade", "pip-tools")
    requirements = set(Path(__file__).parent.glob("requirements*.txt"))
    _ = session.run("pip-sync", *(r.as_posix() for r in requirements))
    _ = session.run("pytest", "--cov-report=term-missing:skip-covered", "-n=auto")
