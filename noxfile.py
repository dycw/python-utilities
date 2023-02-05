from beartype import beartype  # noqa: INP001M
from nox import Session, session


@session(python=["3.9", "3.10"])
@beartype
def tests(session: Session, /) -> None:
    """Run the tests."""
    session.install("-r", "requirements-dev.txt")
    _ = session.run("pytest", "-nauto")
