from __future__ import annotations

from asyncio import sleep
from io import StringIO
from logging import Formatter, Logger, StreamHandler, getLogger
from pathlib import Path
from re import search
from typing import TYPE_CHECKING, Any, cast

from hypothesis import given
from hypothesis.strategies import booleans, integers, none, sampled_from
from pytest import LogCaptureFixture, mark, param, raises

from tests.conftest import SKIPIF_CI_AND_WINDOWS
from utilities.hypothesis import (
    assume_does_not_raise,
    pairs,
    temp_paths,
    text_ascii,
    zoned_datetimes,
)
from utilities.iterables import one
from utilities.logging import (
    FilterForKeyError,
    GetLoggingLevelNumberError,
    SizeAndTimeRotatingFileHandler,
    _compute_rollover_actions,
    _FieldStyleKeys,
    _RotatingLogFile,
    add_filters,
    basic_config,
    filter_for_key,
    get_formatter,
    get_logger,
    get_logging_level_number,
    setup_logging,
)
from utilities.text import unique_str
from utilities.types import LogLevel
from utilities.typing import get_args
from utilities.whenever import format_compact, get_now, to_local_plain

if TYPE_CHECKING:
    from collections.abc import Mapping
    from contextlib import AbstractContextManager
    from logging import _FilterType

    from whenever import ZonedDateTime


class TestAddFilters:
    @given(expected=booleans())
    def test_main(self, *, expected: bool) -> None:
        logger = getLogger(unique_str())
        logger.addHandler(handler := StreamHandler(buffer := StringIO()))
        add_filters(handler, lambda _: expected)
        assert len(handler.filters) == 1
        logger.warning("message")
        result = buffer.getvalue() != ""
        assert result is expected

    def test_no_handlers(self) -> None:
        handler = StreamHandler()
        assert len(handler.filters) == 0
        add_filters(handler)
        assert len(handler.filters) == 0


class TestBasicConfig:
    @mark.parametrize(
        "filters",
        [
            param(lambda _: True),  # pyright: ignore[reportUnknownLambdaType]
            param(None),
        ],
    )
    @mark.parametrize("plain", [param(True), param(False)])
    def test_main(
        self,
        *,
        caplog: LogCaptureFixture,
        filters: _FilterType | None,
        plain: bool,
        set_log_factory: AbstractContextManager[None],
    ) -> None:
        name = unique_str()
        with set_log_factory:
            basic_config(obj=name, filters=filters, plain=plain)
        getLogger(name).warning("message")
        record = one(r for r in caplog.records if r.name == name)
        assert record.message == "message"

    def test_none(self, *, set_log_factory: AbstractContextManager[None]) -> None:
        with set_log_factory:
            basic_config()


class TestComputeRolloverActions:
    async def test_main(self, *, tmp_path: Path) -> None:
        tmp_path.joinpath("log.txt").touch()

        actions = _compute_rollover_actions(tmp_path, "log", ".txt")
        assert len(actions.deletions) == 0
        assert len(actions.rotations) == 1
        actions.do()
        files = list(tmp_path.iterdir())
        assert len(files) == 1
        assert any(p for p in files if search(r"^log\.1\__[\dT]+\.txt$", p.name))

        for _ in range(2):
            await sleep(1)
            tmp_path.joinpath("log.txt").touch()
            actions = _compute_rollover_actions(tmp_path, "log", ".txt")
            assert len(actions.deletions) == 1
            assert len(actions.rotations) == 1
            actions.do()
            files = list(tmp_path.iterdir())
            assert len(files) == 1
            assert any(
                p for p in files if search(r"^log\.1\__[\dT]+__[\dT]+\.txt$", p.name)
            )

    async def test_multiple_backups(self, *, tmp_path: Path) -> None:
        tmp_path.joinpath("log.txt").touch()

        actions = _compute_rollover_actions(tmp_path, "log", ".txt", backup_count=3)
        assert len(actions.deletions) == 0
        assert len(actions.rotations) == 1
        actions.do()
        files = list(tmp_path.iterdir())
        assert len(files) == 1
        assert any(p for p in files if search(r"^log\.1\__[\dT]+\.txt$", p.name))

        await sleep(1)
        tmp_path.joinpath("log.txt").touch()
        actions = _compute_rollover_actions(tmp_path, "log", ".txt", backup_count=3)
        assert len(actions.deletions) == 0
        assert len(actions.rotations) == 2
        actions.do()
        files = list(tmp_path.iterdir())
        assert len(files) == 2
        assert any(
            p for p in files if search(r"^log\.1\__[\dT]+__[\dT]+\.txt$", p.name)
        )
        assert any(p for p in files if search(r"^log\.2\__[\dT]+\.txt$", p.name))

        await sleep(1)
        tmp_path.joinpath("log.txt").touch()
        actions = _compute_rollover_actions(tmp_path, "log", ".txt", backup_count=3)
        assert len(actions.deletions) == 0
        assert len(actions.rotations) == 3
        actions.do()
        files = list(tmp_path.iterdir())
        assert len(files) == 3
        assert any(
            p for p in files if search(r"^log\.1\__[\dT]+__[\dT]+\.txt$", p.name)
        )
        assert any(
            p for p in files if search(r"^log\.2\__[\dT]+__[\dT]+\.txt$", p.name)
        )
        assert all(p for p in files if search(r"^log\.3\__[\dT]+\.txt$", p.name))

        for _ in range(2):
            await sleep(1)
            tmp_path.joinpath("log.txt").touch()
            actions = _compute_rollover_actions(tmp_path, "log", ".txt", backup_count=3)
            assert len(actions.deletions) == 1
            assert len(actions.rotations) == 3
            actions.do()
            files = list(tmp_path.iterdir())
            assert len(files) == 3
            assert any(
                p for p in files if search(r"^log\.1\__[\dT]+__[\dT]+\.txt$", p.name)
            )
            assert any(
                p for p in files if search(r"^log\.2\__[\dT]+__[\dT]+\.txt$", p.name)
            )
            assert all(
                p for p in files if search(r"^log\.3\__[\dT]+__[\dT]+\.txt$", p.name)
            )

    async def test_deleting_old_files(self, *, tmp_path: Path) -> None:
        tmp_path.joinpath("log.txt").touch()

        actions = _compute_rollover_actions(tmp_path, "log", ".txt")
        assert len(actions.deletions) == 0
        assert len(actions.rotations) == 1
        actions.do()
        files = list(tmp_path.iterdir())
        assert len(files) == 1
        assert any(p for p in files if search(r"^log\.1\__[\dT]+\.txt$", p.name))

        await sleep(1)
        tmp_path.joinpath("log.txt").touch()
        now = format_compact(to_local_plain(get_now()))
        tmp_path.joinpath(f"log.99__{now}__{now}.txt").touch()
        actions = _compute_rollover_actions(tmp_path, "log", ".txt")
        assert len(actions.deletions) == 2
        assert len(actions.rotations) == 1
        actions.do()
        files = list(tmp_path.iterdir())
        assert len(files) == 1
        assert any(
            p for p in files if search(r"^log\.1\__[\dT]+__[\dT]+\.txt$", p.name)
        )


class TestFilterForKey:
    @given(key=text_ascii(), value=booleans() | none(), default=booleans())
    def test_main(self, *, key: str, value: bool | None, default: bool) -> None:
        logger = getLogger(unique_str())
        logger.addHandler(handler := StreamHandler(buffer := StringIO()))
        with assume_does_not_raise(FilterForKeyError):
            filter_ = filter_for_key(key, default=default)
        add_filters(handler, filter_)
        match value:
            case bool():
                logger.warning("message", extra={key: value})
                expected = value
            case None:
                logger.warning("message")
                expected = default
        result = buffer.getvalue() != ""
        assert result is expected

    def test_sunder(self) -> None:
        _ = filter_for_key("_key")

    @given(key=sampled_from(["msg", "__dunder__"]))
    def test_error(self, *, key: str) -> None:
        with raises(FilterForKeyError, match="Invalid key: '.*'"):
            _ = filter_for_key(key)


class TestGetFormatter:
    @mark.parametrize("plain", [param(True), param(False)])
    @mark.parametrize("color_field_styles", [param({}), param(None)])
    def test_main(
        self,
        *,
        plain: bool,
        color_field_styles: Mapping[str, _FieldStyleKeys] | None,
        set_log_factory: AbstractContextManager[None],
    ) -> None:
        with set_log_factory:
            formatter = get_formatter(
                plain=plain, color_field_styles=color_field_styles
            )
        assert isinstance(formatter, Formatter)


class TestGetLogger:
    def test_logger(self) -> None:
        logger = getLogger(unique_str())
        result = get_logger(logger=logger)
        assert result is logger

    def test_str(self) -> None:
        name = unique_str()
        logger = getLogger(name)
        assert isinstance(logger, Logger)
        assert logger.name == name

    def test_none(self) -> None:
        result = get_logger()
        assert isinstance(result, Logger)
        assert result.name == "root"


class TestGetLoggingLevelNumber:
    @mark.parametrize(
        ("level", "expected"),
        [
            param("DEBUG", 10),
            param("INFO", 20),
            param("WARNING", 30),
            param("ERROR", 40),
            param("CRITICAL", 50),
        ],
    )
    def test_main(self, *, level: LogLevel, expected: int) -> None:
        assert get_logging_level_number(level) == expected

    def test_error(self) -> None:
        with raises(
            GetLoggingLevelNumberError, match="Invalid logging level: 'invalid'"
        ):
            _ = get_logging_level_number(cast("Any", "invalid"))


class TestLogLevel:
    def test_main(self) -> None:
        assert len(get_args(LogLevel)) == 5


class TestRotatingLogFile:
    def test_from_path(self) -> None:
        path = Path("log.txt")
        result = _RotatingLogFile.from_path(path, "log", ".txt")
        assert result is not None
        assert result.stem == "log"
        assert result.suffix == ".txt"
        assert result.index is None
        assert result.start is None
        assert result.end is None

    @given(index=integers(min_value=1))
    def test_from_path_with_index(self, *, index: int) -> None:
        path = Path(f"log.{index}.txt")
        result = _RotatingLogFile.from_path(path, "log", ".txt")
        assert result is not None
        assert result.stem == "log"
        assert result.suffix == ".txt"
        assert result.index == index
        assert result.start is None
        assert result.end is None

    @given(index=integers(min_value=1), end=zoned_datetimes())
    def test_from_path_with_index_and_end(
        self, *, index: int, end: ZonedDateTime
    ) -> None:
        path = Path(f"log.{index}__{format_compact(to_local_plain(end))}.txt")
        result = _RotatingLogFile.from_path(path, "log", ".txt")
        assert result is not None
        assert result.stem == "log"
        assert result.suffix == ".txt"
        assert result.index == index
        assert result.start is None
        assert result.end == end.round()

    @given(index=integers(min_value=1), datetimes=pairs(zoned_datetimes(), sorted=True))
    def test_from_path_with_index_start_and_end(
        self, *, index: int, datetimes: tuple[ZonedDateTime, ZonedDateTime]
    ) -> None:
        start, end = datetimes
        path = Path(
            f"log.{index}__{format_compact(to_local_plain(start))}__{format_compact(to_local_plain(end))}.txt"
        )
        result = _RotatingLogFile.from_path(path, "log", ".txt")
        assert result is not None
        assert result.stem == "log"
        assert result.suffix == ".txt"
        assert result.index == index
        assert result.start == start.round()
        assert result.end == end.round()

    def test_from_path_none(self) -> None:
        path = Path("invalid.txt")
        result = _RotatingLogFile.from_path(path, "log", ".txt")
        assert result is None

    def test_path(self, *, tmp_path: Path) -> None:
        file = _RotatingLogFile(directory=tmp_path, stem="log", suffix=".txt")
        assert file.path == tmp_path.joinpath("log.txt")

    @given(index=integers(min_value=1), root=temp_paths())
    def test_path_with_index(self, *, index: int, root: Path) -> None:
        file = _RotatingLogFile(directory=root, stem="log", suffix=".txt", index=index)
        assert file.path == root.joinpath(f"log.{index}.txt")

    @given(root=temp_paths(), index=integers(min_value=1), end=zoned_datetimes())
    def test_path_with_index_and_end(
        self, *, root: Path, index: int, end: ZonedDateTime
    ) -> None:
        file = _RotatingLogFile(
            directory=root, stem="log", suffix=".txt", index=index, end=end
        )
        assert file.path == root.joinpath(
            f"log.{index}__{format_compact(to_local_plain(end))}.txt"
        )

    @given(
        root=temp_paths(),
        index=integers(min_value=1),
        datetimes=pairs(zoned_datetimes(), sorted=True),
    )
    def test_path_with_index_start_and_end(
        self, *, root: Path, index: int, datetimes: tuple[ZonedDateTime, ZonedDateTime]
    ) -> None:
        start, end = datetimes
        file = _RotatingLogFile(
            directory=root, stem="log", suffix=".txt", index=index, start=start, end=end
        )
        assert file.path == root.joinpath(
            f"log.{index}__{format_compact(to_local_plain(start))}__{format_compact(to_local_plain(end))}.txt"
        )


class TestSetupLogging:
    def test_main(self, *, tmp_path: Path) -> None:
        name = unique_str()
        setup_logging(logger=name, files_dir=tmp_path)
        logger = getLogger(name)
        assert len(logger.handlers) == 7
        logger.warning("message")
        files = {p.name for p in tmp_path.iterdir() if p.is_file()}
        expected = {
            "debug.txt",
            "info.txt",
            "error.txt",
            f"{name}-debug.txt",
            f"{name}-info.txt",
            f"{name}-error.txt",
        }
        assert files == expected


class TestSizeAndTimeRotatingFileHandler:
    def test_handlers(self, *, tmp_path: Path) -> None:
        logger = getLogger(unique_str())
        filename = tmp_path.joinpath("log")
        logger.addHandler(SizeAndTimeRotatingFileHandler(filename=filename))
        logger.warning("message")
        content = filename.read_text()
        assert content == "message\n"

    def test_create_parents(self, *, tmp_path: Path) -> None:
        logger = getLogger(unique_str())
        filename = tmp_path.joinpath("foo", "bar", "bar", "log")
        logger.addHandler(SizeAndTimeRotatingFileHandler(filename=filename))
        assert filename.exists()

    @SKIPIF_CI_AND_WINDOWS
    async def test_size(self, *, tmp_path: Path) -> None:
        logger = getLogger(unique_str())
        logger.addHandler(
            SizeAndTimeRotatingFileHandler(
                filename=tmp_path.joinpath("log.txt"), maxBytes=100, backupCount=3
            )
        )
        for cycle in range(1, 10):
            for i in range(1, 4):
                logger.warning("%s message %d", 100 * "long" if i % 3 == 0 else "", i)
                files = list(tmp_path.iterdir())
                assert len(files) == min(cycle, 4)
                assert any(p for p in files if search(r"^log\.txt$", p.name))
                if cycle == 2:
                    assert any(
                        p for p in files if search(r"^log\.1__[\dT]+\.txt$", p.name)
                    )
                elif cycle == 3:
                    assert any(
                        p
                        for p in files
                        if search(r"^log\.1__[\dT]+__[\dT]+\.txt$", p.name)
                    )
                    assert any(
                        p for p in files if search(r"^log\.2__[\dT]+\.txt$", p.name)
                    )
                elif cycle == 4:
                    assert any(
                        p
                        for p in files
                        if search(r"^log\.1__[\dT]+__[\dT]+\.txt$", p.name)
                    )
                    assert any(
                        p
                        for p in files
                        if search(r"^log\.2__[\dT]+__[\dT]+\.txt$", p.name)
                    )
                    assert any(
                        p for p in files if search(r"^log\.3__[\dT]+\.txt$", p.name)
                    )
                elif cycle >= 5:
                    assert any(
                        p
                        for p in files
                        if search(r"^log\.1__[\dT]+__[\dT]+\.txt$", p.name)
                    )
                    assert any(
                        p
                        for p in files
                        if search(r"^log\.2__[\dT]+__[\dT]+\.txt$", p.name)
                    )
                    assert any(
                        p
                        for p in files
                        if search(r"^log\.3__[\dT]+__[\dT]+\.txt$", p.name)
                    )
                await sleep(0.1)

    @SKIPIF_CI_AND_WINDOWS
    async def test_time(self, *, tmp_path: Path) -> None:
        logger = getLogger(unique_str())
        logger.addHandler(
            SizeAndTimeRotatingFileHandler(
                filename=tmp_path.joinpath("log.txt"),
                backupCount=3,
                when="S",
                interval=1,
            )
        )

        files = list(tmp_path.iterdir())
        assert len(files) == 1
        assert any(p for p in files if search(r"^log\.txt$", p.name))

        logger.warning("message 1")
        files = list(tmp_path.iterdir())
        assert len(files) == 1
        assert any(p for p in files if search(r"^log\.txt$", p.name))

        await sleep(1.1)
        for i in range(2, 4):
            logger.warning("message %d", i)
            files = list(tmp_path.iterdir())
            assert len(files) == 2
            assert any(p for p in files if search(r"^log\.txt$", p.name))
            assert any(p for p in files if search(r"^log\.1__[\dT]+\.txt$", p.name))

        await sleep(1.1)
        for i in range(4, 6):
            logger.warning("message %d", i)
            files = list(tmp_path.iterdir())
            assert len(files) == 3
            assert any(p for p in files if search(r"^log\.txt$", p.name))
            assert any(
                p for p in files if search(r"^log\.1__[\dT]+__[\dT]+\.txt$", p.name)
            )
            assert any(p for p in files if search(r"^log\.2__[\dT]+\.txt$", p.name))

        await sleep(1.1)
        for i in range(6, 8):
            logger.warning("message %d", i)
            files = list(tmp_path.iterdir())
            assert len(files) == 4
            assert any(p for p in files if search(r"^log\.txt$", p.name))
            assert any(
                p for p in files if search(r"^log\.1__[\dT]+__[\dT]+\.txt$", p.name)
            )
            assert any(
                p for p in files if search(r"^log\.2__[\dT]+__[\dT]+\.txt$", p.name)
            )
            assert any(p for p in files if search(r"^log\.3__[\dT]+\.txt$", p.name))

        for _ in range(2):
            await sleep(1.1)
            for i in range(8, 10):
                logger.warning("message %d", i)
                files = list(tmp_path.iterdir())
                assert len(files) == 4
                assert any(p for p in files if search(r"^log\.txt$", p.name))
                assert any(
                    p for p in files if search(r"^log\.1__[\dT]+__[\dT]+\.txt$", p.name)
                )
                assert any(
                    p for p in files if search(r"^log\.2__[\dT]+__[\dT]+\.txt$", p.name)
                )
                assert any(
                    p for p in files if search(r"^log\.3__[\dT]+__[\dT]+\.txt$", p.name)
                )

    @mark.parametrize("max_bytes", [param(0), param(1)])
    @SKIPIF_CI_AND_WINDOWS
    def test_should_rollover_file_not_found(
        self, *, tmp_path: Path, max_bytes: int, caplog: LogCaptureFixture
    ) -> None:
        logger = getLogger(name := unique_str())
        path = tmp_path.joinpath("log")
        logger.addHandler(
            handler := SizeAndTimeRotatingFileHandler(filename=path, maxBytes=max_bytes)
        )
        logger.warning("message")
        record = one(r for r in caplog.records if r.name == name)
        path.unlink()
        assert not handler._should_rollover(record)
