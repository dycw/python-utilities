from __future__ import annotations

from pytest import raises

from utilities.traceback import yield_extended_frame_summaries, yield_frames


class TestYieldExtendedFrameSummaries:
    def test_explicit_traceback(self) -> None:
        def f() -> None:
            return g()

        def g() -> None:
            raise NotImplementedError

        with raises(NotImplementedError) as exc_info:
            f()
        frames = list(
            yield_extended_frame_summaries(exc_info.value, traceback=exc_info.tb)
        )
        assert len(frames) == 3
        expected = [
            TestYieldExtendedFrameSummaries.test_explicit_traceback.__qualname__,
            f.__qualname__,
            g.__qualname__,
        ]
        for frame, exp in zip(frames, expected, strict=True):
            assert frame.qualname == exp

    def test_implicit_traceback(self) -> None:
        def f() -> None:
            return g()

        def g() -> None:
            raise NotImplementedError

        try:
            f()
        except NotImplementedError as error:
            frames = list(yield_extended_frame_summaries(error))
            assert len(frames) == 3
            expected = [
                TestYieldExtendedFrameSummaries.test_implicit_traceback.__qualname__,
                f.__qualname__,
                g.__qualname__,
            ]
            for frame, exp in zip(frames, expected, strict=True):
                assert frame.qualname == exp


class TestYieldFrames:
    def test_main(self) -> None:
        def f() -> None:
            return g()

        def g() -> None:
            raise NotImplementedError

        with raises(NotImplementedError) as exc_info:
            f()
        frames = list(yield_frames(traceback=exc_info.tb))
        assert len(frames) == 3
        expected = ["test_main", "f", "g"]
        for frame, exp in zip(frames, expected, strict=True):
            assert frame.f_code.co_name == exp
