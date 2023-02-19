import numba

import numpy as np

from functools import cached_property


def _nd_func_maker(cls, arg, **kwargs):
    if callable(arg) and not kwargs:
        return cls(arg)
    else:
        return lambda func: cls(func, signature=arg, **kwargs)


def ndmovingexp(*args, **kwargs):
    """N-dimensional exponential moving window function."""
    return _nd_func_maker(NumbaNDMovingExp, *args, **kwargs)


def _validate_axis(axis, ndim):
    """Helper function to convert axis into a non-negative integer, or raise if
    it's invalid.
    """
    if axis < 0:
        axis += ndim
    if axis < 0 or axis >= ndim:
        raise ValueError("invalid axis %s" % axis)
    return axis


def ndim(arg):
    return getattr(arg, "ndim", 0)


_ALPHABET = "abcdefghijkmnopqrstuvwxyz"


def _gufunc_arg_str(arg):
    return "(%s)" % ",".join(_ALPHABET[: ndim(arg)])


def gufunc_string_signature(numba_args):
    """Convert a tuple of numba types into a numpy gufunc signature.
    The last type is used as output argument.
    Example:
    >>> gufunc_string_signature((float64[:], float64))
    '(a)->()'
    """
    return (
        ",".join(map(_gufunc_arg_str, numba_args[:-1]))
        + "->"
        + _gufunc_arg_str(numba_args[-1])
    )


MOVE_WINDOW_ERR_MSG = "invalid window (not between 1 and %d, inclusive): %r"


def rolling_validator(arr, window):
    if (window < 1) or (window > arr.shape[-1]):
        raise ValueError(MOVE_WINDOW_ERR_MSG % (arr.shape[-1], window))


DEFAULT_MOVING_SIGNATURE = ((numba.float64[:], numba.int64, numba.float64[:]),)


class NumbaNDMoving:
    def __init__(
        self,
        func,
        signature=DEFAULT_MOVING_SIGNATURE,
        window_validator=rolling_validator,
    ) -> None:
        self.func = func
        self.window_validator = window_validator

        for sig in signature:
            if not isinstance(sig, tuple):
                raise TypeError(f"signatures for ndmoving must be tuples: {signature}")
        self.signature = signature

    @property
    def __name__(self):
        return self.func.__name__

    def __repr__(self) -> str:
        return f"<numbagg.decorators.{type(self).__name__} {self.__name__}>"

    @cached_property
    def gufunc(self):
        gufunc_sig = gufunc_string_signature(self.signature[0])
        vectorize = numba.guvectorize(self.signature, gufunc_sig, nopython=True)
        return vectorize(self.func)

    def __call__(self, arr, window, min_count=None, axis=-1):
        if min_count is None:
            min_count = window
        if not 0 < window < arr.shape[axis]:
            raise ValueError(f"window not in valid range: {window}")
        if min_count < 0:
            raise ValueError(f"min_count must be positive: {min_count}")
        axis = _validate_axis(axis, arr.ndim)
        arr = np.moveaxis(arr, axis, -1)
        result = self.gufunc(arr, window, min_count)
        return np.moveaxis(result, -1, axis)


class NumbaNDMovingExp(NumbaNDMoving):
    def __call__(self, arr, alpha, axis=-1):
        if alpha < 0:
            raise ValueError(f"alpha must be positive: {alpha}")
        # If an empty tuple is passed, there's no reduction to do, so we return the
        # original array.
        # Ref https://github.com/pydata/xarray/pull/5178/files#r616168398
        if axis == ():
            return arr
        axis = _validate_axis(axis, arr.ndim)
        arr = np.moveaxis(arr, axis, -1)
        result = self.gufunc(arr, alpha)
        return np.moveaxis(result, -1, axis)
