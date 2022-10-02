from re import compile

from beartype import beartype


@beartype
def extract_group(pattern: str, text: str, /) -> str:
    """Apply a regex with 1 capture group and check that there is exactly 1
    match, and then return it.
    """

    if compile(pattern).groups <= 1:
        (result,) = extract_groups(pattern, text)
        return result
    else:
        raise MultipleCaptureGroups(pattern)


class MultipleCaptureGroups(ValueError):
    ...


@beartype
def extract_groups(pattern: str, text: str, /) -> list[str]:
    """Apply a regex with a positive number of capture groups and check that
    there is exactly 1 match, and then return their contents as a single list.
    """

    compiled = compile(pattern)
    if (n_groups := compiled.groups) == 0:
        raise NoCaptureGroups(pattern)
    else:
        results = compiled.findall(text)
        if (n_results := len(results)) == 0:
            raise NoMatches(f"{pattern=}, {text=}")
        elif n_results == 1:
            if n_groups == 1:
                return results
            else:
                (result,) = results
                return list(result)
        else:
            raise MultipleMatches(f"{pattern=}, {text=}")


class NoCaptureGroups(ValueError):
    ...


class NoMatches(ValueError):
    ...


class MultipleMatches(ValueError):
    ...
