from collections.abc import Callable, Iterator


def get_qmark_params() -> Iterator[str]:
    while True:
        yield "?"


def get_numbered_params(prefix: str = "$") -> Iterator[str]:
    index = 1
    while True:
        yield f"{prefix}{index}"
        index += 1


def get_format_specifier() -> Iterator[str]:
    while True:
        yield "%s"


ParamGenerator = Callable[[], Iterator[str]]
