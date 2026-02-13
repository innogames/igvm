"""igvm - Color output helpers

Copyright (c) 2026 InnoGames GmbH

Replacement for fabric.colors which is not available in fabric 3.x.
"""


def _wrap(code: int, text: str, bold: bool = False) -> str:
    if bold:
        return f'\033[1;{code}m{text}\033[0m'
    return f'\033[{code}m{text}\033[0m'


def green(text: str, bold: bool = False) -> str:
    return _wrap(32, text, bold)


def red(text: str, bold: bool = False) -> str:
    return _wrap(31, text, bold)


def yellow(text: str, bold: bool = False) -> str:
    return _wrap(33, text, bold)


def white(text: str, bold: bool = False) -> str:
    return _wrap(37, text, bold)
