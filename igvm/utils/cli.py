import fabric.colors
import sys


def _wrapped_color(color):
    def fn(s, bold=False):
        if not sys.stdout.isatty():
            return s
        return color(s, bold=bold)
    return fn


for color in (c for c in dir(fabric.colors) if not c.startswith('_')):
    globals()[color] = _wrapped_color(getattr(fabric.colors, color))
