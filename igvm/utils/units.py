from __future__ import division

import re

_SIZE_FACTORS = {
    'T': 1024**4,
    'G': 1024**3,
    'M': 1024**2,
    'K': 1024,
    'B': 1
}
def parse_size(text, unit):
    """Return the size as integer in the desired unit.

    The TiB/GiB/MiB/KiB prefix is allowed as long as long as not ambiguous.
    We are dealing with the units case in-sensitively.
    """

    text = text.upper()
    unit = unit.upper()

    # First, handle the suffixes
    if text.endswith('B'):
        text = text[:-1]
        if text.endswith('I'):
            text = text[:-1]

    if not text:
        return ValueError('Empty size')

    if text[-1] in _SIZE_FACTORS:
        factor = _SIZE_FACTORS[text[-1]]
        text = text[:-1]
    else:
        factor = _SIZE_FACTORS[unit]

    text = text.strip()

    if not unicode(text).isnumeric():
        raise ValueError(
            'Size has to be in {}iB without decimal place.'.format(unit)
        )

    value = int(text) * factor
    if value % _SIZE_FACTORS[unit]:
        raise ValueError('Value must be multiple of 1 {}iB'.format(unit))
    return int(value / _SIZE_FACTORS[unit])

def convert_size(size, from_name, to_name):
    return size / _size_factors[from_name.upper()] * _size_factors[to_name.upper()]
