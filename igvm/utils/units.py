from __future__ import division


_SIZE_FACTORS = {
    'T': 1024 ** 4,
    'G': 1024 ** 3,
    'M': 1024 ** 2,
    'K': 1024 ** 1,
    'B': 1024 ** 0,
}


def parse_size(text, unit):
    """Return the size as integer in the desired unit.

    The TiB/GiB/MiB/KiB prefix is allowed as long as long as not ambiguous.
    We are dealing with the units case in-sensitively.
    """

    text = text.strip()
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

    try:
        value = float(text) * factor
    except ValueError:
        raise ValueError(
            'Cannot parse "{}" as {}iB value.'.format(text, unit)
        )

    if value % _SIZE_FACTORS[unit]:
        raise ValueError('Value must be multiple of 1 {}iB'.format(unit))
    return int(value / _SIZE_FACTORS[unit])


def convert_size(size, from_name, to_name):
    return size / (
        _SIZE_FACTORS[from_name.upper()] * _SIZE_FACTORS[to_name.upper()]
    )
