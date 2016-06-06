from __future__ import division

import re

_size_factors = {
    'G': 1073741824,
    'M': 1048576,
    'K': 1024,
    'B': 1
}
def parse_size(size):
    match = re.match('(\d+(.\d+)?)\s*(G|M|K)?', size, re.IGNORECASE)

    if match:
        unit = match.group(3)
        factor = _size_factors.get(unit, 1)

        if match.group(2):
            size_number = float(match.group(1))
        else:
            size_number = int(match.group(1))

        return size_number * factor
    else:
        raise ValueError('Invalid size')

def convert_size(size, from_name, to_name):
    return size / _size_factors[from_name.upper()] * _size_factors[to_name.upper()]
