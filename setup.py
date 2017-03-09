#!/usr/bin/env python
"""igvm - Setup

Copyright (c) 2017, InnoGames GmbH
"""

from distutils.core import setup

setup(
    name='igvm',
    version='1.0',
    url='https://gitlab.innogames.de/sysadmins/igvm',
    packages=['igvm', 'igvm.utils', ],
    entry_points={
        'console_scripts': [
            'igvm=igvm.cli:main',
        ],
    },
    package_data={
        'igvm': [
            'templates/hv/domain.sxp',
            'templates/hv/domain.xml',
            'templates/etc/network/interfaces',
            'templates/etc/fstab',
            'templates/etc/hosts',
            'templates/etc/inittab',
            'templates/etc/resolv.conf',
        ]
    },
    author='InnoGames System Administration',
    author_email='it@innogames.com',
)
