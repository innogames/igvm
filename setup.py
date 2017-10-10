#!/usr/bin/env python3
"""igvm - Setup

Copyright (c) 2017, InnoGames GmbH
"""

from setuptools import setup

setup(
    name='igvm',
    version='1.1',
    url='https://gitlab.innogames.de/sysadmins/igvm',
    packages=['igvm', 'igvm.utils', 'igvm.balance'],
    entry_points={
        'console_scripts': [
            'igvm=igvm.cli:main',
        ],
    },
    package_data={
        'igvm': [
            'templates/domain.xml',
            'templates/etc/network/interfaces',
            'templates/etc/fstab',
            'templates/etc/hosts',
            'templates/etc/inittab',
            'templates/etc/resolv.conf',
            'templates/balance.json',
        ]
    },
    author='InnoGames System Administration',
    author_email='it@innogames.com',
)
