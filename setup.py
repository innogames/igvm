#!/usr/bin/env python
from distutils.core import setup

setup(
    name='igvm',
    version='1.0',
    url='https://gitlab.innogames.de/sysadmins/igvm',
    packages=['igvm', 'igvm.utils', ],
    scripts=[
        'bin/igvm',
    ],
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
