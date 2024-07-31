#!/usr/bin/env python3
"""igvm - Setup

Copyright (c) 2024 InnoGames GmbH
"""

from setuptools import setup

from igvm import VERSION


def install_requires():
    # This isn't the recommended way because install_requires and 
    # requirements.txt are for different things but in our case this
    # is the bare minimum we need.
    # 
    # See: https://packaging.python.org/en/latest/discussions/install-requires-vs-requirements
    with open('requirements.txt') as f:
        return f.readlines()


setup(
    name='igvm',
    version='.'.join(str(v) for v in VERSION),
    packages=['igvm'],
    entry_points={
        'console_scripts': [
            'igvm=igvm.cli:main',
        ],
    },
    package_data={
        'igvm': [
            'templates/aws_user_data.cfg',
            'templates/domain.xml',
            'templates/etc/network/interfaces',
            'templates/etc/fstab',
            'templates/etc/hosts',
            'templates/etc/inittab',
            'templates/etc/resolv.conf',
            'scripts/ssh_wrapper',
        ]
    },
    author='InnoGames System Administration',
    author_email='it@innogames.com',
    license='MIT',
    platforms='POSIX',
    description='InnoGames VM Provisioning Tool',
    url='https://github.com/innogames/igvm',
    install_requires=install_requires(),
)
