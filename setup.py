"""igvm - Setup

Copyright (c) 2024 InnoGames GmbH
"""

import re

from setuptools import setup

from igvm import VERSION


# igvm runs with either modern "fabric" or legacy "fabric3" through
# igvm.fabric_compat, so we deliberately do not hard-depend on one specific
# fabric. requirements.txt still installs modern fabric for pip-based dev
# setups; the Debian package expresses the choice as the alternative
# dependency "python3-fabric | python3-fabric3" (see --depends3 in the deb
# build). Without this, dh_python3 would turn the requirements.txt entry into
# a strict "python3-fabric" dependency and the .deb would refuse to install on
# fabric3-only hosts.
_EXCLUDE_FROM_INSTALL_REQUIRES = {'fabric', 'fabric3'}


def install_requires():
    # This isn't the recommended way because install_requires and
    # requirements.txt are for different things but in our case this
    # is the bare minimum we need.
    #
    # See: https://packaging.python.org/en/latest/discussions/install-requires-vs-requirements
    with open('requirements.txt') as f:
        requirements = []
        for line in f:
            dist_name = re.split(r'[<>=~!;@\[ ]', line.strip(), maxsplit=1)[0]
            if dist_name.lower() in _EXCLUDE_FROM_INSTALL_REQUIRES:
                continue
            requirements.append(line)
        return requirements


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
