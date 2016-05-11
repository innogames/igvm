#!/usr/bin/env python
from distutils.core import setup

setup(
    name='managevm',
    version='1.0',
    url='git@gitlab.innogames.de:sysadmins/ig.managevm.git',
    packages=['managevm', 'managevm.utils', 'managevm.hooks'],
    scripts=[
        'bin/buildvm',
        'bin/migratevm',
    ],
    package_data={
        'managevm': [
            'templates/etc/network/interfaces',
            'templates/etc/xen/domains/hostname.sxp',
            'templates/etc/xen/domains/tribalwars.sxp',
            'templates/etc/xen/domains/tribalwars_ssd.sxp',
            'templates/etc/xen/domains/tribalwars_us.sxp',
            'templates/etc/fstab',
            'templates/etc/hosts',
            'templates/etc/inittab',
            'templates/etc/resolv.conf',
            'templates/libvirt/domain.xml',
            'templates/libvirt/domain_memhotplug.xml',
        ]
    },
    author='Henning Pridohl',
    author_email='henning.pridohl@innogames.de',
    maintainer='Kajetan Staszkiewicz',
    maintainer_email='kajetan.staszkiewicz@innogames.de',
)
