#!/usr/bin/env python
from distutils.core import setup

setup(
    name='igvm',
    version='1.0',
    url='https://gitlab.innogames.de/sysadmins/igvm',
    packages=['igvm', 'igvm.utils', 'managevm', 'managevm.utils', 'managevm.hooks'],
    scripts=[
        'bin/igvm',
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
        ],
        'igvm': [
            'templates/hv/domain.sxp',
            'templates/hv/domain.xml',
        ]
    },
    author='Henning Pridohl',
    author_email='henning.pridohl@innogames.de',
    maintainer='Kajetan Staszkiewicz',
    maintainer_email='kajetan.staszkiewicz@innogames.de',
)
