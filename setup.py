#!/usr/bin/env python
from distutils.core import setup

setup(
        name='managevm',
        version='0.1',
        url='git@gitlab.innogames.de:sysadmins/ig.managevm.git',
        packages=['managevm', 'managevm.utils', 'managevm.hooks'],
        scripts=['bin/buildvm', 'bin/migratevm'],
        package_data={'managevm': ['templates/*']},
        author='Henning Pridohl',
        author_email='henning.pridohl@innogames.de',
        maintainer='Kajetan Staszkiewicz',
        maintainer_email='kajetan.staszkiewicz@innogames.de',
        )

