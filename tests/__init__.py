"""igvm - Tests

Copyright (c) 2020 InnoGames GmbH
"""
# Configuration of VMs used for tests
# Keep in mind that the whole hostname must fit in 64 characters.
from os import environ
from re import split
from uuid import uuid4

if environ.get('EXECUTOR_NUMBER'):
    JENKINS_EXECUTOR = '{:02}'.format(int(environ['EXECUTOR_NUMBER']))
else:
    JENKINS_EXECUTOR = 'manual'

if environ.get('BUILD_NUMBER'):
    JENKINS_BUILD = environ['BUILD_NUMBER']
else:
    JENKINS_BUILD = uuid4()

if environ.get('PYTEST_XDIST_WORKER'):
    PYTEST_XDIST_WORKER = int(
        split('[a-zA-Z]+', environ['PYTEST_XDIST_WORKER'])[1]
    )
else:
    PYTEST_XDIST_WORKER = 0

if environ.get('PYTEST_XDIST_WORKER_COUNT'):
    PYTEST_XDIST_WORKER_COUNT = int(environ['PYTEST_XDIST_WORKER_COUNT'])
else:
    PYTEST_XDIST_WORKER_COUNT = 0


VM_NET = 'igvm-net-{}-aw.test.ig.local'.format(JENKINS_EXECUTOR)

VM_HOSTNAME_PATTERN = 'igvm-{}-{}-{}.test.ig.local'
VM_HOSTNAME = VM_HOSTNAME_PATTERN.format(
    JENKINS_BUILD,
    JENKINS_EXECUTOR,
    PYTEST_XDIST_WORKER,
)
