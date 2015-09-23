#!/usr/bin/python
from __future__ import print_function

import argparse
import sys
import os

import adminapi
from adminapi.utils import IP
from adminapi.dataset import DatasetError, query

from managevm.migratevm import migratevm

parser = argparse.ArgumentParser(description='Migrate a virtual machine.')
parser.add_argument('--dsthv',    metavar='hostname',   required=True, help='Hostname of destination hypervisor')
parser.add_argument('--newip',    metavar='IP address',                help='IP address to move VM to, in case you migrate between segments')
parser.add_argument('--nopuppet', action='store_true',                 help='Skip running puppet in chroot before powering up')
parser.add_argument('--nolbdowntime', action='store_true',             help='Don\t use testtool\'s downtime feature during migration')
parser.add_argument('--offline',  action='store_true',                 help='Force offline migration')
parser.add_argument('guest',      metavar='hostname',                  help='Hostname of the guest system')

args = vars(parser.parse_args())

adminapi.auth()

migratevm(args.pop('guest'), args.pop('dsthv'), **args)

