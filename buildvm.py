#!/usr/bin/python
from __future__ import print_function

import argparse
import sys
import os

#print('Disabled during the weekend')
#sys.exit(1)

import adminapi
from adminapi.utils import IP
from adminapi.dataset import DatasetError, query

from managevm.buildvm import buildvm

parser = argparse.ArgumentParser(description='Creates a new virtual machine.')
parser.add_argument('guest',      metavar='guest',           help='Hostname of the guest system')
parser.add_argument('--image',    metavar='image',           help='Image file for the guest')
parser.add_argument('--postboot', metavar='postboot_script', help='Run postboot_script on the guest after first boot')
parser.add_argument('--nopuppet', action='store_true',       help='Skip running puppet in chroot before powering up',)

args = vars(parser.parse_args())

adminapi.auth()

buildvm(args.pop('guest'), **args)
