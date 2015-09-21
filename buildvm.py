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
parser.add_argument('--boot',     action='store_true',       help='Boot after setup')
parser.add_argument('--postboot', metavar='postboot_script', help='Run postboot_script on the guest after first boot')
parser.add_argument('--nopuppet', action='store_true',       help='Skip running puppet in chroot before powering up',)
parser.add_argument('-o', metavar='key=value', nargs='+', type=lambda x: x.split('=', 1), help='Sets an option')

args = parser.parse_args()

config = {}

adminapi.auth()

if args.guest:
    config['vm_hostname'] = args.guest

if args.image:
    config['image'] = args.image

if args.nopuppet:
    config['runpuppet'] = False
else:
    config['runpuppet'] = True

if args.boot:
    config['boot'] = True
else:
    config['boot'] = False

if args.postboot:
    config['postboot_script'] = args.postboot
    config['boot'] = True

buildvm(config)
