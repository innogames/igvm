#!/usr/bin/env python3
"""igvm - Convenience script for testing

Copyright (c) 2017 InnoGames GmbH
"""
# NOTE: This binary is provided for convenience.  It is nice to be able to run
# the checks from the repository.  This is not included on the releases.  We
# are using entry_points mechanism on the setup.py.

from igvm.cli import main

main()
