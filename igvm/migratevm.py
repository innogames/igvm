"""igvm - migratevm

Copyright (c) 2018, InnoGames GmbH
"""
# XXX: This module is for backwards compatibility.

from igvm.commands import vm_migrate

# Compatibility wrapper for script consumers
migratevm = vm_migrate
