"""igvm - Hypervisor Ranking

Copyright (c) 2018 InnoGames GmbH
"""

from igvm.settings import HYPERVISOR_PREFERENCES
from igvm.utils import ComparableByKey, LazyCompare


class HypervisorRanking(ComparableByKey):
    """Encapsulate hypervisors for lazy comparison of their rankings"""
    def __init__(self, vm, hypervisor):
        self.hypervisor = hypervisor
        self.ranks = [
            LazyCompare(p, vm, hypervisor) for p in HYPERVISOR_PREFERENCES
        ]

    def sort_key(self):
        return self.ranks

    def get_last_preference_index(self):
        """Return the index of the last needed preference"""
        for index, rank in enumerate(self.ranks):
            if not rank.executed:
                break
        return index
