"""igvm - Hypervisor Ranking

Copyright (c) 2018 InnoGames GmbH
"""

from igvm.settings import HYPERVISOR_PREFERENCES


class HypervisorRanking(object):
    """Encapsulate hypervisors for lazy comparison of their rankings"""
    def __init__(self, vm, hypervisor):
        self.vm = vm
        self.hypervisor = hypervisor
        self.ranks = []

    # It is sufficient to define the __lt__() method only, because this class
    # is only for sorting, and Python sort routines are guaranteed to use
    # this method only.
    def __lt__(self, other):
        assert self.vm == other.vm

        # Compare the current preferences
        rank_len = min(len(self.ranks), len(other.ranks))
        ranks = self.ranks[:rank_len]
        other_ranks = other.ranks[:rank_len]
        if ranks < other_ranks:
            return True
        if ranks > other_ranks:
            return False

        # Check new preferences
        for index in range(rank_len, len(HYPERVISOR_PREFERENCES)):
            rank = self.get_rank(index)
            other_rank = other.get_rank(index)
            if rank < other_rank:
                return True
            if rank > other_rank:
                return False

        raise Exception(
            'Exact same preferences for hypervisor "{}" and "{}"'
            .format(self.hypervisor, other.hypervisor)
        )

    def get_rank(self, index):
        if len(self.ranks) == index:
            preference = HYPERVISOR_PREFERENCES[index]
            rank = preference(self.vm, self.hypervisor)
            self.ranks.append(rank)
        else:
            rank = self.ranks[index]

        return rank

    def get_last_preference_index(self):
        """Return the index of the last needed preference"""
        return len(self.ranks) - 1
