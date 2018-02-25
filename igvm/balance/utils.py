"""igvm - Balancing Utilities

Copyright (c) 2018, InnoGames GmbH
"""

import logging


class ServeradminCache(object):
    _serveradmin_object_cache = {}

    @classmethod
    def get(cls, key):
        if key in cls._serveradmin_object_cache:
            return cls._serveradmin_object_cache[key]

        logging.error('Cache mismatch for {}'.format(key))

        return None

    @classmethod
    def set(cls, key, value):
        cls._serveradmin_object_cache[key] = value

    @classmethod
    def query(cls, **kwargs):
        # WARNING: This does only work if cache is filled with expected value.
        rs = []

        for entry in cls._serveradmin_object_cache.values():
            for key, value in kwargs.items():
                if key not in entry or entry[key] != value:
                    break
            else:
                rs.append(entry)

        return rs

    @classmethod
    def size(cls):
        return len(cls._serveradmin_object_cache)


def filter_hypervisors(vm, hypervisors, constraints):
    """Get hypervisors which fulfill the constraints

    Takes a list of hypervisors and returns the ones which fulfill the given
    constraints.

    :param: vm: igvm.balance.models.VM object
    :param: hypervisors: list of igvm.balance.models.Hypervisor objects
    :param: constraints: list of constraint configuration tuples

    :return: list
    """

    for constraint in constraints:
        logging.info('Checking constraint {}'.format(
            type(constraint).__name__)
        )
        dismiss = list()
        for hypervisor in hypervisors:
            if not constraint.fulfilled(vm, hypervisor):
                dismiss.append(hypervisor)

        filter(lambda h: hypervisors.remove(h), dismiss)

    return hypervisors


def get_ranking(vm, hypervisors, rules):
    """Get hypervisor ranking for rules

    Takes a list of hypervisors and returns a ranking as dictionary with the
    hypervisor hostname as key and the score as value for the given rules.

    :param: vm: igvm.balance.models.VM object
    :param: hypervisors: list of igvm.balance.models.Hyperviosr objects
    :param: rules: list of rules configuration tuples

    :return: dict
    """

    logging.info('Getting hypervisor ranking')

    rule_ranking = dict()
    for hypervisor in hypervisors:
        for rule in rules:
            score = rule.score(vm, hypervisor)
            rulename = rule.__class__.__name__
            hostname = hypervisor.hostname

            if rulename not in rule_ranking:
                rule_ranking[rulename] = {}

            # We allow to increase the importance of specific rules by
            # setting a factor for the rule in balance.json.
            score = float(score)
            if hasattr(rule, 'weight'):
                score = score * float(rule.weight)

            rule_ranking[rulename][hostname] = score

    normalized_ranking = _normalize_ranking(rule_ranking)
    logging.debug(normalized_ranking)

    return normalized_ranking


def _normalize_ranking(rule_ranking):
    """Normalize ranking of rules

    Takes the intermediate rule_ranking from get_ranking and normalizes it.
    This means applying weights for rules if given and also make it relative to
    each other so that no rule fucks up ranking in case it give a much higher
    score than others.

    :param: rule_ranking: rule_ranking structure as dictionary

    :return: dict ranking with hypervisor hostname as key and score as value
    """

    ranking = dict()

    # TODO: Improve this since the scores are very different.  If there is
    # a way to normalize them, that would be great!
    for rule_name, hypervisor_score in rule_ranking.items():
        for hypervisor, score in hypervisor_score.items():
            if hypervisor not in ranking:
                ranking[hypervisor] = score
            else:
                ranking[hypervisor] += score

    return ranking
