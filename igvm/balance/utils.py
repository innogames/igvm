import os
import json
import logging

from importlib import import_module


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


def get_config_keys():
    """Get balance configurations available

    Load config and return available configuration keys alphabetically sorted.

    :return: set
    """

    dirname = os.path.join(os.path.dirname(__file__), '..', 'templates')
    filename = os.path.abspath(dirname + '/balance.json')

    with open(filename, 'r') as f:
        config = json.loads(f.read())
        return set(sorted(config.keys()))


def get_config(name, fallback='basic'):
    """Get balance configuration for name or fallback if not present

    :param: name: key of configuration name as string
    :param: fallback: key of fallback if name is not present

    :return: dict
    """

    dirname = os.path.join(os.path.dirname(__file__), '..', 'templates')
    filename = os.path.abspath(dirname + '/balance.json')

    with open(filename, 'r') as f:
        config = json.loads(f.read())
        if name in config.keys():
            return config[name]
        else:
            return config[fallback]


def get_constraints(config):
    """Get list of constraint configuration

    Create a list of configuration tuples which are ready to get instantiated
    with _get_instance function when vm and hv attribute are added to kwargs.

    :param: config: list of dictionaries with constraint configuration

    :return: str, dict: module name, constraint configuration
    """

    module = 'igvm.balance.constraints'
    return [(module, constraint) for constraint in config]


def get_rules(config):
    """Get list of rule configuration

    Create a list of configuration tuples which are ready to get instantiated
    with _get_instance function when vm and hv attribute are added to kwargs.

    :param: config: list of dictionaries with rule configuration

    :return: str, dict: module name, rule configuration
    """
    rules = list()

    module = 'igvm.balance.rules'
    for rule in config:
        rules.append((module, rule))

    return rules


def _get_instance(module, class_name, class_kwargs={}):
    """Get object for class in module

    Creates an instance for the class in the given module and sets the given
    keyword arguments as attributes.

    :param: module: dot separated module path as string
    :param: class_name: class name as string
    :param: class_kwargs: optional class attributes and values as dictionary

    :return: object
    """

    c_module = import_module(module)
    c_class = getattr(c_module, class_name)

    return c_class(**class_kwargs)


def filter_hypervisors(vm, hypervisors, constraints):
    """Get hypervisors which fulfill the constraints

    Takes a list of hypervisors and returns the ones which fulfill the given
    constraints.

    :param: vm: igvm.balance.models.VM object
    :param: hypervisors: list of igvm.balance.models.Hypervisor objects
    :param: constraints: list of constraint configuration tuples

    :return: list
    """

    for c_module, c_config in constraints:
        logging.info('Checking constraint {}'.format(c_config['class']))
        dismiss = list()
        for hypervisor in hypervisors:
            c_kwargs = c_config.copy()
            c_class = c_kwargs.pop('class')
            constraint = _get_instance(c_module, c_class, c_kwargs)
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
        for r_module, r_config in rules:
            r_kwargs = r_config.copy()
            r_class = r_kwargs.pop('class')
            rule = _get_instance(r_module, r_class, r_kwargs)
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

    # @TODO Improve this since the scores are very different. If there would
    # be a way to normalize them that would be great!
    for rule_name, hypervisor_score in rule_ranking.items():
        for hypervisor, score in hypervisor_score.items():
            if hypervisor not in ranking:
                ranking[hypervisor] = score
            else:
                ranking[hypervisor] += score

    return ranking
