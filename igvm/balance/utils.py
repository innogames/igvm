import os
import json
import logging

from importlib import import_module


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

        i_constraints = list()
        for hypervisor in hypervisors:
            c_kwargs = c_config.copy()
            c_class = c_kwargs.pop('class')
            c_kwargs['vm'] = vm
            c_kwargs['hv'] = hypervisor
            constraint = _get_instance(c_module, c_class, c_kwargs)
            i_constraints.append(constraint)
        logging.info(
            'Evaluating constraint {}, {} hypervisors'.format(
                c_config['class'],
                len(hypervisors)
            ))

        threads_running = 0
        while i_constraints:
            logging.debug((
                '{} hypervisors left, {} constraints left, {}/32 threads used'
            ).format(
                len(hypervisors), len(i_constraints), threads_running
            ))
            for i_constraint in i_constraints:
                if threads_running < 32:
                    # Result is set with a boolean value when constraint
                    # execution is finished and is_alive is False when thread
                    # has not started yet or is finished so we can be sure if
                    # result is None and is_alive returns False that the thread
                    # has not been started yet.
                    if (
                        i_constraint.result is None and
                        i_constraint.is_alive() is False
                    ):
                        threads_running = threads_running + 1
                        i_constraint.start()

                # If result is not None anymore and is_alive returns False we
                # can be sure that the thread has finished.
                if (
                    i_constraint.result is not None and
                    i_constraint.is_alive() is False
                ):
                    if not i_constraint.result:
                        hypervisors.remove(i_constraint.hv)

                    i_constraints.remove(i_constraint)
                    threads_running = threads_running - 1
        logging.info(
            'Constrain applied, {} hypervisors left'.format(len(hypervisors))
        )

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

    i_rules = list()
    for hypervisor in hypervisors:
        for r_module, r_config in rules:
            r_kwargs = r_config.copy()
            r_class = r_kwargs.pop('class')
            r_kwargs['vm'] = vm
            r_kwargs['hv'] = hypervisor
            rule = _get_instance(r_module, r_class, r_kwargs)
            i_rules.append(rule)

    threads_running = 0
    rule_ranking = dict()
    while i_rules:
        logging.debug('{} rules left, {}/32 threads used'.format(
            len(i_rules), threads_running)
        )

        for i_rule in i_rules:
            if threads_running < 32:
                # Result is set with a boolean value when constraint
                # execution is finished and is_alive is False when thread
                # has not started yet or is finished so we can be sure if
                # result is None and is_alive returns False that the thread
                # has not been started yet.
                if i_rule.result is None and i_rule.is_alive() is False:
                    threads_running = threads_running + 1
                    i_rule.start()

            # If result is not None anymore and is_alive returns False we
            # can be sure that the thread has finished.
            if i_rule.result is not None and i_rule.is_alive() is False:
                rulename = i_rule.__class__.__name__
                hostname = i_rule.hv.hostname

                if rulename not in rule_ranking:
                    rule_ranking[rulename] = {}

                # We allow to increase the importance of specific rules by
                # setting a factor for the rule in balance.json.
                score = float(i_rule.result)
                if hasattr(i_rule, 'weight'):
                    score = score * float(i_rule.weight)

                rule_ranking[rulename][hostname] = score
                i_rules.remove(i_rule)
                threads_running = threads_running - 1

    normalized_ranking = _normalize_ranking(rule_ranking)

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
