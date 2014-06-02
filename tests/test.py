# -*- coding: utf-8 -*-

import logging

from docker_registry import testing
from docker_registry.core import exceptions

from nose import tools

logger = logging.getLogger(__name__)

GOOD_REMOTE = "localhost:1025:2-0"
BAD_REMOTE = "localhost:1100:2-0"


class TestQuery(testing.Query):
    def __init__(self):
        self.scheme = 'elliptics'


class TestDriver(testing.Driver):
    def __init__(self):
        self.scheme = 'elliptics'
        self.path = ''
        self.config = testing.Config({'elliptics_nodes': GOOD_REMOTE})


def _set_up_with_config(config):
    config = testing.Config(config)
    driver = testing.Driver(scheme='elliptics',
                            config=config)
    driver.setUp()
    return driver


@tools.raises(exceptions.ConfigError)
def test_elliptics_no_nodes_conf():
    _set_up_with_config({})


@tools.raises(exceptions.ConfigError)
def test_elliptics_wrong_nodes_type_conf():
    _set_up_with_config({'elliptics_nodes': 1111111})


@tools.raises(exceptions.ConnectionError)
def test_elliptics_bad_nodes_conf():
    _set_up_with_config({'elliptics_nodes': [BAD_REMOTE]})


@tools.raises(exceptions.ConfigError)
def test_elliptics_zero_groups_conf():
    _set_up_with_config({'elliptics_groups': []})


def test_elliptics_groups_conf():
    groups = [1, 2, 3]
    driver = _set_up_with_config({'elliptics_groups': groups,
                                  'elliptics_nodes': GOOD_REMOTE})
    assert sorted(driver._storage._session.groups) == sorted(groups)

    groups_as_string = "[1, 2,3]"
    driver = _set_up_with_config({'elliptics_groups': groups_as_string,
                                  'elliptics_nodes': GOOD_REMOTE})
    assert sorted(driver._storage._session.groups) == sorted(groups)
