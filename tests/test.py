# -*- coding: utf-8 -*-

import logging

from docker_registry import testing

from nose import tools

logger = logging.getLogger(__name__)


class TestQuery(testing.Query):
    def __init__(self):
        self.scheme = 'elliptics'


class TestDriver(testing.Driver):
    def __init__(self):
        self.scheme = 'elliptics'
        self.path = ''
        self.config = testing.Config({"elliptics_nodes": "localhost:1025:2-0"})


@tools.raises(ValueError)
def test_elliptics_nodes_conf():
    config = testing.Config({})
    driver = testing.Driver(scheme='elliptics',
                            config=config)
    driver.setUp()


@tools.raises(ValueError)
def test_elliptics_zero_groups_conf():
    config = testing.Config({'elliptics_groups': []})
    driver = testing.Driver(scheme='elliptics',
                            config=config)
    driver.setUp()
