# -*- coding: utf-8 -*-

import logging
import random
import string

from docker_registry.core import driver
from docker_registry.core import exceptions
from docker_registry import testing

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

    @tools.raises(exceptions.FileNotFoundError)
    def test_remove_inexistent_path(self):
        filename = self.gen_random_string()
        self._storage.remove("/".join((filename, filename)))


class TestBorderDriverCases(object):
    def __init__(self):
        self.scheme = 'elliptics'
        self.path = ''
        self.config = testing.Config({'elliptics_nodes': GOOD_REMOTE,
                                      'elliptics_groups': [999, 1000]})

    def gen_random_string(self, length=16):
        return ''.join([random.choice(string.ascii_uppercase + string.digits)
                        for x in range(length)]).lower()

    def setUp(self):
        storage = driver.fetch(self.scheme)
        self._storage = storage(self.path, self.config)

    @tools.raises(exceptions.FileNotFoundError)
    def test_s_remove(self):
        filename = self.gen_random_string()
        self._storage.s_remove(filename)

    @tools.raises(exceptions.UnspecifiedError)
    def test_s_write(self):
        filename = self.gen_random_string()
        tag = self.gen_random_string(length=5)
        self._storage.s_write(filename, "dummycontent", (tag,))

    @tools.raises(exceptions.UnspecifiedError)
    def test_s_append(self):
        filename = self.gen_random_string()
        self._storage.s_append(filename, "dummycontent")


def _set_up_with_config(config):
    config = testing.Config(config)
    d = testing.Driver(scheme='elliptics',
                       config=config)
    d.setUp()
    return d


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
    dr = _set_up_with_config({'elliptics_groups': groups,
                              'elliptics_nodes': GOOD_REMOTE})
    assert sorted(dr._storage._session.groups) == sorted(groups)

    groups_as_string = "[1, 2,3]"
    dr = _set_up_with_config({'elliptics_groups': groups_as_string,
                              'elliptics_nodes': GOOD_REMOTE})
    assert sorted(dr._storage._session.groups) == sorted(groups)
