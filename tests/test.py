# -*- coding: utf-8 -*-

import logging

from docker_registry import testing

logger = logging.getLogger(__name__)


class TestQuery(testing.Query):
    def __init__(self):
        self.scheme = 'elliptics'


class TestDriver(testing.Driver):
    def __init__(self):
        self.scheme = 'elliptics'
        self.path = ''
        self.config = testing.Config({"elliptics_nodes": "1.1.2.3:1025:2 10.0.0.2:1025:10"})
