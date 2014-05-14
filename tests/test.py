# -*- coding: utf-8 -*-

import logging

from docker_registry.testing import Driver
from docker_registry.testing import Query
from docker_registry.testing.utils import Config

logger = logging.getLogger(__name__)


class TestQuery(Query):
    def __init__(self):
        self.scheme = 'elliptics'


class TestDriver(Driver):
    def __init__(self):
        self.scheme = 'elliptics'
        self.path = ''
        self.config = Config({})
