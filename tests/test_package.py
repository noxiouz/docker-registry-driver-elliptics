# -*- coding: utf-8 -*-

import logging

logger = logging.getLogger(__name__)


class TestQuery(object):
    def test_package(self):
        import elliptics
        logger.debug("Got elliptics %s" % elliptics)
