# -*- coding: utf-8 -*-
"""
docker_registry.drivers.elliptics
~~~~~~~~~~~~~~~~~~~~~~~~~~

Elliptics is a fault tolerant distributed key/value storage.
See http://reverbrain.com/elliptics and
https://github.com/reverbrain/elliptics for additional info.

Docs: http://doc.reverbrain.com/
Deployment guide: http://doc.reverbrain.com/elliptics:server-tutorial
Packages: http://repo.reverbrain.com/

"""

import types

import itertools
import logging


def import_non_local(name, custom_name=None):
    import imp
    import sys

    custom_name = custom_name or name

    f, pathname, desc = imp.find_module(name, sys.path[1:])
    return imp.load_module(custom_name, f, pathname, desc)

elliptics = import_non_local('elliptics')

from docker_registry.core import driver
from docker_registry.core import exceptions
from docker_registry.core import lru

logger = logging.getLogger(__name__)

DEFAULT_NAMESPACE = "DOCKER"


DEFAUL_WAIT_TIMEOUT = 60
DEFAULT_CHECK_TIMEOUT = 60
DEFAULT_IO_THREAD_NUM = 2
DEFAULT_NET_THREAD_NUM = 2
DEFAULT_NONBLOCKING_IO_THREAD_NUM = 2
DEFAULT_GROUPS = [1]


class Storage(driver.Base):

    def __init__(self, path=None, config=None):
        cfg = elliptics.Config()
        # The parameter which sets the time to wait for the operation complete
        cfg.config.wait_timeout = int(config.get("elliptics_wait_timeout",
                                                 DEFAUL_WAIT_TIMEOUT))
        # The parameter which sets the timeout for pinging node
        cfg.config.check_timeout = int(config.get("elliptics_check_timeout",
                                                  DEFAULT_CHECK_TIMEOUT))
        # Number of IO threads in processing pool
        cfg.config.io_thread_num = int(config.get("elliptics_io_thread_num",
                                                  DEFAULT_IO_THREAD_NUM))
        # Number of threads in network processing pool
        cfg.config.net_thread_num = int(config.get("elliptics_net_thread_num",
                                                   DEFAULT_NET_THREAD_NUM))
        # Number of IO threads in processing pool dedicated to nonblocking ops
        nblock_iothreads = config.get("elliptics_nonblocking_io_thread_num",
                                      DEFAULT_NONBLOCKING_IO_THREAD_NUM)

        cfg.config.nonblocking_io_thread_num = int(nblock_iothreads)
        self.groups = config.get('elliptics_groups', DEFAULT_GROUPS)

        if isinstance(self.groups, types.StringTypes):
            self.groups = map(int, self.groups.strip('[]').split(','))

        if len(self.groups) == 0:
            raise ValueError("Specify groups")

        # loglevel of elliptics logger
        elliptics_log_level = int(config.get('elliptics_verbosity', 0))

        # path to logfile
        elliptics_log_file = config.get('elliptics_logfile', '/dev/stderr')
        log = elliptics.Logger(elliptics_log_file, elliptics_log_level)
        self._elliptics_node = elliptics.Node(log, cfg)

        self.namespace = config.get('elliptics_namespace', DEFAULT_NAMESPACE)
        logger.info("Using namespace %s", self.namespace)

        remotes_configuration = config.get('elliptics_nodes')
        if remotes_configuration is None:
            raise ValueError("elliptics_nodes must be specified")
        elif isinstance(remotes_configuration,
                        (types.TupleType, types.ListType)):
            remotes = remotes_configuration
        elif isinstance(remotes_configuration, types.StringTypes):
            remotes = remotes_configuration.split()
        else:
            raise ValueError("elliptics_nodes must be list, tuple or string")

        at_least_one = False
        for remote in remotes:
            try:
                logger.debug("Add remote %s", remote)
                self._elliptics_node.add_remote(remote)
                at_least_one = True
            except Exception as err:
                logger.error("Failed to add remote %s: %s", remote, err)

        if not at_least_one:
            raise Exception("Unable to connect to Elliptics")

    @property
    def _session(self):
        session = elliptics.Session(self._elliptics_node)
        session.groups = self.groups
        session.set_namespace(self.namespace)
        session.exceptions_policy = elliptics.exceptions_policy.no_exceptions
        # data should be stored in number of copies at least groups/2 + 1,
        # otherwise exception will be raised
        # i.e 3 groups -> 2 copies, 1 groups -> 1 copy
        session.set_checker(elliptics.checkers.quorum)
        return session

    def s_find(self, tags):
        r = self._session.find_all_indexes(list(tags))
        r.wait()
        result = r.get()
        return [str(i.indexes[0].data) for i in itertools.chain(result)]

    def s_remove(self, key):
        fail = False
        r = self._session.remove(key)
        r.wait()
        err = r.error()
        if err.code != 0:
            logger.warning("Unable to remove key %s %s", key, err.message)
            fail = True

        r = self._session.set_indexes(key, [], [])
        r.wait()
        err = r.error()
        if err.code != 0:
            logger.warning("Unable to remove indexes for key %s %s",
                           key, err.message)
        if fail:
            raise exceptions.FileNotFoundError("No such file %s" % key)

    def s_read(self, path):
        r = self._session.read_data(path, offset=0, size=0)
        r.wait()
        err = r.error()
        if err.code != 0:
            raise exceptions.FileNotFoundError("No such file %s" % path)

        res = r.get()[0]
        return str(res.data)

    def s_write(self, key, value, tags):
        # Write data with given key
        r = self._session.write_data(key, str(value))
        r.wait()
        err = r.error()
        if err.code != 0:
            raise IOError("Writing failed {0}".format(err))

        # Set indexes
        r = self._session.update_indexes(key, list(tags), [key] * len(tags))
        r.wait()
        err = r.error()
        if err.code != 0:
            raise IOError("Setting indexes failed {0}".format(err))

    @lru.get
    def get_content(self, path):
        try:
            return self.s_read(path)
        except Exception:
            raise exceptions.FileNotFoundError("File not found %s" % path)

    @lru.set
    def put_content(self, path, content):
        tag, _, _ = path.rpartition('/')
        if len(content) == 0:
            content = "EMPTY"
        self.s_write(path, content, ('docker', tag))
        spl_path = path.rsplit('/')[:-1]
        while spl_path:
            _path = '/'.join(spl_path)
            _tag = '/'.join(spl_path[:-1])
            spl_path.pop()
            self.s_write(_path, "DIRECTORY", ('docker', _tag))
        return path

    def stream_write(self, path, fp):
        chunks = []
        while True:
            try:
                buf = fp.read(self.buffer_size)
                if not buf:
                    break
                chunks += buf
            except IOError:
                break
        self.put_content(path, ''.join(chunks))

    def stream_read(self, path, bytes_range=None):
        yield self.get_content(path)

    def list_directory(self, path=None):
        if path is None:
            path = ""

        if not self.exists(path) and path:
            raise exceptions.FileNotFoundError(
                'No such directory: \'{0}\''.format(path))

        for item in self.s_find(('docker', path)):
            yield item

    def exists(self, path):
        tag, _, _ = path.rpartition('/')
        res = self.s_find(('docker', tag))
        return path in res

    @lru.remove
    def remove(self, path):
        try:
            for subdir in self.list_directory(path):
                self.s_remove(subdir)
        except OSError as err:
            logger.warning(err)
        self.s_remove(path)

    def get_size(self, path):
        return len(self.get_content(path))
