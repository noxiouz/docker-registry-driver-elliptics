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

import os
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
DEFAULT_VERBOSITY = 'error'


class Storage(driver.Base):

    def __init__(self, path=None, config=None):
        # Turn on streaming support
        self.supports_bytes_range = True
        # Increase buffer size up to 640 Kb
        self.buffer_size = 128 * 1024
        # Create default Elliptics config
        cfg = elliptics.Config()
        # The parameter which sets the time to wait for the operation complete
        cfg.config.wait_timeout = (config.elliptics_wait_timeout or
                                   DEFAUL_WAIT_TIMEOUT)
        # The parameter which sets the timeout for pinging node
        cfg.config.check_timeout = (config.elliptics_check_timeout or
                                    DEFAULT_CHECK_TIMEOUT)
        # Number of IO threads in processing pool
        cfg.config.io_thread_num = (config.elliptics_io_thread_num or
                                    DEFAULT_IO_THREAD_NUM)
        # Number of threads in network processing pool
        cfg.config.net_thread_num = (config.elliptics_net_thread_num or
                                     DEFAULT_NET_THREAD_NUM)
        # Number of IO threads in processing pool dedicated to nonblocking ops
        nblock_iothreads = (config.elliptics_nonblocking_io_thread_num or
                            DEFAULT_NONBLOCKING_IO_THREAD_NUM)

        cfg.config.nonblocking_io_thread_num = nblock_iothreads
        self.groups = config.elliptics_groups or DEFAULT_GROUPS

        if isinstance(self.groups, types.StringTypes):
            self.groups = map(int, self.groups.strip('[]').split(','))

        if len(self.groups) == 0:
            raise exceptions.ConfigError("elliptics_groups must be specified")

        # loglevel of elliptics logger
        elliptics_log_level = (config.elliptics_verbosity or
                               DEFAULT_VERBOSITY).lower()
        if elliptics_log_level not in elliptics.log_level.names.keys():
            raise exceptions.ConfigError('Invalid log level %s. Use one of %s'
                                         % (elliptics_log_level,
                                            ','.join(elliptics.log_level.names.keys())))

        # path to logfile
        elliptics_log_file = config.elliptics_logfile or '/dev/stderr'
        log = elliptics.Logger(elliptics_log_file, getattr(elliptics.log_level, elliptics_log_level))
        self._elliptics_node = elliptics.Node(log, cfg)

        self.namespace = config.elliptics_namespace or DEFAULT_NAMESPACE
        logger.info("Using namespace %s", self.namespace)

        remotes_configuration = config.elliptics_nodes
        if remotes_configuration is None:
            raise exceptions.ConfigError("elliptics_nodes must be specified")
        elif isinstance(remotes_configuration,
                        (types.TupleType, types.ListType)):
            remotes = remotes_configuration
        elif isinstance(remotes_configuration, types.StringTypes):
            remotes = remotes_configuration.split()
        else:
            raise exceptions.ConfigError("elliptics_nodes must be list,"
                                         "tuple or string")

        for remote in remotes:
            try:
                logger.debug("Remote %s is being added", remote)
                self._elliptics_node.add_remote(remote)
                logger.info("%s remote has been added successfully", remote)
            except Exception as err:
                logger.error("Failed to add remote %s: %s", remote, err)

        if not self._session.routes.addresses():
            # routing table is empty,
            # as no remotes have been successfully added or conencted.
            raise exceptions.ConnectionError("Unable to connect to Elliptics")

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
            logger.warning("Unable to remove key %s indexes %s",
                           key, err.message)
        if fail:
            raise exceptions.FileNotFoundError("No such file %s" % key)

    def s_read(self, path, offset=0, size=0):
        r = self._session.read_data(path, offset=offset, size=size)
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
            raise exceptions.UnspecifiedError("Writing failed %s" % err)

        # Set indexes
        r = self._session.update_indexes(key, list(tags), [key] * len(tags))
        r.wait()
        err = r.error()
        if err.code != 0:
            raise exceptions.UnspecifiedError("Indexe setting failed %s" % err)

    def s_append(self, key, content):
        session = self._session
        session.ioflags = elliptics.io_flags.append

        # set offset to resolve function overloading
        r = session.write_data(key, content, offset=0)
        r.wait()
        err = r.error()
        if err.code != 0:
            raise exceptions.UnspecifiedError("Writing failed {0}".format(err))

    def s_write_file(self, path, content):
        tag, _, _ = path.rpartition('/')
        if len(content) == 0:
            content = "EMPTY"
        logger.debug("put_content: write %s with tag %s", path, tag)
        self.s_write(path, content, ('docker', tag))
        self.create_fake_dir_struct(path)
        return path

    @lru.get
    def get_content(self, path):
        try:
            return self.s_read(path)
        except Exception:
            raise exceptions.FileNotFoundError("File not found %s" % path)

    @lru.set
    def put_content(self, path, content):
        logger.debug("put_content %s %d", path, len(content))
        return self.s_write_file(path, content)

    def create_fake_dir_struct(self, path):
        """`path` is full filename (i.e. to create structure for file 'a/b/c'
        `path` must be `a/b/c`).

        To support listing and existance operations fake directory struct
        is created on top of key-value and index operations.
        Every file is tagged with his dirname index.
        (i.e a/b/c. `c` content would be written as key `a/b/c` and marked
        in index `a/b`
        Fake directory file would be written as key `a/b` with a dummy content
        to support existance operation.
        Listing of `path` is performed as looking for all key marked
        with `path` index.
        """
        logger.debug("creating fake directory structure %s", path)
        # get parent dir for a given filepath
        fakedir_key = os.path.dirname(path)
        while True:
            _tag = os.path.dirname(fakedir_key)
            logger.debug("creating fake dir %s %s", fakedir_key, _tag)
            self.s_write(fakedir_key, "DIRECTORY", ('docker', _tag))
            fakedir_key = _tag
            if not fakedir_key:  # root has been reached. fakedir_key is empty.
                break
        logger.debug("fake directory structure %s has been created", path)

    def stream_write(self, path, fp):
        first_chunk = True
        while True:
            try:
                buf = fp.read(self.buffer_size)
                if not buf:
                    break

                # add buffer-in-the-middle
                # not to write small chunks

                if not first_chunk:
                    self.s_append(path, buf)
                else:
                    # first of all the old file should be rewritten if exists.
                    # all tags will be set up.
                    self.s_write_file(path, buf)
                    first_chunk = False

            except IOError as err:
                logger.error("unable to read from a given socket %s", err)
                break
        # should I clean not completely written file
        # in case of error?

    def stream_read(self, path, bytes_range=None):
        logger.debug("read range %s from %s", str(bytes_range), path)
        if not self.exists(path):
            raise exceptions.FileNotFoundError(
                'No such directory: \'{0}\''.format(path))

        if bytes_range is None:
            yield self.s_read(path)
        else:
            offset = bytes_range[0]
            size = bytes_range[1] - bytes_range[0] + 1
            yield self.s_read(path, offset=offset, size=size)

    def list_directory(self, path=None):
        if path is None:  # pragma: no cover
            path = ""

        if not self.exists(path) and path:
            raise exceptions.FileNotFoundError(
                'No such directory: \'{0}\''.format(path))

        for item in self.s_find(('docker', path)):
            yield item

    def exists(self, path):
        logger.debug("Check existance of %s", path)
        try:
            # read is used instead of lookup
            # just for future quorum reading check
            self.s_read(path, 0, 1)
        except exceptions.FileNotFoundError:
            logger.debug("%s doesn't exist", path)
            return False
        else:
            logger.debug("%s exists", path)
            return True

    @lru.remove
    def remove(self, path):
        try:
            for subdir in self.list_directory(path):
                self.s_remove(subdir)
        except exceptions.FileNotFoundError as err:
            logger.warning(err)
        self.s_remove(path)

    def get_size(self, path):
        logger.debug("get_size of %s", path)
        r = self._session.lookup(path)
        r.wait()
        lookups = r.get()
        err = r.error()
        if err.code != 0:
            raise exceptions.FileNotFoundError(
                "Unable to get size of %s %s" % (path, err))
        size = lookups[0].size
        logger.debug("size of %s = %d", path, size)
        return size
