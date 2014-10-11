#!/usr/bin/env python
from __future__ import with_statement

import logging
import os
import stat
import sys

from fuse import FUSE
from fuse import FuseOSError
from fuse import LoggingMixIn
from fuse import Operations
import yaml

from docker_registry.drivers.elliptics import Storage
from docker_registry.lib import config

logging.basicConfig()
log = logging.getLogger("")
log.setLevel(logging.DEBUG)

DIRECTORY_CONTENT = "DIRECTORY"
MAGIC_NUMBER = len(DIRECTORY_CONTENT)

class RegistryFS(LoggingMixIn, Operations):
    def __init__(self):
        cfg = config.load()
        try:
            self.storage = Storage(path=None,config=cfg)
        except Exception as err:
            log.error(err)
            raise FuseOSError(-100)

    def transform_path(self, path):
        # strip a starting slash
        # and convert unicode to a normal string
        return str(path.lstrip("/"))

    def readdir(self, path, fh):
        path = self.transform_path(path)

        def apply(item, path):
            if item.startswith(path):
                item = item[len(path):]
            return item.lstrip("/")

        return (apply(i, path) for i in self.storage.list_directory(path) if i)

    def getattr(self, path, fh=None):
        if path == "/":
            r = os.lstat(path)
            return dict((key, getattr(r, key))
                        for key in dir(r) if key.startswith("st_"))

        path = self.transform_path(path)
        ares = self.storage._session.lookup(path)
        # ugly hack
        for i in ares.get():
            res = {'st_atime': i.timestamp.tsec,
                   'st_ctime': i.timestamp.tsec,
                   'st_mode': 0o777,  # ugly hack
                   'st_mtime': i.timestamp.tsec,
                   'st_nlink': 1,
                   'st_size': i.size}

        if res['st_size'] == MAGIC_NUMBER and\
           self.storage.get_content(path) == DIRECTORY_CONTENT:
            res['st_mode'] |= stat.S_IFDIR
        else:
            res['st_mode'] |= stat.S_IFREG

        return res

    def read(self, path, length, offset, fh):
        path = self.transform_path(path)
        return self.storage.get_content(path)


def main(mountpoint):
    FUSE(RegistryFS(),
         mountpoint, foreground=True)

if __name__ == '__main__':
    main(sys.argv[1])
