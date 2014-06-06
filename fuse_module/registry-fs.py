#!/usr/bin/env python
from __future__ import with_statement

import logging
import os
import sys
import stat

import yaml
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

from docker_registry.drivers.elliptics import Storage


logging.basicConfig()
log = logging.getLogger("")
log.setLevel(logging.DEBUG)

DIRECTORY_CONTENT = "DIRECTORY"
MAGIC_NUMBER = len(DIRECTORY_CONTENT)


class RegistryFS(LoggingMixIn, Operations):
    def __init__(self, config_path):
        with open(config_path, 'r') as f:
            cfg = yaml.load(f)
        try:
            self.storage = Storage(config=cfg)
        except Exception as err:
            log.error(err)
            FuseOSError(-100)

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
                   'st_mode': 0777,  # ugly hack
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


def main(mountpoint, config_path):
    FUSE(RegistryFS(config_path),
         mountpoint, foreground=True)

if __name__ == '__main__':
    main(sys.argv[2], sys.argv[1])
