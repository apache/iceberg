# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import gzip

from iceberg.api.io import InputFile, OutputFile

from .util import get_fs


class FileSystem(object):

    def open(self, path, mode='rb'):
        raise NotImplementedError()

    def create(self, path, overwrite=False):
        raise NotImplementedError()

    def exists(self, path):
        raise NotImplementedError()

    def delete(self, path):
        raise NotImplementedError()

    def stat(self, path):
        raise NotImplementedError()

    def rename(self, src, dest):
        raise NotImplementedError()


class FileSystemInputFile(InputFile):

    def __init__(self, fs, path, conf, length=None, stat=None):
        self.fs = fs
        self.path = path
        self.conf = conf
        self.length = length
        self.stat = stat

    @staticmethod
    def from_location(location, conf):
        fs = get_fs(location, conf)
        return FileSystemInputFile(fs, location, conf)

    def location(self):
        return self.path

    def get_length(self):
        return self.get_stat().length

    def get_stat(self):
        return self.lazy_stat()

    def lazy_stat(self):
        if self.stat is None:
            self.stat = self.fs.stat(self.path)
        return self.stat

    def new_stream(self, gzipped=False):
        with self.fs.open(self.location()) as fo:
            if gzipped:
                fo = gzip.GzipFile(fileobj=fo)
            for line in fo:
                yield line

    def new_fo(self, mode="rb"):
        return self.fs.open(self.location(), mode=mode)

    def __repr__(self):
        return "FileSystemInputFile({})".format(self.path)

    def __str__(self):
        return self.__repr__()


class FileSystemOutputFile(OutputFile):

    @staticmethod
    def from_path(path, conf):
        return FileSystemOutputFile(path, conf)

    def __init__(self, path, conf):
        self.path = path
        self.conf = conf

    def create(self, mode="w"):
        fs = get_fs(self.path, self.conf)
        if fs.exists(self.path):
            raise RuntimeError("File %s already exists" % self.path)

        return fs.open(self.path, mode=mode)

    def create_or_overwrite(self):
        fs = get_fs(self.path, self.conf)

        return fs.open(self.path, "wb")

    def location(self):
        return str(self.path)

    def __repr__(self):
        return "FileSystemOutputFile({})".format(self.path)

    def __str__(self):
        return self.__repr__()
