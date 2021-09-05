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
import os

from iceberg.exceptions import AlreadyExistsException

from .expressions import JAVA_MAX_INT
from .io import (InputFile,
                 OutputFile,
                 PositionOutputStream,
                 SeekableInputStream)


class Files(object):

    @staticmethod
    def local_output(file):
        return LocalOutputFile(file)

    @staticmethod
    def local_input(file):
        return LocalInputFile(file)


class LocalOutputFile(OutputFile):

    def __init__(self, file):
        self.file = file

    def create(self):
        if os.path.exists(self.file):
            raise AlreadyExistsException("File already exists: %s" % self.file)

        return PositionOutputStream(open(self.file, "w"))

    def create_or_overwrite(self):
        if os.path.exists(self.file):
            if not os.remove(self.file.name):
                raise RuntimeError("File deleting file: %s" % self.file)

        return self.create()

    def location(self):
        return self.file

    def __str__(self):
        return self.location()


class LocalInputFile(InputFile):

    def __init__(self, file):
        self.file = file

    def get_length(self):
        return os.path.getsize(self.file)

    def new_stream(self, gzipped=False):
        with open(self.file, "rb") as fo:
            if gzipped:
                fo = gzip.GzipFile(fileobj=fo)
            return fo

    def location(self):
        return self.file

    def __str__(self):
        return self.location()


class SeekableFileInputStream(SeekableInputStream):

    def __init__(self, stream):
        self.stream = stream

    def get_pos(self):
        return self.stream.tell()

    def seek(self, new_pos):
        return self.stream.seek(new_pos)

    def read(self, b=None, off=None, read_len=None):
        if b is None and off is None and read_len is None:
            return None, self.stream.read()
        if b is None:
            raise RuntimeError("supplied byte field is None")

        if off is None and read_len is None:
            new_b = self.stream.read(b.length)
            return len(new_b), new_b

        if off is not None and read_len is None or off is None and read_len is not None:
            raise RuntimeError("Invalid args: read_len(%s), off(%s)" % (read_len, off))

        if read_len < 0 or off < 0 or (len(b) - off < read_len):
            raise RuntimeError("Invalid args: read_len(%s), off(%s), len_b_offset(%s)" % (read_len, off, len(b) - off))

        new_b = bytes(self.stream.read(read_len), "utf8")

        if off > 0:
            new_b = b[0:off] + new_b
        if off + read_len < len(b):
            new_b = new_b + b[off + read_len:]
        return read_len, new_b

    def skip(self, n):
        if n > JAVA_MAX_INT:
            return self.stream.seek(JAVA_MAX_INT)
        else:
            return self.stream.seek(n)
