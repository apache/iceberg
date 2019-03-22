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

from iceberg.api.io import InputFile

from .util import get_fs


class HadoopInputFile(InputFile):

    def __init__(self, fs, path, conf, length=None, stat=None):
        self.fs = fs
        self.path = path
        self.conf = conf
        self.length = length
        self.stat = stat

    @staticmethod
    def from_location(location, conf):
        fs = get_fs(location, conf)
        return HadoopInputFile(fs, location, conf)

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

    def new_fo(self):
        return self.fs.open(self.location())
