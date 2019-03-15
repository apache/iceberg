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

from iceberg.api.io import OutputFile

from .util import get_fs


class HadoopOutputFile(OutputFile):

    @staticmethod
    def from_path(path, conf):
        return HadoopOutputFile(path, conf)

    def __init__(self, path, conf):
        self.path = path
        self.conf = conf

    def create(self):
        fs = get_fs(self.path, self.conf)
        if fs.exists():
            raise RuntimeError("File %s already exists" % self.path)

        return fs.open(self.path)

    def create_or_overwrite(self):
        fs = get_fs(self.path, self.conf)

        return fs.open(self.path, "wb")

    def location(self):
        return str(self.path)

    def __str__(self):
        return self.location()
