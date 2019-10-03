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

from iceberg.api import FileScanTask

from .partition_spec_parser import PartitionSpecParser
from .schema_parser import SchemaParser


class BaseFileScanTask(FileScanTask):

    def __init__(self, file, schema_str, spec_str, residuals):
        self._file = file
        self._schema_str = schema_str
        self._spec_str = spec_str
        self._spec = None
        self._residuals = residuals

    @property
    def file(self):
        return self._file

    @property
    def spec(self):
        if self._spec is None:
            self._spec = PartitionSpecParser.from_json(SchemaParser.from_json(self._schema_str), self._spec_str)

        return self._spec

    @property
    def start(self):
        return 0

    @property
    def length(self):
        return self._file.file_size_in_bytes()

    @property
    def residual(self):
        return self._residuals.residual_for(self._file.partition())

    def split(self, split_size):
        if self.file.format().is_splittable():
            return [task for task in SplitScanTaskIterator(split_size, self)]
        else:
            return self

    def __repr__(self):
        fields = ["file: {}".format(self._file.path()),
                  "partition_data: {}".format(self._file.partition()),
                  "residual: {}".format(self.residual)]

        return "BaseFileScanTask({})".format(", ".join(fields))

    def __str__(self):
        return self.__repr__()


class SplitScanTaskIterator(object):

    def __init__(self, split_size, file_scan_task):
        self._offset = 0
        self._remaining_len = file_scan_task.length
        self._split_size = split_size
        self._file_scan_task = file_scan_task

    def has_next(self):
        return self._remaining_len > 0

    def __iter__(self):
        return self

    def __next__(self):
        if self.has_next():
            len = min(self._split_size, self._remaining_len)
            split_task = SplitScanTask(self._offset, len, self._file_scan_task)
            self._offset += len
            self._remaining_len -= len
            return split_task

        raise StopIteration


class SplitScanTask(FileScanTask):

    def __init__(self, offset, len, file_scan_task):
        self._offset = offset
        self._len = len
        self._file_scan_task = file_scan_task

    @property
    def file(self):
        return self._file_scan_task.file

    @property
    def spec(self):
        return self._file_scan_task.spec

    @property
    def start(self):
        return self._offset

    @property
    def length(self):
        return self._len

    @property
    def residual(self):
        return self._file_scan_task.residual()

    def split(self):
        raise RuntimeError("Cannot split a task which is already split")
