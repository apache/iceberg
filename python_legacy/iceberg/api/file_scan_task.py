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

from .scan_task import ScanTask


class FileScanTask(ScanTask):

    @property
    def file(self):
        raise NotImplementedError()

    @property
    def spec(self):
        raise NotImplementedError()

    @property
    def start(self):
        raise NotImplementedError()

    @property
    def length(self):
        raise NotImplementedError()

    @property
    def residual(self):
        raise NotImplementedError()

    def is_file_scan_task(self):
        return True

    def as_file_scan_task(self):
        return self
