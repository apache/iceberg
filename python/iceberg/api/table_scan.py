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


class TableScan(object):

    def __init__(self):
        raise NotImplementedError()

    @property
    def row_filter(self):
        raise NotImplementedError()

    def use_snapshot(self, snapshot_id):
        raise NotImplementedError()

    def as_of_time(self, timestamp_millis):
        raise NotImplementedError()

    def project(self, schema):
        raise NotImplementedError()

    def select(self, columns):
        raise NotImplementedError()

    def select_except(self, columns):
        raise NotImplementedError()

    def filter(self, expr):
        raise NotImplementedError()

    def plan_files(self):
        raise NotImplementedError()

    def plan_tasks(self):
        raise NotImplementedError()

    def is_case_sensitive(self):
        raise NotImplementedError()

    def options(self):
        raise NotImplementedError()

    def to_arrow_table(self):
        raise NotImplementedError()

    def to_pandas(self):
        raise NotImplementedError()
