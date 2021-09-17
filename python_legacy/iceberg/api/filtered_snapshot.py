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

from .expressions import Expressions
from .filterable import Filterable


class FilteredSnapshot(Filterable):

    def __init__(self, snapshot, part_filter, row_filter, columns):
        self.snapshot = snapshot
        self.part_filter = part_filter
        self.row_filter = row_filter
        self.columns = columns

    def select(self, columns):
        return FilteredSnapshot(self.snapshot, self.part_filter, self.row_filter, columns)

    def filter_partitions(self, expr):
        return FilteredSnapshot(self.snapshot, Expressions.and_(self.part_filter, expr), self.row_filter, self.columns)

    def filter_rows(self, expr):
        return FilteredSnapshot(self.snapshot, self.part_filter, Expressions.and_(self.row_filter, expr), self.columns)

    def iterator(self):
        return self.snapshot.iterator(self.part_filter, self.row_filter, self.columns)
