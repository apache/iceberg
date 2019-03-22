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


from iceberg.api import Filterable
from iceberg.api import TableScan
from iceberg.api.expressions import Expressions
from iceberg.api.io import CloseableGroup


class BaseTableScan(CloseableGroup, TableScan):

    def __init__(self, ops, table, snapshot_id=None, columns=None, row_filter=None):
        self.ops = ops
        self.table = table
        self.snapshot_id = snapshot_id
        self.columns = columns
        self.row_filter = row_filter

        if self.columns is None and self.row_filter is None:
            self.columns = Filterable.ALL_COLUMNS
            self.row_filter = Expressions.always_true()
