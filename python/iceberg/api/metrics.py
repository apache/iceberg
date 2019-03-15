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


class Metrics(object):

    def __init__(self, row_count=None,
                 column_sizes=None,
                 value_counts=None, null_value_counts=None,
                 lower_bounds=None, upper_bounds=None):
        self.row_count = row_count
        self.column_sizes = column_sizes
        self.value_counts = value_counts
        self.null_value_counts = null_value_counts
        self.lower_bounds = lower_bounds
        self.upper_bounds = upper_bounds
