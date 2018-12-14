# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
  API for configuring a table scan.
  <p>
  TableScan objects are immutable and can be shared between threads. Refinement methods, like
  {@link #select(Collection)} and {@link #filter(Expression)}, create new TableScan instances.
 """


class TableScan(object):

    def __init__(self):
        raise RuntimeError("Interface implementation")

    def use_snapshot(self, snapshot_id):
        raise RuntimeError("Interface implementation")

    def as_of_time(self, timestamp_millis):
        raise RuntimeError("Interface implementation")

    def select(self, columns):
        raise RuntimeError("Interface implementation")

    def filter(self, expr=None):
        raise RuntimeError("Interface implementation")

    def plan_file(self):
        raise RuntimeError("Interface implementation")

    def plan_tasks(self):
        raise RuntimeError("Interface implementation")
