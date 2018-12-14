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


class Table(object):

    def __init__(self):
        raise RuntimeError("Interface implementation")

    def refresh(self):
        raise RuntimeError("Interface implementation")

    def new_scan(self):
        raise RuntimeError("Interface implementation")

    def schema(self):
        raise RuntimeError("Interface implementation")

    def spec(self):
        raise RuntimeError("Interface implementation")

    def properties(self):
        raise RuntimeError("Interface implementation")

    def location(self):
        raise RuntimeError("Interface implementation")

    def snapshots(self):
        raise RuntimeError("Interface implementation")

    def update_schema(self):
        raise RuntimeError("Interface implementation")

    def update_properties(self):
        raise RuntimeError("Interface implementation")

    def new_append(self):
        raise RuntimeError("Interface implementation")

    def new_fast_append(self):
        return self.new_append()

    def new_rewrite(self):
        raise RuntimeError("Interface implementation")

    def new_delete(self):
        raise RuntimeError("Interface implementation")

    def expire_snapshots(self):
        raise RuntimeError("Interface implementation")

    def rollback(self):
        raise RuntimeError("Interface implementation")

    def new_transaction(self):
        raise RuntimeError("Interface implementation")
