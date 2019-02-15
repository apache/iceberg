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

from iceberg.api import UpdateSchema


class SchemaUpdate(UpdateSchema):
    TABLE_ROOT_ID = -1

    def add_column(self, name, type, parent=None):
        raise NotImplementedError()

    def rename_column(self, name, new_name):
        raise NotImplementedError()

    def update_column(self, name, new_type):
        raise NotImplementedError()

    def delete_column(self, name):
        raise NotImplementedError()

    def apply(self):
        raise NotImplementedError()

    def commit(self):
        raise NotImplementedError()
