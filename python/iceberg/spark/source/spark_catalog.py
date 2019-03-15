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


class SparkCatalog(object):

    def create(self, ident, schema, spec, properties):
        pass

    def load(self, ident):
        pass

    def drop(self, ident):
        pass

    def load_table(self, ident):
        pass

    def create_table(self, ident, table_type, partitions, properties):
        pass

    def apply_property_changes(self, table, changes):
        pass

    def apply_schema_changes(self, table, changes):
        pass

    def drop_table(self, ident):
        pass

    def initialize(self, name, options):
        self.name = name
        self.options = options

    def convert(self, schema, partitioning):
        pass

    def __init__(self):
        self.name = ""
        self.options = None
