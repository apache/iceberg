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

import unittest

from iceberg.api import PartitionSpec, Schema
from iceberg.api.types import IntegerType, NestedField, StringType
from iceberg.core import PartitionSpecParser


class TestPartitionSpecParser(unittest.TestCase):

    def test_to_json_conversion(self):
        spec_schema = Schema(NestedField.required(1, "id", IntegerType.get()),
                             NestedField.required(2, "data", StringType.get()))

        spec = PartitionSpec\
            .builder_for(spec_schema) \
            .identity("id")\
            .bucket("data", 16)\
            .build()

        expected = '{"spec-id": 0, "fields": [' \
                   '{"name": "id", "transform": "identity", "source-id": 1}, ' \
                   '{"name": "data_bucket", "transform": "bucket[16]", "source-id": 2}]}'
        assert expected == PartitionSpecParser.to_json(spec)


if __name__ == '__main__':
    unittest.main()
